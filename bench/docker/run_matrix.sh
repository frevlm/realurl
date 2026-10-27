#!/usr/bin/env bash
# Build realurl_bench in Docker (Linux, clang-18, io_uring) from the live worktree and
# run the slow_reader matrix. See README.md. Never applies --cpus/--memory: the Docker
# VM (2C/2G) is treated as the production box.
#
# env: REPEAT=3  MATRIX=matrix.txt  ONLY=<regex on label>  SKIP_BUILD=1
#      PG_CONTAINER=spill-pg  PG_CONNINFO=...  ROW_TIMEOUT=900  SRC=<worktree>
#      URING=ON|OFF (default ON)  OUT=<dir>  BUILD_VOL=<volume>
#      BASE=<base image>  FORCE_RUN=1 (do not abort on foreign containers)
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
SRC=${SRC:-$(cd "$HERE/../.." && pwd)}
REPEAT=${REPEAT:-3}
MATRIX=${MATRIX:-$HERE/matrix.txt}
ONLY=${ONLY:-}
OUT=${OUT:-$HERE/out}
PG_CONTAINER=${PG_CONTAINER:-spill-pg}
PG_CONNINFO=${PG_CONNINFO:-host=host.docker.internal port=55432 user=postgres password=bench dbname=postgres}
export ROW_TIMEOUT=${ROW_TIMEOUT:-900}
IMAGE=realurl-bench:linux
RUN_NAME=realurl-bench-run
BUILD_NAME=realurl-bench-build
export URING=${URING:-ON}   # ON|OFF: raw io_uring transport (OFF uses its own build volume)
if [ "$URING" = ON ]; then V_BUILD=${BUILD_VOL:-realurl-bench-build}; else V_BUILD=${BUILD_VOL:-realurl-bench-build-nouring}; fi
V_CCACHE=realurl-bench-ccache
V_SPILL=realurl-bench-spill

die() { echo "run_matrix: $*" >&2; exit 1; }
command -v docker >/dev/null || die "docker not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"

# Only the postgres container and our own bench containers may be running.
check_containers() {
    local others
    others=$(docker ps --format '{{.Names}}' | grep -v -x -e "$PG_CONTAINER" -e "$RUN_NAME" -e "$BUILD_NAME" || true)
    if [ -n "$others" ] && [ -z "${FORCE_RUN:-}" ]; then
        echo "run_matrix: ABORT: other containers are running and would skew the 2C/2G box:" >&2
        echo "$others" | sed 's/^/    /' >&2
        echo "stop them yourself (or FORCE_RUN=1 to ignore); this script never stops foreign containers." >&2
        return 1
    fi
    return 0
}
check_containers || exit 2
docker ps --format '{{.Names}}' | grep -q -x "$PG_CONTAINER" ||
    echo "run_matrix: WARNING: $PG_CONTAINER is not running; postgres rows will fail" >&2

cleanup() { docker rm -f "$RUN_NAME" >/dev/null 2>&1; [ -n "${SAMPLER:-}" ] && kill "$SAMPLER" 2>/dev/null; }
trap cleanup EXIT

# ---- image + build -----------------------------------------------------------------
if [ -z "${BASE:-}" ]; then
    if docker image inspect realurl-linux:dev >/dev/null 2>&1; then BASE=realurl-linux:dev; else BASE=ubuntu:24.04; fi
fi
echo "== image $IMAGE (base $BASE)"
docker build -q -t "$IMAGE" --build-arg "BASE=$BASE" "$HERE" >/dev/null || die "docker build failed"
for v in $V_BUILD $V_CCACHE $V_SPILL; do docker volume create "$v" >/dev/null; done

if [ -z "${SKIP_BUILD:-}" ]; then
    echo "== building realurl_bench from $SRC (incremental, volume $V_BUILD)"
    mkdir -p "$OUT"
    docker run --rm --name "$BUILD_NAME" \
        -e URING -v "$SRC":/src:ro -v $V_BUILD:/build -v $V_CCACHE:/ccache "$IMAGE" build_bench.sh \
        2>&1 | tee "$OUT/build.log" | tail -n 25
    [ "${PIPESTATUS[0]}" -eq 0 ] || die "build failed, see $OUT/build.log"
fi

# ---- matrix ------------------------------------------------------------------------
rm -rf "$OUT/raw" "$OUT/results.csv" "$OUT/results.md"
mkdir -p "$OUT/raw"

to_mb() { # "45.3MiB" / "1.2GiB" / "800kB" -> MiB
    awk -v s="$1" 'BEGIN{ match(s,/[A-Za-z]+/); u=substr(s,RSTART); v=substr(s,1,RSTART-1)+0;
        if(u=="GiB"||u=="GB")v*=1024; else if(u=="KiB"||u=="kB"||u=="KB")v/=1024; else if(u=="B")v/=1048576;
        printf "%.1f", v }'
}
pg_sample() { docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$PG_CONTAINER" 2>/dev/null | head -n1; }

expand() { # placeholder -> single argument
    case "$1" in
        *@OVERLAY@*) echo "${1//@OVERLAY@//var/tmp/spill}" ;;
        *@VOL@*)     echo "${1//@VOL@//spill/run}" ;;
        *@TMPFS@*)   echo "${1//@TMPFS@//spilltmp/run}" ;;
        *@PG@*)      echo "${1//@PG@/$PG_CONNINFO}" ;;
        *)           echo "$1" ;;
    esac
}

n=0
aborted=
while IFS= read -r line || [ -n "$line" ]; do
    line=${line%%#*}
    # shellcheck disable=SC2206
    words=($line)
    [ ${#words[@]} -ge 2 ] || continue
    label=${words[0]}
    if [ -n "$ONLY" ] && ! [[ $label =~ $ONLY ]]; then continue; fi
    args=()
    for w in "${words[@]:1}"; do args+=("$(expand "$w")"); done
    is_pg=; for a in "${args[@]}"; do [ "$a" = postgres ] && is_pg=1; done

    for ((run = 1; run <= REPEAT; run++)); do
        check_containers || { aborted=1; break 2; }
        n=$((n + 1))
        id=$(printf '%03d_%s_r%d' "$n" "$label" "$run")
        log=$OUT/raw/$id.log
        echo "== [$label run $run/$REPEAT] ${args[*]}"
        docker rm -f "$RUN_NAME" >/dev/null 2>&1

        pg_before=; SAMPLER=; samples=$OUT/raw/$id.pgstats
        if [ -n "$is_pg" ]; then
            pg_before=$(pg_sample)
            ( while :; do pg_sample >>"$samples"; sleep 1; done ) &
            SAMPLER=$!
        fi
        t0=$(date +%s)
        docker run --name "$RUN_NAME" --security-opt seccomp=unconfined --oom-score-adj=500 \
            --add-host host.docker.internal:host-gateway \
            -e ROW_TIMEOUT \
            -v "$SRC":/src:ro -v $V_BUILD:/build:ro -v $V_SPILL:/spill --tmpfs /spilltmp \
            --entrypoint run_row.sh "$IMAGE" "${args[@]}" >"$log" 2>&1
        rc=$?
        t1=$(date +%s)
        if [ -n "$SAMPLER" ]; then kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null; SAMPLER=; fi
        oomk=$(docker inspect -f '{{.State.OOMKilled}}' "$RUN_NAME" 2>/dev/null || echo unknown)
        docker rm -f "$RUN_NAME" >/dev/null 2>&1
        pg_after=; [ -n "$is_pg" ] && pg_after=$(pg_sample)

        has_result=; grep -q '^RESULT ' "$log" && has_result=1
        if [ "$oomk" = true ] || [ $rc -eq 137 ]; then status=oom
        elif [ $rc -eq 124 ]; then status=timeout
        elif [ $rc -eq 0 ]; then status=ok
        elif [ -n "$has_result" ]; then status=partial
        else status=fail; fi

        pg_cpu_max=; pg_mem_max=; pg_mb=; pg_ma=
        if [ -n "$is_pg" ]; then
            [ -s "$samples" ] && pg_cpu_max=$(awk '{gsub("%","",$1); if($1+0>m)m=$1+0} END{print m+0}' "$samples")
            [ -s "$samples" ] && pg_mem_max=$(while read -r _ m _; do to_mb "$m"; echo; done <"$samples" | sort -n | tail -n1)
            [ -n "$pg_before" ] && pg_mb=$(to_mb "$(echo "$pg_before" | awk '{print $2}')")
            [ -n "$pg_after" ] && pg_ma=$(to_mb "$(echo "$pg_after" | awk '{print $2}')")
            docker ps --format '{{.Names}}' | grep -q -x "$PG_CONTAINER" || echo "run_matrix: WARNING: $PG_CONTAINER died during this row" >&2
        fi
        {
            echo "label=$label"; echo "run=$run"; echo "status=$status"; echo "exit=$rc"
            echo "wall_s=$((t1 - t0))"; echo "pg_cpu_max=$pg_cpu_max"; echo "pg_mem_max_mb=$pg_mem_max"
            echo "pg_mem_before_mb=$pg_mb"; echo "pg_mem_after_mb=$pg_ma"
            echo "args=${args[*]//$PG_CONNINFO/<pg>}"
        } >"$OUT/raw/$id.meta"
        echo "   -> $status (exit $rc, $((t1 - t0))s) $(grep '^RESULT ' "$log" | tail -n1 | cut -c1-150)"
        [ "$status" = fail ] && tail -n 3 "$log" | sed 's/^/      /'
    done
done <"$MATRIX"

python3 -I "$HERE/summarize.py" "$OUT" >/dev/null && echo "== $OUT/results.csv  $OUT/results.md"
[ -z "$aborted" ] || { echo "run_matrix: ABORTED early (foreign container appeared); partial results written" >&2; exit 2; }
