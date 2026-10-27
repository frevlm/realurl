# Docker spill benchmark harness

Runs the `slow_reader` spill matrix on Linux (clang-18, Release, raw io_uring) inside the
Docker Desktop VM. The VM (2 CPU / ~2 GB) is treated as the production box, so no container
gets `--cpus` / `--memory`; the numbers are whole-VM behaviour, including the OOM killer.

## Run

    bench/docker/run_matrix.sh                      # REPEAT=3, whole matrix.txt
    REPEAT=1 bench/docker/run_matrix.sh             # smoke run
    ONLY='^(file|dir)_' bench/docker/run_matrix.sh  # label regex filter
    SKIP_BUILD=1 bench/docker/run_matrix.sh         # reuse the binary in the build volume

Other env: `MATRIX=` (alternate matrix file), `ROW_TIMEOUT=900` (s per run),
`PG_CONTAINER=spill-pg`, `PG_CONNINFO=...`, `SRC=` (worktree, default: repo root), `BASE=`
(base image; defaults to `realurl-linux:dev` when present, else `ubuntu:24.04`),
`FORCE_RUN=1` (skip the foreign-container check), `URING=ON|OFF` (default ON; OFF builds into
its own volume `realurl-bench-build-nouring`), `OUT=` (output dir, default `bench/docker/out`).

What it does:

1. Builds image `realurl-bench:linux` (`Dockerfile`; adds libssl/sqlite3/libpq dev packages and ccache).
2. Mounts the worktree read-only at `/src` and builds `realurl_bench` + `spill_backends_test`
   into named volume `realurl-bench-build` (incremental; ccache in `realurl-bench-ccache`).
   Rebuilds from the live worktree on every invocation.
3. Aborts if any container other than `spill-pg` (and its own `realurl-bench-*`) is running.
   It never stops or removes containers it did not create.
4. Runs every `matrix.txt` row `REPEAT` times, each in a fresh container
   (`--security-opt seccomp=unconfined` for io_uring, `--oom-score-adj=500` so the kernel
   picks the bench rather than postgres).
5. Writes `out/results.csv` (one line per run, all `RESULT` keys, unknown keys pass through),
   `out/results.md` (median per label) and `out/raw/` (per-run log, meta, pg samples).

Run status: `ok` (exit 0), `partial` (RESULT printed, exit 1: some streams failed),
`oom` (container OOMKilled or exit 137, recorded not fatal), `timeout` (exit 124),
`fail` (no RESULT, e.g. store unavailable).

## matrix.txt

One row per line: `label args...`, `#` comments. Args are handed to `realurl_bench` verbatim:

    slow_reader <streams> <response_mib> <read_kib_per_s> <threshold_kib>
                [store=file|sqlite|sqlite_mt|postgres] [offload=0|1] [store_arg]
                [total_limit_mib=0] [threads=2] [window_kib=256]

Placeholders, each one argument: `@OVERLAY@` (/var/tmp/spill, container overlay fs),
`@VOL@` (/spill/run, named volume = real disk), `@TMPFS@` (/spilltmp/run, tmpfs, memory
backed: expect it in `cg_file_max_mb`), `@PG@` (conninfo of `spill-pg`).

For rows with `postgres`, `docker stats` of `spill-pg` is sampled about once a second during
the run: `pg_cpu_max` (%), `pg_mem_max_mb`, plus `pg_mem_before_mb` / `pg_mem_after_mb`.
The sampler itself costs a little CPU in the VM. Note `spill-pg` has its own memory limit.

## Known issue (as of the first validation run)

With `URING=ON` on this VM (linuxkit 7.0.14, arm64) almost every `slow_reader` run failed
within ~0.05 s with `h2 read: Connection reset by peer` (even the no-spill baseline, about 1 run
in 6 survived); the same tree built with `URING=OFF` passed 16/16 rows. The harness reports
those runs as `partial`. Use `URING=OFF` for meaningful numbers until this is fixed.
