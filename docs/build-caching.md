# Build caching

A cold build of this repository spends its time almost entirely in the C++
front end. Measured on an M-series MacBook (10 cores, Release `-O3`): ~84s of
CPU across 55 translation units, ~7.8s wall. The cost is dominated by parsing
and instantiating the Boost.Asio templates pulled in by the public server
headers, not by code generation — compiling at `-O2`, `-O1` or `-O0` all
measured *slower* than `-O3` (~112-117s CPU), and `target_precompile_headers`
saved ~2s in total. Compiler-flag tuning is a dead end; the only effective
lever is reusing front-end work with ccache.

## ccache auto-detection

The root `CMakeLists.txt` picks up a `ccache` on `PATH` automatically and
uses it as `CMAKE_CXX_COMPILER_LAUNCHER`. If ccache is installed, every build
directory, fresh clone and container replays unchanged translation units from
cache: a repeat cold build of an identical tree takes ~0.7s instead of ~7.8s.
Pass `-DCMAKE_CXX_COMPILER_LAUNCHER=` (empty) to opt out.

Cache entries are keyed on the compiler binary, flags and source content.
Changing the compiler version, build type or any flag invalidates the
affected entries; that is expected.

## CI

`.github/workflows/ci.yml` installs ccache and caches its directory with
`actions/cache@v4` keyed per OS and commit. The first push after the cache
key changes compiles everything once; subsequent pushes replay. The
auto-detection above means the workflow needs no extra CMake flags.

## Docker

Three things must hold for a fresh container to reuse the cache:

1. ccache is installed in the image (the CMake auto-detection finds it on
   `PATH`),
2. `CCACHE_DIR` points at a persistent volume, not the container's writable
   layer — a cache stored inside a `--rm` container dies with it,
3. the volume is warm: the first build populates it.

A dependency image plus a named volume is enough. The build itself runs
against a copy of the mounted source:

```dockerfile
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      cmake ninja-build clang-18 libstdc++-14-dev libboost-dev \
      libssl-dev ccache
```

```sh
docker build -t realurl-deps .
docker run --rm -v "$PWD:/src:ro" -v realurl-ccache:/ccache realurl-deps \
  bash -c 'cp -r /src /work && cd /work && rm -rf build && \
           CCACHE_DIR=/ccache cmake -S . -B build -G Ninja \
             -DCMAKE_CXX_COMPILER=clang++-18 && \
           CCACHE_DIR=/ccache ninja -C build'
```

Measured in a 2-core container, fresh `ubuntu:24.04`: ~57s total for a cold
build (~35s installing dependencies, ~22s compiling). With a baked
dependency image and a warm `realurl-ccache` volume, a brand-new container
builds in ~1.8s total. When the image itself builds the project, keep the
cache outside the layer with a BuildKit cache mount instead:

```dockerfile
RUN --mount=type=cache,target=/ccache \
    CCACHE_DIR=/ccache cmake -S . -B build -G Ninja \
      -DCMAKE_CXX_COMPILER=clang++-18 \
 && CCACHE_DIR=/ccache ninja -C build
```

Note the cache never leaves the host, so sharing a volume between projects or
machines is safe; hashes are self-contained.

Compilation is CPU-bound and scales with the cores the daemon provides. A
2-core Docker Desktop VM caps the wall time of a cold build at roughly half
its CPU time; give the builder enough cores and don't run other heavy
containers against the same limit.
