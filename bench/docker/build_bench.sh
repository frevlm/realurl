#!/bin/sh
# env URING=ON|OFF (default ON).
# Inside the image: configure + build the bench targets from /src (read-only)
# into /build (named volume). Incremental across runs.
set -eu
cmake -S /src -B /build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DREALURL_BUILD_BENCH=ON -DREALURL_WITH_IO_URING="${URING:-ON}" \
    -DREALURL_BUILD_EXAMPLES=OFF
targets=realurl_bench
# older trees have no spill_backends_test
ninja -C /build -t targets all 2>/dev/null | grep -q '^spill_backends_test:' && targets="$targets spill_backends_test"
cmake --build /build -j"$(nproc)" --target $targets
ccache -s 2>/dev/null | grep -i -E 'hit|miss' || true
