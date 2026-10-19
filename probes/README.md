# Probes

These programs exercise the components directly. They are not tests and are not
registered with CTest; build them with `-DREALURL_BUILD_PROBES=ON`.

## stream_abandon_probe

Reproduces the `StreamResult` lifetime defect documented in
`docs/performance.md`: destroying a `StreamResult` without calling
`stream_close()` leaves the easy handle attached to the multi handle.

```sh
cmake -S . -B build-asan -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DREALURL_BUILD_PROBES=ON \
  -DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer"
cmake --build build-asan
./build-asan/probes/realurl_probe_stream_abandon          # AddressSanitizer reports use-after-free
./build-asan/probes/realurl_probe_stream_abandon --close  # clean
```

## component_probe

Reports whether the `BodyFeeder`/`BodySource` pair is released, and measures
`StreamPumpPool` concurrency for same-IP versus distinct-IP submissions.

```sh
./build/probes/realurl_probe_component
```

`body_ownership.feeder_expired` and `body_ownership.source_expired` are `false`
because `BodyFeeder::make_source()` captures a strong reference back to the
feeder. This is a known limitation, not a regression.
