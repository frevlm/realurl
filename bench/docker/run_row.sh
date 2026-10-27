#!/bin/sh
# Inside the image: run one matrix row. Args are passed to realurl_bench verbatim.
# Spill locations: /var/tmp/spill (overlay fs), /spill (volume), /spilltmp (tmpfs).
set -u
mkdir -p /var/tmp/spill /spill/run /spilltmp/run
rm -rf /var/tmp/spill/* /spill/run/* /spilltmp/run/* 2>/dev/null
BIN=$(find /build -name realurl_bench -type f -perm -u+x | head -n1)
[ -n "$BIN" ] || { echo "run_row: realurl_bench not built" >&2; exit 3; }
echo "run_row: nproc=$(nproc) $(grep -E 'MemTotal' /proc/meminfo) kernel=$(uname -r)"
echo "run_row: $BIN $*"
timeout -s TERM "${ROW_TIMEOUT:-900}" "$BIN" "$@"
rc=$?
rm -rf /var/tmp/spill/* /spill/run/* /spilltmp/run/* 2>/dev/null
exit $rc
