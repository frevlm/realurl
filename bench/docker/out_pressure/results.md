# slow_reader matrix (median over runs)

io_uring transport: OFF

status = count per status over runs. Memory columns are whole-container cgroup peaks (the Docker VM, 2C/2G, is the box). pg_* = spill-pg container during postgres rows.

| label | status | upstream_done_p50_s | read_done_p50_s | rss_kb_per_stream | cpu_ms_per_mib | cg_mem_max_mb | cg_anon_max_mb | cg_file_max_mb | cg_sock_max_mb | loop_stall_max_ms | pg_cpu_max | pg_mem_max_mb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| press_nospill | 2xoom,1xok | 16.99 | 16.99 | 4291 | 1.27 | 1742 | 1688 | 23.7 | 16.3 | 2467 | - | - |
| press_file_t4 | 3xok | 7.09 | 16.05 | 925 | 1.82 | 1412 | 364.1 | 1128 | 12.2 | 86.6 | - | - |
| press_volume_t4 | 3xok | 8.33 | 16.04 | 880 | 1.86 | 1316 | 346.5 | 1090 | 11.8 | 63.1 | - | - |
| press_tmpfs_t4 | 3xok | 15.65 | 16.03 | 1169 | 1.26 | 1403 | 460.1 | 988.4 | 9.4 | 39.9 | - | - |
| press_sqlite_t4 | 3xok | 23.22 | 56.75 | 1734 | 6.95 | 1651 | 679.1 | 1143 | 1 | 17.9 | - | - |

## other result keys (median)

| label | threads | window_kib | upstream_done_max_s | read_done_max_s | rss_delta_mb | cpu_s |
|---|---|---|---|---|---|---|
| press_nospill | 2 | 256 | 17 | 17 | 1676 | 8.11 |
| press_file_t4 | 4 | 256 | 7.3 | 16.05 | 361.2 | 11.63 |
| press_volume_t4 | 4 | 256 | 8.78 | 16.04 | 343.9 | 11.9 |
| press_tmpfs_t4 | 4 | 256 | 15.77 | 16.03 | 456.6 | 8.08 |
| press_sqlite_t4 | 4 | 256 | 23.8 | 56.77 | 677.4 | 44.48 |

## runs

| label | run | status | exit | wall_s | args |
|---|---|---|---|---|---|
| press_nospill | 1 | oom | 137 | 13 | `slow_reader 400 16 1024 0` |
| press_nospill | 2 | oom | 137 | 16 | `slow_reader 400 16 1024 0` |
| press_nospill | 3 | ok | 0 | 18 | `slow_reader 400 16 1024 0` |
| press_file_t4 | 1 | ok | 0 | 17 | `slow_reader 400 16 1024 256 file 1 /var/tmp/spill 0 4` |
| press_file_t4 | 2 | ok | 0 | 17 | `slow_reader 400 16 1024 256 file 1 /var/tmp/spill 0 4` |
| press_file_t4 | 3 | ok | 0 | 16 | `slow_reader 400 16 1024 256 file 1 /var/tmp/spill 0 4` |
| press_volume_t4 | 1 | ok | 0 | 17 | `slow_reader 400 16 1024 256 file 1 /spill/run 0 4` |
| press_volume_t4 | 2 | ok | 0 | 16 | `slow_reader 400 16 1024 256 file 1 /spill/run 0 4` |
| press_volume_t4 | 3 | ok | 0 | 16 | `slow_reader 400 16 1024 256 file 1 /spill/run 0 4` |
| press_tmpfs_t4 | 1 | ok | 0 | 17 | `slow_reader 400 16 1024 256 file 1 /spilltmp/run 0 4` |
| press_tmpfs_t4 | 2 | ok | 0 | 16 | `slow_reader 400 16 1024 256 file 1 /spilltmp/run 0 4` |
| press_tmpfs_t4 | 3 | ok | 0 | 16 | `slow_reader 400 16 1024 256 file 1 /spilltmp/run 0 4` |
| press_sqlite_t4 | 1 | ok | 0 | 57 | `slow_reader 400 16 1024 256 sqlite 1 /var/tmp/spill 0 4` |
| press_sqlite_t4 | 2 | ok | 0 | 54 | `slow_reader 400 16 1024 256 sqlite 1 /var/tmp/spill 0 4` |
| press_sqlite_t4 | 3 | ok | 0 | 63 | `slow_reader 400 16 1024 256 sqlite 1 /var/tmp/spill 0 4` |
