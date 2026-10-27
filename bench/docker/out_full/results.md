# slow_reader matrix (median over runs)

io_uring transport: OFF

status = count per status over runs. Memory columns are whole-container cgroup peaks (the Docker VM, 2C/2G, is the box). pg_* = spill-pg container during postgres rows.

| label | status | upstream_done_p50_s | read_done_p50_s | rss_kb_per_stream | cpu_ms_per_mib | cg_mem_max_mb | cg_anon_max_mb | cg_file_max_mb | cg_sock_max_mb | loop_stall_max_ms | pg_cpu_max | pg_mem_max_mb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base_nospill | 3xok | 8.14 | 16.02 | 5456 | 1.35 | 542.5 | 535.5 | 0 | 4.2 | 84.5 | - | - |
| file_sync | 3xok | 0.4 | 16.02 | 612 | 1.95 | 830.3 | 62.5 | 773 | 4.6 | 14.3 | - | - |
| file_offload | 3xok | 0.35 | 16.01 | 892 | 2.81 | 854.3 | 89.8 | 773.4 | 3.7 | 15 | - | - |
| file_offload_t4 | 3xok | 0.48 | 16.01 | 754 | 2.71 | 832 | 76.4 | 760.9 | 3.8 | 12.3 | - | - |
| dir_overlay_t4 | 3xok | 0.55 | 16.02 | 741 | 2.99 | 825.1 | 75 | 760.9 | 4.2 | 13.1 | - | - |
| dir_volume_t4 | 3xok | 0.46 | 16.01 | 796 | 2.67 | 828 | 80.4 | 760.9 | 4.7 | 11.9 | - | - |
| dir_tmpfs_t4 | 3xok | 0.41 | 16.01 | 834 | 2.53 | 821 | 84.2 | 773.4 | 4.5 | 15.1 | - | - |
| sqlite_offload_t4 | 3xok | 1.16 | 16.01 | 4098 | 7.39 | 1155 | 401.9 | 760.2 | 0 | 25.7 | - | - |
| sqlitemt_offload_t4 | 3xok | 1.76 | 16.01 | 2804 | 8.29 | 972 | 275.5 | 710.8 | 0.1 | 6 | - | - |
| postgres_offload_t4 | 3xok | 2.65 | 16.01 | 967 | 3.47 | 98.8 | 97.1 | 0 | 0.2 | 13.3 | 80.39 | 173 |
| cap200_file_t4 | 3xok | 10.3 | 16.01 | 869 | 3.4 | 416.3 | 87.6 | 340.8 | 0.3 | 11.9 | - | - |
| win64_file_t4 | 3xok | 0.49 | 16.01 | 670 | 2.62 | 820.8 | 68.1 | 760.9 | 3.3 | 11.4 | - | - |
| win256_file_t4 | 3xok | 0.52 | 16.02 | 792 | 2.87 | 827.9 | 80 | 760.9 | 4.7 | 16.4 | - | - |
| win1024_file_t4 | 3xok | 0.5 | 16.01 | 832 | 2.84 | 828.7 | 83.9 | 760.9 | 4.7 | 17.3 | - | - |
| heavy_nospill | 3xok | 12.29 | 16.06 | 5743 | 1.13 | 1134 | 1124 | 1 | 4.5 | 167.5 | - | - |
| heavy_file_offload | 3xok | 2.45 | 16.02 | 884 | 2.28 | 1176 | 175.3 | 1067 | 7.7 | 32.3 | - | - |

## other result keys (median)

| label | threads | window_kib | upstream_done_max_s | read_done_max_s | rss_delta_mb | cpu_s |
|---|---|---|---|---|---|---|
| base_nospill | 2 | 256 | 8.14 | 16.02 | 532.8 | 1.08 |
| file_sync | 2 | 256 | 0.4 | 16.02 | 59.8 | 1.56 |
| file_offload | 2 | 256 | 0.36 | 16.01 | 87.1 | 2.24 |
| file_offload_t4 | 4 | 256 | 0.48 | 16.01 | 73.7 | 2.17 |
| dir_overlay_t4 | 4 | 256 | 0.55 | 16.02 | 72.3 | 2.39 |
| dir_volume_t4 | 4 | 256 | 0.47 | 16.01 | 77.8 | 2.14 |
| dir_tmpfs_t4 | 4 | 256 | 0.41 | 16.01 | 81.5 | 2.03 |
| sqlite_offload_t4 | 4 | 256 | 1.22 | 16.01 | 400.2 | 5.91 |
| sqlitemt_offload_t4 | 4 | 256 | 1.94 | 16.01 | 273.8 | 6.63 |
| postgres_offload_t4 | 4 | 256 | 2.74 | 16.01 | 94.4 | 2.78 |
| cap200_file_t4 | 4 | 256 | 15.23 | 16.01 | 84.9 | 2.72 |
| win64_file_t4 | 4 | 64 | 0.49 | 16.01 | 65.4 | 2.1 |
| win256_file_t4 | 4 | 256 | 0.53 | 16.02 | 77.3 | 2.3 |
| win1024_file_t4 | 4 | 1024 | 0.5 | 16.01 | 81.3 | 2.27 |
| heavy_nospill | 2 | 256 | 12.3 | 16.09 | 1122 | 3.62 |
| heavy_file_offload | 4 | 256 | 2.49 | 16.02 | 172.6 | 7.3 |

## runs

| label | run | status | exit | wall_s | args |
|---|---|---|---|---|---|
| base_nospill | 1 | ok | 0 | 16 | `slow_reader 100 8 512 0` |
| base_nospill | 2 | ok | 0 | 16 | `slow_reader 100 8 512 0` |
| base_nospill | 3 | ok | 0 | 16 | `slow_reader 100 8 512 0` |
| file_sync | 1 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 0 /var/tmp/spill` |
| file_sync | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 0 /var/tmp/spill` |
| file_sync | 3 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 0 /var/tmp/spill` |
| file_offload | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill` |
| file_offload | 2 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill` |
| file_offload | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill` |
| file_offload_t4 | 1 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4` |
| file_offload_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4` |
| file_offload_t4 | 3 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4` |
| dir_overlay_t4 | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4` |
| dir_overlay_t4 | 2 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4` |
| dir_overlay_t4 | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4` |
| dir_volume_t4 | 1 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /spill/run 0 4` |
| dir_volume_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /spill/run 0 4` |
| dir_volume_t4 | 3 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /spill/run 0 4` |
| dir_tmpfs_t4 | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /spilltmp/run 0 4` |
| dir_tmpfs_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /spilltmp/run 0 4` |
| dir_tmpfs_t4 | 3 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /spilltmp/run 0 4` |
| sqlite_offload_t4 | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 sqlite 1 /var/tmp/spill 0 4` |
| sqlite_offload_t4 | 2 | ok | 0 | 17 | `slow_reader 100 8 512 256 sqlite 1 /var/tmp/spill 0 4` |
| sqlite_offload_t4 | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 sqlite 1 /var/tmp/spill 0 4` |
| sqlitemt_offload_t4 | 1 | ok | 0 | 17 | `slow_reader 100 8 512 256 sqlite_mt 1 /var/tmp/spill 0 4` |
| sqlitemt_offload_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 sqlite_mt 1 /var/tmp/spill 0 4` |
| sqlitemt_offload_t4 | 3 | ok | 0 | 17 | `slow_reader 100 8 512 256 sqlite_mt 1 /var/tmp/spill 0 4` |
| postgres_offload_t4 | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 postgres 1 <pg> 0 4` |
| postgres_offload_t4 | 2 | ok | 0 | 18 | `slow_reader 100 8 512 256 postgres 1 <pg> 0 4` |
| postgres_offload_t4 | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 postgres 1 <pg> 0 4` |
| cap200_file_t4 | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 200 4` |
| cap200_file_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 200 4` |
| cap200_file_t4 | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 200 4` |
| win64_file_t4 | 1 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 64` |
| win64_file_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 64` |
| win64_file_t4 | 3 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 64` |
| win256_file_t4 | 1 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 256` |
| win256_file_t4 | 2 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 256` |
| win256_file_t4 | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 256` |
| win1024_file_t4 | 1 | ok | 0 | 17 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 1024` |
| win1024_file_t4 | 2 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 1024` |
| win1024_file_t4 | 3 | ok | 0 | 16 | `slow_reader 100 8 512 256 file 1 /var/tmp/spill 0 4 1024` |
| heavy_nospill | 1 | ok | 0 | 16 | `slow_reader 200 16 1024 0` |
| heavy_nospill | 2 | ok | 0 | 16 | `slow_reader 200 16 1024 0` |
| heavy_nospill | 3 | ok | 0 | 17 | `slow_reader 200 16 1024 0` |
| heavy_file_offload | 1 | ok | 0 | 16 | `slow_reader 200 16 1024 256 file 1 /var/tmp/spill 0 4` |
| heavy_file_offload | 2 | ok | 0 | 17 | `slow_reader 200 16 1024 256 file 1 /var/tmp/spill 0 4` |
| heavy_file_offload | 3 | ok | 0 | 16 | `slow_reader 200 16 1024 256 file 1 /var/tmp/spill 0 4` |
