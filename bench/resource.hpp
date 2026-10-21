#pragma once

/// Process resource sampling for benchmarks: RSS and CPU time.

#include <sys/resource.h>
#include <sys/types.h>

#include <cstdint>

#if defined(__APPLE__)
#include <mach/mach.h>
#else
#include <cstdio>
#include <unistd.h>
#endif

namespace bench
{

/// Resident set size right now.
inline std::uint64_t current_rss_bytes()
{
#if defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (::task_info(::mach_task_self(), MACH_TASK_BASIC_INFO,
                    reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS)
        return 0;
    return static_cast<std::uint64_t>(info.resident_size);
#else
    FILE *statm = std::fopen("/proc/self/statm", "r");
    if (statm == nullptr)
        return 0;
    long total_pages = 0;
    long resident_pages = 0;
    if (std::fscanf(statm, "%ld %ld", &total_pages, &resident_pages) != 2)
        resident_pages = 0;
    std::fclose(statm);
    return static_cast<std::uint64_t>(resident_pages) * static_cast<std::uint64_t>(::sysconf(_SC_PAGESIZE));
#endif
}

/// Peak resident set size. ru_maxrss is bytes on macOS, KiB on Linux.
inline std::uint64_t peak_rss_bytes()
{
    rusage usage{};
    ::getrusage(RUSAGE_SELF, &usage);
#if defined(__APPLE__)
    return static_cast<std::uint64_t>(usage.ru_maxrss);
#else
    return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024u;
#endif
}

struct CpuTime {
    double user = 0.0;
    double system = 0.0;

    double total() const noexcept
    {
        return user + system;
    }
};

inline CpuTime cpu_time()
{
    rusage usage{};
    ::getrusage(RUSAGE_SELF, &usage);
    return CpuTime{
        .user = static_cast<double>(usage.ru_utime.tv_sec) + static_cast<double>(usage.ru_utime.tv_usec) / 1e6,
        .system = static_cast<double>(usage.ru_stime.tv_sec) + static_cast<double>(usage.ru_stime.tv_usec) / 1e6,
    };
}

} // namespace bench
