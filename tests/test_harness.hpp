#pragma once

#include <iostream>

namespace test
{

inline int failures = 0;

inline void check(bool ok, const char *expr, const char *file, int line)
{
    if (ok)
        return;
    std::cerr << file << ':' << line << ": check failed: " << expr << '\n';
    ++failures;
}

inline int summary()
{
    if (failures == 0) {
        std::cout << "ok\n";
        return 0;
    }
    std::cerr << failures << " failure(s)\n";
    return 1;
}

} // namespace test

#define CHECK(expr) ::test::check((expr), #expr, __FILE__, __LINE__)
