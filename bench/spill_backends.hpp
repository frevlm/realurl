#pragma once

/// Database-backed SpillStore implementations used only by the benchmark.

#include "realurl/spill_store.hpp"

#include <string_view>

namespace bench
{

/// Factory for the named backend ("sqlite", "postgres"), or an empty factory
/// when the backend is unknown or was not compiled in. `arg` is backend
/// specific: a directory for sqlite, a libpq connection string for postgres.
realurl::SpillStoreFactory make_spill_store(std::string_view name, std::string_view arg);

} // namespace bench
