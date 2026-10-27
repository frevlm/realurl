#include "spill_backends.hpp"

// Database-backed SpillStore implementations (benchmark only).
//
// Both backends share one row scheme. A store owns a unique `store_id`; its
// bytes live in rows (store_id, seq, data) of at most kRowBytes each, written
// with increasing seq and consumed in seq order. The store object tracks
// next_write / next_read; the row currently being consumed is held in memory
// together with the offset of its unread remainder, so each row is fetched
// (and deleted) exactly once.
//
// sqlite configuration (see SqliteShared::kDefaultPragmas):
//   page_size=16384       measured: 4K..64K within noise, 16K marginally best
//   journal_mode=MEMORY   scratch data, no journal file; measured ~1.7x faster
//                         than journal_mode=OFF on macOS (OFF is not the
//                         fastest path), and avoids WAL checkpoint costs
//   synchronous=OFF       never fsync
//   locking_mode=EXCLUSIVE  single shared connection: no lock syscalls
//   temp_store=MEMORY, cache_size=-262144 (256 MiB), mmap_size=0
//   table spill(store_id, seq, data) WITHOUT ROWID, PRIMARY KEY(store_id, seq)
// "sqlite_mt" instead opens one connection per calling thread (WAL,
// page_size=4096, wal_autocheckpoint=1000, synchronous=OFF, busy_timeout) so
// the loop and helper threads do not serialize on a mutex in this process;
// SQLite still has a single writer, so it is not faster than "sqlite".
// REALURL_BENCH_SQLITE_PRAGMAS, if set, replaces the pragma string (tuning).
//
// postgres configuration: UNLOGGED table, data column STORAGE EXTERNAL (TOAST
// without pglz compression), binary-format PQexecPrepared, pool of 4
// connections with statements prepared once per connection,
// synchronous_commit=off.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#ifdef REALURL_BENCH_SQLITE
#include <sqlite3.h>
#endif
#ifdef REALURL_BENCH_POSTGRES
#include <libpq-fe.h>
#endif

namespace bench
{
namespace
{

constexpr size_t kRowBytes = 64 * 1024;

/// What a database must offer; every call is thread-safe and returns false on
/// any error. `take` deletes the row and returns its bytes.
class RowBackend {
public:
    virtual ~RowBackend() = default;
    virtual bool put(int64_t id, int64_t seq, std::string_view data) = 0;
    virtual bool take(int64_t id, int64_t seq, std::string& out) = 0;
    virtual bool drop(int64_t id) = 0;
    virtual int64_t new_id() = 0;
};

class RowStore final : public realurl::SpillStore {
public:
    explicit RowStore(std::shared_ptr<RowBackend> db) : db_(std::move(db)), id_(db_->new_id()) {}

    ~RowStore() override
    {
        if (next_write_ != 0)
            (void)db_->drop(id_);
    }

    bool append(std::string_view bytes) override
    {
        try {
            while (!bytes.empty()) {
                const size_t n = std::min(bytes.size(), kRowBytes);
                if (!db_->put(id_, next_write_, bytes.substr(0, n)))
                    return false;
                ++next_write_;
                bytes.remove_prefix(n);
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    bool read(std::span<char> out) override
    {
        try {
            while (!out.empty()) {
                if (cur_off_ == cur_.size()) {
                    if (next_read_ == next_write_)
                        return false;
                    if (!db_->take(id_, next_read_, cur_))
                        return false;
                    ++next_read_;
                    cur_off_ = 0;
                    if (cur_.empty())
                        return false;
                }
                const size_t n = std::min(out.size(), cur_.size() - cur_off_);
                std::memcpy(out.data(), cur_.data() + cur_off_, n);
                cur_off_ += n;
                out = out.subspan(n);
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    void reset() override
    {
        try {
            if (next_write_ != 0)
                (void)db_->drop(id_);
        } catch (...) {
        }
        next_write_ = next_read_ = 0;
        cur_.clear();
        cur_off_ = 0;
    }

private:
    std::shared_ptr<RowBackend> db_;
    int64_t id_;
    int64_t next_write_ = 0;
    int64_t next_read_ = 0;
    std::string cur_;
    size_t cur_off_ = 0;
};

realurl::SpillStoreFactory make_factory(std::shared_ptr<RowBackend> db)
{
    return [db = std::move(db)]() -> std::unique_ptr<realurl::SpillStore> {
        try {
            return std::make_unique<RowStore>(db);
        } catch (...) {
            return nullptr;
        }
    };
}

#ifdef REALURL_BENCH_SQLITE

// ---------------------------------------------------------------- sqlite

/// Database files not yet removed; swept at process exit.
struct PathRegistry {
    std::mutex mu;
    std::set<std::string> paths;
};

PathRegistry& registry()
{
    static PathRegistry* r = [] {
        std::atexit([] {
            PathRegistry& reg = registry();
            std::lock_guard lk(reg.mu);
            for (const auto& p : reg.paths)
                for (const char* sfx : {"", "-wal", "-shm", "-journal"})
                    ::unlink((p + sfx).c_str());
            reg.paths.clear();
        });
        return new PathRegistry; // leaked on purpose: used from atexit
    }();
    return *r;
}

class SqliteShared final : public RowBackend {
public:
    static constexpr const char* kDefaultPragmas =
        "PRAGMA page_size=16384;"
        "PRAGMA journal_mode=MEMORY;"
        "PRAGMA synchronous=OFF;"
        "PRAGMA locking_mode=EXCLUSIVE;"
        "PRAGMA temp_store=MEMORY;"
        "PRAGMA cache_size=-262144;"
        "PRAGMA mmap_size=0;";
    static constexpr const char* kMtPragmas =
        "PRAGMA page_size=4096;"
        "PRAGMA journal_mode=WAL;"
        "PRAGMA synchronous=OFF;"
        "PRAGMA wal_autocheckpoint=1000;"
        "PRAGMA temp_store=MEMORY;"
        "PRAGMA cache_size=-65536;"
        "PRAGMA mmap_size=0;";

    SqliteShared(std::string dir, bool per_thread) : per_thread_(per_thread)
    {
        static std::atomic<unsigned> counter{0};
        if (dir.empty()) {
            const char* t = std::getenv("TMPDIR");
            dir = (t && *t) ? t : "/tmp";
        }
        while (dir.size() > 1 && dir.back() == '/')
            dir.pop_back();
        path_ = dir + "/realurl-spill-" + std::to_string(::getpid()) + "-" +
                std::to_string(counter.fetch_add(1)) + ".db";
        PathRegistry& reg = registry();
        std::lock_guard lk(reg.mu);
        reg.paths.insert(path_);
    }

    ~SqliteShared() override
    {
        conns_.clear(); // close before unlinking
        PathRegistry& reg = registry();
        std::lock_guard lk(reg.mu);
        if (reg.paths.erase(path_))
            for (const char* sfx : {"", "-wal", "-shm", "-journal"})
                ::unlink((path_ + sfx).c_str());
    }

    bool put(int64_t id, int64_t seq, std::string_view data) override
    {
        Lease l(*this);
        Conn* c = l.conn;
        if (!c)
            return false;
        sqlite3_stmt* s = c->ins;
        sqlite3_bind_int64(s, 1, id);
        sqlite3_bind_int64(s, 2, seq);
        sqlite3_bind_blob64(s, 3, data.data(), data.size(), SQLITE_STATIC);
        const int rc = sqlite3_step(s);
        sqlite3_reset(s);
        sqlite3_clear_bindings(s);
        return rc == SQLITE_DONE;
    }

    bool take(int64_t id, int64_t seq, std::string& out) override
    {
        Lease l(*this);
        Conn* c = l.conn;
        if (!c)
            return false;
        sqlite3_stmt* s = c->take;
        sqlite3_bind_int64(s, 1, id);
        sqlite3_bind_int64(s, 2, seq);
        bool ok = false;
        int rc = sqlite3_step(s);
        if (rc == SQLITE_ROW) {
            const auto* p = static_cast<const char*>(sqlite3_column_blob(s, 0));
            const int n = sqlite3_column_bytes(s, 0);
            out.assign(p ? p : "", static_cast<size_t>(n));
            ok = true;
            rc = sqlite3_step(s); // run the statement to completion
            ok = rc == SQLITE_DONE;
        }
        sqlite3_reset(s);
        return ok;
    }

    bool drop(int64_t id) override
    {
        Lease l(*this);
        Conn* c = l.conn;
        if (!c)
            return false;
        sqlite3_bind_int64(c->del, 1, id);
        const int rc = sqlite3_step(c->del);
        sqlite3_reset(c->del);
        return rc == SQLITE_DONE;
    }

    int64_t new_id() override { return next_id_.fetch_add(1); }

private:
    struct Conn {
        sqlite3* db = nullptr;
        sqlite3_stmt* ins = nullptr;
        sqlite3_stmt* take = nullptr;
        sqlite3_stmt* del = nullptr;
        std::mutex mu; // single-connection mode only
        ~Conn()
        {
            sqlite3_finalize(ins);
            sqlite3_finalize(take);
            sqlite3_finalize(del);
            if (db)
                sqlite3_close(db);
        }
    };

    /// Resolves the connection for the calling thread and, in shared mode,
    /// holds its mutex for the duration of the call.
    struct Lease {
        explicit Lease(SqliteShared& s) : conn(s.conn_for_caller())
        {
            if (conn && !s.per_thread_)
                lk = std::unique_lock(conn->mu);
        }
        Conn* conn;
        std::unique_lock<std::mutex> lk;
    };

    static bool exec(sqlite3* db, const char* sql)
    {
        char* err = nullptr;
        const int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
        if (rc != SQLITE_OK) {
            std::fprintf(stderr, "sqlite: %s: %s\n", sql, err ? err : "error");
            sqlite3_free(err);
        }
        return rc == SQLITE_OK;
    }

    std::unique_ptr<Conn> open_conn(bool create)
    {
        auto c = std::make_unique<Conn>();
        if (sqlite3_open_v2(path_.c_str(), &c->db,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK)
            return nullptr;
        sqlite3_busy_timeout(c->db, 60000);
        const char* env = std::getenv("REALURL_BENCH_SQLITE_PRAGMAS"); // experiment hook
        const char* pragmas = env ? env : (per_thread_ ? kMtPragmas : kDefaultPragmas);
        if (!exec(c->db, pragmas))
            return nullptr;
        if (create && !exec(c->db, "CREATE TABLE IF NOT EXISTS spill("
                                   "store_id INTEGER NOT NULL, seq INTEGER NOT NULL, data BLOB NOT NULL,"
                                   "PRIMARY KEY(store_id, seq)) WITHOUT ROWID;"))
            return nullptr;
        if (sqlite3_prepare_v3(c->db, "INSERT INTO spill(store_id,seq,data) VALUES(?1,?2,?3)", -1,
                               SQLITE_PREPARE_PERSISTENT, &c->ins, nullptr) != SQLITE_OK ||
            sqlite3_prepare_v3(c->db, "DELETE FROM spill WHERE store_id=?1 AND seq=?2 RETURNING data", -1,
                               SQLITE_PREPARE_PERSISTENT, &c->take, nullptr) != SQLITE_OK ||
            sqlite3_prepare_v3(c->db, "DELETE FROM spill WHERE store_id=?1", -1, SQLITE_PREPARE_PERSISTENT,
                               &c->del, nullptr) != SQLITE_OK) {
            std::fprintf(stderr, "sqlite: prepare: %s\n", sqlite3_errmsg(c->db));
            return nullptr;
        }
        return c;
    }

    Conn* conn_for_caller()
    {
        std::lock_guard lk(mu_);
        const auto key = per_thread_ ? std::this_thread::get_id() : std::thread::id{};
        auto it = conns_.find(key);
        if (it != conns_.end())
            return it->second.get();
        auto c = open_conn(conns_.empty());
        if (!c)
            return nullptr;
        return conns_.emplace(key, std::move(c)).first->second.get();
    }

    const bool per_thread_;
    std::string path_;
    std::atomic<int64_t> next_id_{1};
    std::mutex mu_; // guards conns_
    std::unordered_map<std::thread::id, std::unique_ptr<Conn>> conns_;
};

realurl::SpillStoreFactory make_sqlite(std::string_view dir, bool per_thread)
{
    return make_factory(std::make_shared<SqliteShared>(std::string(dir), per_thread));
}

#endif // REALURL_BENCH_SQLITE

#ifdef REALURL_BENCH_POSTGRES

// -------------------------------------------------------------- postgres

constexpr unsigned kInt8Oid = 20;
constexpr unsigned kByteaOid = 17;
constexpr int kPoolSize = 4;

void put_be64(char* dst, int64_t v)
{
    const auto u = static_cast<uint64_t>(v);
    for (int i = 0; i < 8; ++i)
        dst[i] = static_cast<char>(u >> (56 - 8 * i));
}

class PgPool final : public RowBackend {
public:
    /// Creates and truncates the table, then connects the pool. Null on failure.
    static std::shared_ptr<PgPool> open(const std::string& conninfo)
    {
        auto pool = std::shared_ptr<PgPool>(new PgPool(conninfo));
        PGconn* c = pool->connect(false);
        if (!c)
            return nullptr;
        const bool ok =
            run(c, "CREATE UNLOGGED TABLE IF NOT EXISTS realurl_spill("
                   "store_id bigint NOT NULL, seq bigint NOT NULL, data bytea NOT NULL,"
                   "PRIMARY KEY(store_id, seq)) WITH (autovacuum_vacuum_scale_factor=0.02)") &&
            run(c, "ALTER TABLE realurl_spill ALTER COLUMN data SET STORAGE EXTERNAL") &&
            run(c, "TRUNCATE realurl_spill");
        PQfinish(c);
        if (!ok)
            return nullptr;
        for (int i = 0; i < kPoolSize; ++i) {
            c = pool->connect(true);
            if (!c)
                return nullptr;
            pool->all_.push_back(c);
            pool->idle_.push_back(c);
        }
        return pool;
    }

    ~PgPool() override
    {
        for (PGconn* c : all_)
            PQfinish(c);
    }

    bool put(int64_t id, int64_t seq, std::string_view data) override
    {
        Lease l(*this);
        if (!l.conn)
            return false;
        char a[8], b[8];
        put_be64(a, id);
        put_be64(b, seq);
        const char* vals[3] = {a, b, data.data()};
        const int lens[3] = {8, 8, static_cast<int>(data.size())};
        const int fmts[3] = {1, 1, 1};
        PGresult* r = PQexecPrepared(l.conn, "spill_ins", 3, vals, lens, fmts, 1);
        const bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        report(r, ok);
        PQclear(r);
        return ok;
    }

    bool take(int64_t id, int64_t seq, std::string& out) override
    {
        Lease l(*this);
        if (!l.conn)
            return false;
        char a[8], b[8];
        put_be64(a, id);
        put_be64(b, seq);
        const char* vals[2] = {a, b};
        const int lens[2] = {8, 8};
        const int fmts[2] = {1, 1};
        PGresult* r = PQexecPrepared(l.conn, "spill_take", 2, vals, lens, fmts, 1);
        bool ok = PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1;
        if (ok)
            out.assign(PQgetvalue(r, 0, 0), static_cast<size_t>(PQgetlength(r, 0, 0)));
        report(r, ok);
        PQclear(r);
        return ok;
    }

    bool drop(int64_t id) override
    {
        Lease l(*this);
        if (!l.conn)
            return false;
        char a[8];
        put_be64(a, id);
        const char* vals[1] = {a};
        const int lens[1] = {8};
        const int fmts[1] = {1};
        PGresult* r = PQexecPrepared(l.conn, "spill_drop", 1, vals, lens, fmts, 1);
        const bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        report(r, ok);
        PQclear(r);
        return ok;
    }

    int64_t new_id() override { return next_id_.fetch_add(1); }

private:
    explicit PgPool(std::string conninfo) : conninfo_(std::move(conninfo)) {}

    static bool run(PGconn* c, const char* sql)
    {
        PGresult* r = PQexec(c, sql);
        const bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        if (!ok)
            std::fprintf(stderr, "postgres: %s: %s", sql, PQerrorMessage(c));
        PQclear(r);
        return ok;
    }

    static void report(PGresult* r, bool ok)
    {
        if (!ok)
            std::fprintf(stderr, "postgres: %s\n", PQresultErrorMessage(r));
    }

    static bool prepare(PGconn* c)
    {
        const unsigned two[2] = {kInt8Oid, kInt8Oid};
        const unsigned three[3] = {kInt8Oid, kInt8Oid, kByteaOid};
        const unsigned one[1] = {kInt8Oid};
        struct S {
            const char* name;
            const char* sql;
            int n;
            const unsigned* types;
        };
        const S stmts[] = {
            {"spill_ins", "INSERT INTO realurl_spill(store_id,seq,data) VALUES($1,$2,$3)", 3, three},
            {"spill_take", "DELETE FROM realurl_spill WHERE store_id=$1 AND seq=$2 RETURNING data", 2, two},
            {"spill_drop", "DELETE FROM realurl_spill WHERE store_id=$1", 1, one},
        };
        for (const S& s : stmts) {
            PGresult* r = PQprepare(c, s.name, s.sql, s.n, s.types);
            const bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
            if (!ok)
                std::fprintf(stderr, "postgres: prepare %s: %s", s.name, PQerrorMessage(c));
            PQclear(r);
            if (!ok)
                return false;
        }
        return true;
    }

    PGconn* connect(bool with_statements)
    {
        PGconn* c = PQconnectdb(conninfo_.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            std::fprintf(stderr, "postgres: connect: %s", PQerrorMessage(c));
            PQfinish(c);
            return nullptr;
        }
        // The table is rebuilt at factory creation, so skipping the commit
        // flush is harmless; UNLOGGED data does not use the WAL anyway.
        if (!run(c, "SET client_min_messages=warning") || !run(c, "SET synchronous_commit=off") || (with_statements && !prepare(c))) {
            PQfinish(c);
            return nullptr;
        }
        return c;
    }

    /// Checks a pooled connection out for one call; `conn` is null when it is
    /// down and could not be re-established (the slot is still returned).
    struct Lease {
        explicit Lease(PgPool& p) : pool(p)
        {
            std::unique_lock lk(p.mu_);
            p.cv_.wait(lk, [&] { return !p.idle_.empty(); });
            slot = p.idle_.back();
            p.idle_.pop_back();
            lk.unlock();
            conn = slot;
            if (PQstatus(conn) != CONNECTION_OK) {
                PQreset(conn);
                if (PQstatus(conn) != CONNECTION_OK || !prepare(conn))
                    conn = nullptr;
            }
        }
        ~Lease()
        {
            std::lock_guard lk(pool.mu_);
            pool.idle_.push_back(slot);
            pool.cv_.notify_one();
        }
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        PgPool& pool;
        PGconn* slot = nullptr;
        PGconn* conn = nullptr;
    };

    std::string conninfo_;
    std::atomic<int64_t> next_id_{1};
    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<PGconn*> idle_;
    std::vector<PGconn*> all_;
};

#endif // REALURL_BENCH_POSTGRES

} // namespace

realurl::SpillStoreFactory make_spill_store(std::string_view name, std::string_view arg)
{
    try {
#ifdef REALURL_BENCH_SQLITE
        if (name == "sqlite")
            return make_sqlite(arg, false);
        if (name == "sqlite_mt")
            return make_sqlite(arg, true);
#endif
#ifdef REALURL_BENCH_POSTGRES
        if (name == "postgres") {
            auto pool = PgPool::open(std::string(arg));
            if (!pool)
                return {};
            return make_factory(std::move(pool));
        }
#endif
    } catch (...) {
    }
    (void)name;
    (void)arg;
    return {};
}

} // namespace bench
