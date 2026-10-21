#pragma once

/// HPACK (RFC 7541) decoder and encoder.
///
/// The decoder is complete: static table, dynamic table, Huffman, table size
/// updates. The encoder uses the static table and literal-without-indexing;
/// it deliberately does not emit Huffman-coded strings in the first version
/// (valid, slightly larger headers, much less CPU on the send path).

#include "realurl/detail/http2_types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace realurl::detail::h2
{

bool huffman_decode(std::string_view encoded, std::string &out, std::size_t max_output);

class HpackDecoder {
public:
    explicit HpackDecoder(std::uint32_t max_table_size = kDefaultHeaderTableSize);

    /// Decode one header block. Returns false on a compression error; the
    /// caller must treat it as a connection error.
    bool decode(std::string_view block, HeaderList &out, std::size_t max_header_list_size = 65536);

    /// Peer SETTINGS_HEADER_TABLE_SIZE.
    void set_max_allowed_table_size(std::uint32_t size);

    std::uint32_t table_size() const noexcept
    {
        return max_size_;
    }

private:
    struct Entry {
        std::string name;
        std::string value;
        std::size_t size = 0;
    };

    bool lookup(std::uint32_t index, std::string_view &name, std::string_view &value) const;
    void add(std::string name, std::string value);
    void evict();

    std::vector<Entry> dynamic_;
    std::size_t dynamic_size_ = 0;
    std::uint32_t max_size_ = kDefaultHeaderTableSize;
    std::uint32_t max_allowed_ = kDefaultHeaderTableSize;
};

class HpackEncoder {
public:
    void encode(const HeaderList &headers, std::string &out);
    void encode_one(std::string_view name, std::string_view value, std::string &out);
};

} // namespace realurl::detail::h2
