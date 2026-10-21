#include "realurl/detail/hpack.hpp"

#include "hpack_tables.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace realurl::detail::h2
{
namespace
{

struct HuffmanNode {
    std::int16_t child[2];
    std::int16_t symbol;
};

constexpr std::size_t kHuffmanNodes = 520;

const std::array<HuffmanNode, kHuffmanNodes> &huffman_tree()
{
    static const std::array<HuffmanNode, kHuffmanNodes> tree = [] {
        std::array<HuffmanNode, kHuffmanNodes> nodes{};
        for (HuffmanNode &node : nodes) {
            node.child[0] = -1;
            node.child[1] = -1;
            node.symbol = -1;
        }

        std::int16_t next = 1;
        for (int symbol = 0; symbol < 257; ++symbol) {
            int node = 0;
            for (int bit = kHuffmanTable[symbol].bits - 1; bit >= 0; --bit) {
                const int branch = (kHuffmanTable[symbol].code >> bit) & 1;
                if (nodes[static_cast<std::size_t>(node)].child[branch] < 0) {
                    nodes[static_cast<std::size_t>(node)].child[branch] = next;
                    ++next;
                }
                node = nodes[static_cast<std::size_t>(node)].child[branch];
            }
            nodes[static_cast<std::size_t>(node)].symbol = static_cast<std::int16_t>(symbol);
        }
        return nodes;
    }();
    return tree;
}

bool decode_int(std::string_view data, std::size_t &pos, int prefix_bits, std::uint32_t &out)
{
    if (pos >= data.size())
        return false;

    const std::uint32_t mask = (1u << prefix_bits) - 1;
    std::uint64_t value = static_cast<unsigned char>(data[pos]) & mask;
    ++pos;
    if (value < mask) {
        out = static_cast<std::uint32_t>(value);
        return true;
    }

    unsigned shift = 0;
    for (;;) {
        if (pos >= data.size())
            return false;
        const std::uint8_t byte = static_cast<unsigned char>(data[pos]);
        ++pos;
        value += static_cast<std::uint64_t>(byte & 0x7f) << shift;
        if (value > std::numeric_limits<std::uint32_t>::max())
            return false;
        if ((byte & 0x80) == 0)
            break;
        shift += 7;
        if (shift > 35)
            return false;
    }

    out = static_cast<std::uint32_t>(value);
    return true;
}

bool decode_string(std::string_view data, std::size_t &pos, std::string &out, std::size_t max_output)
{
    if (pos >= data.size())
        return false;

    const bool huffman = (static_cast<unsigned char>(data[pos]) & 0x80) != 0;
    std::uint32_t length = 0;
    if (!decode_int(data, pos, 7, length))
        return false;
    if (length > data.size() - pos)
        return false;

    const std::string_view raw = data.substr(pos, length);
    pos += length;

    if (!huffman) {
        if (raw.size() > max_output)
            return false;
        out.assign(raw);
        return true;
    }
    return huffman_decode(raw, out, max_output);
}

void encode_int(std::string &out, std::uint32_t value, int prefix_bits, std::uint8_t pattern)
{
    const std::uint32_t mask = (1u << prefix_bits) - 1;
    if (value < mask) {
        out.push_back(static_cast<char>(pattern | value));
        return;
    }

    out.push_back(static_cast<char>(pattern | mask));
    value -= mask;
    while (value >= 0x80) {
        out.push_back(static_cast<char>(0x80 | (value & 0x7f)));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

void encode_string(std::string &out, std::string_view value)
{
    encode_int(out, static_cast<std::uint32_t>(value.size()), 7, 0x00);
    out.append(value);
}

bool is_sensitive(std::string_view name)
{
    return name == "authorization" || name == "cookie" || name == "set-cookie";
}

} // namespace

bool huffman_decode(std::string_view encoded, std::string &out, std::size_t max_output)
{
    const std::array<HuffmanNode, kHuffmanNodes> &tree = huffman_tree();

    int node = 0;
    std::size_t bits_since_symbol = 0;
    bool trailing_ones = true;

    for (char raw_byte : encoded) {
        const auto byte = static_cast<unsigned char>(raw_byte);
        for (int bit_index = 7; bit_index >= 0; --bit_index) {
            const int bit = (byte >> bit_index) & 1;
            if (bit == 0)
                trailing_ones = false;
            ++bits_since_symbol;

            node = tree[static_cast<std::size_t>(node)].child[bit];
            if (node < 0)
                return false;

            const int symbol = tree[static_cast<std::size_t>(node)].symbol;
            if (symbol < 0)
                continue;

            if (symbol == 256)
                return false;
            if (out.size() >= max_output)
                return false;

            out.push_back(static_cast<char>(symbol));
            node = 0;
            bits_since_symbol = 0;
            trailing_ones = true;
        }
    }

    // Padding must be at most 7 bits and must be a prefix of EOS (all ones).
    return bits_since_symbol <= 7 && trailing_ones;
}

HpackDecoder::HpackDecoder(std::uint32_t max_table_size)
    : max_size_(max_table_size)
    , max_allowed_(max_table_size)
{
}

void HpackDecoder::set_max_allowed_table_size(std::uint32_t size)
{
    max_allowed_ = size;
    if (max_size_ > size) {
        max_size_ = size;
        evict();
    }
}

bool HpackDecoder::lookup(std::uint32_t index, std::string_view &name, std::string_view &value) const
{
    if (index == 0)
        return false;

    if (index <= 61) {
        const StaticHeader &entry = kStaticTable[index - 1];
        name = entry.name;
        value = entry.value;
        return true;
    }

    const std::uint32_t dynamic_index = index - 62;
    if (dynamic_index >= dynamic_.size())
        return false;

    const Entry &entry = dynamic_[dynamic_index];
    name = entry.name;
    value = entry.value;
    return true;
}

void HpackDecoder::evict()
{
    while (dynamic_size_ > max_size_ && !dynamic_.empty()) {
        dynamic_size_ -= dynamic_.back().size;
        dynamic_.pop_back();
    }
}

void HpackDecoder::add(std::string name, std::string value)
{
    const std::size_t size = name.size() + value.size() + 32;
    if (size > max_size_) {
        dynamic_.clear();
        dynamic_size_ = 0;
        return;
    }

    dynamic_.insert(dynamic_.begin(), Entry{ std::move(name), std::move(value), size });
    dynamic_size_ += size;
    evict();
}

bool HpackDecoder::decode(std::string_view block, HeaderList &out, std::size_t max_header_list_size)
{
    std::size_t pos = 0;
    std::size_t total = 0;

    while (pos < block.size()) {
        const auto byte = static_cast<unsigned char>(block[pos]);

        if ((byte & 0x80) != 0) {
            std::uint32_t index = 0;
            if (!decode_int(block, pos, 7, index))
                return false;

            std::string_view name;
            std::string_view value;
            if (!lookup(index, name, value))
                return false;

            total += name.size() + value.size() + 32;
            if (total > max_header_list_size)
                return false;
            out.push_back({ std::string(name), std::string(value) });
            continue;
        }

        if ((byte & 0x40) != 0) {
            std::uint32_t index = 0;
            if (!decode_int(block, pos, 6, index))
                return false;

            std::string name;
            std::string value;
            if (index == 0) {
                if (!decode_string(block, pos, name, max_header_list_size))
                    return false;
            } else {
                std::string_view static_name;
                std::string_view static_value;
                if (!lookup(index, static_name, static_value))
                    return false;
                name.assign(static_name);
            }

            if (!decode_string(block, pos, value, max_header_list_size))
                return false;

            total += name.size() + value.size() + 32;
            if (total > max_header_list_size)
                return false;

            add(name, value);
            out.push_back({ std::move(name), std::move(value) });
            continue;
        }

        if ((byte & 0x20) != 0) {
            std::uint32_t size = 0;
            if (!decode_int(block, pos, 5, size))
                return false;
            if (size > max_allowed_)
                return false;
            max_size_ = size;
            evict();
            continue;
        }

        std::uint32_t index = 0;
        if (!decode_int(block, pos, 4, index))
            return false;

        std::string name;
        std::string value;
        if (index == 0) {
            if (!decode_string(block, pos, name, max_header_list_size))
                return false;
        } else {
            std::string_view static_name;
            std::string_view static_value;
            if (!lookup(index, static_name, static_value))
                return false;
            name.assign(static_name);
        }

        if (!decode_string(block, pos, value, max_header_list_size))
            return false;

        total += name.size() + value.size() + 32;
        if (total > max_header_list_size)
            return false;
        out.push_back({ std::move(name), std::move(value) });
    }

    return true;
}

void HpackEncoder::encode(const HeaderList &headers, std::string &out)
{
    for (const HeaderField &header : headers)
        encode_one(header.name, header.value, out);
}

void HpackEncoder::encode_one(std::string_view name, std::string_view value, std::string &out)
{
    for (std::uint32_t i = 0; i < 61; ++i) {
        const StaticHeader &entry = kStaticTable[i];
        if (entry.name == name && entry.value == value) {
            encode_int(out, i + 1, 7, 0x80);
            return;
        }
    }

    std::uint32_t name_index = 0;
    for (std::uint32_t i = 0; i < 61; ++i) {
        if (kStaticTable[i].name == name) {
            name_index = i + 1;
            break;
        }
    }

    const std::uint8_t pattern = is_sensitive(name) ? 0x10 : 0x00;
    if (name_index != 0) {
        encode_int(out, name_index, 4, pattern);
    } else {
        encode_int(out, 0, 4, pattern);
        encode_string(out, name);
    }
    encode_string(out, value);
}

} // namespace realurl::detail::h2
