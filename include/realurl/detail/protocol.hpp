#pragma once

/// Wire-level constants shared by the HTTP/2 and realurl native transports.
///
/// The native transport reuses the HTTP/2 frame header, stream state machine
/// and flow control; only the HEADERS/CONTINUATION payload codec differs.

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace realurl::detail
{

enum class WireMode : std::uint8_t { Http2 = 0, Native = 1 };

inline constexpr std::string_view kH2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
inline constexpr std::string_view kNativePreface = "REALURL/1\r\n";
inline constexpr std::string_view kNativeAlpn = "realurl/1";

/// Extension frame sent by a native server before SETTINGS. A plaintext client
/// uses it to distinguish a realurl peer from an ordinary h2c server that also
/// sends SETTINGS first.
inline constexpr std::uint8_t kNativeHelloType = 0x10;
inline constexpr std::string_view kNativeHelloMagic = "REALURL1";
inline constexpr std::size_t kNativeHelloPayloadSize = 12;
inline constexpr std::uint8_t kNativeVersionMajor = 1;
inline constexpr std::uint8_t kNativeVersionMinor = 0;

/// Native-only SETTINGS entry. The value is a bitmask of the DATA codecs the
/// sender can decode; a peer compresses DATA only after it sees the bit.
inline constexpr std::uint16_t kSettingNativeCompression = 0x10;
inline constexpr std::uint32_t kNativeCompressionZstd = 0x1;

/// Native-only DATA flag: the payload is one standalone zstd frame.
inline constexpr std::uint8_t kFlagNativeCompressed = 0x20;

} // namespace realurl::detail
