#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace realurl::detail::h2
{

struct HeaderField {
    std::string name;
    std::string value;
};

using HeaderList = std::vector<HeaderField>;

enum class ErrorCode : std::uint32_t {
    NoError = 0,
    Protocol = 1,
    Internal = 2,
    FlowControl = 3,
    SettingsTimeout = 4,
    StreamClosed = 5,
    FrameSize = 6,
    RefusedStream = 7,
    Cancel = 8,
    Compression = 9,
    Connect = 10,
    EnhanceYourCalm = 11,
    InadequateSecurity = 12,
    Http11Required = 13,
};

enum class FrameType : std::uint8_t {
    Data = 0,
    Headers = 1,
    Priority = 2,
    RstStream = 3,
    Settings = 4,
    PushPromise = 5,
    Ping = 6,
    GoAway = 7,
    WindowUpdate = 8,
    Continuation = 9,
};

enum FrameFlag : std::uint8_t {
    kFlagEndStream = 0x1,
    kFlagAck = 0x1,
    kFlagEndHeaders = 0x4,
    kFlagPadded = 0x8,
    kFlagPriority = 0x20,
};

enum SettingId : std::uint16_t {
    kSettingHeaderTableSize = 1,
    kSettingEnablePush = 2,
    kSettingMaxConcurrentStreams = 3,
    kSettingInitialWindowSize = 4,
    kSettingMaxFrameSize = 5,
    kSettingMaxHeaderListSize = 6,
};

struct Setting {
    std::uint16_t id = 0;
    std::uint32_t value = 0;
};

constexpr std::uint32_t kDefaultMaxFrameSize = 16384;
constexpr std::uint32_t kDefaultInitialWindowSize = 65535;
constexpr std::uint32_t kDefaultHeaderTableSize = 4096;

} // namespace realurl::detail::h2
