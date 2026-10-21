#include "realurl/detail/frame.hpp"
#include "test_harness.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

using namespace realurl::detail::h2;

int main()
{
    {
        FrameWriter writer;
        writer.data(1, "abc", true);

        FrameParser parser;
        std::size_t offset = 0;
        Frame frame;
        CHECK(parser.next(writer.buffer(), offset, frame) == ParseResult::Ok);
        CHECK(frame.header.type == FrameType::Data);
        CHECK(frame.header.stream_id == 1);
        CHECK((frame.header.flags & kFlagEndStream) != 0);
        CHECK(frame.payload == "abc");
        CHECK(offset == writer.buffer().size());
    }

    {
        FrameWriter writer;
        writer.headers(3, "block", false, true);
        writer.continuation(3, "-tail", true);

        FrameParser parser;
        std::size_t offset = 0;
        Frame frame;
        CHECK(parser.next(writer.buffer(), offset, frame) == ParseResult::Ok);
        CHECK(frame.header.type == FrameType::Headers);
        CHECK(frame.header.stream_id == 3);
        CHECK((frame.header.flags & kFlagEndStream) != 0);
        CHECK((frame.header.flags & kFlagEndHeaders) == 0);
        CHECK(frame.payload == "block");

        CHECK(parser.next(writer.buffer(), offset, frame) == ParseResult::Ok);
        CHECK(frame.header.type == FrameType::Continuation);
        CHECK((frame.header.flags & kFlagEndHeaders) != 0);
        CHECK(frame.payload == "-tail");
    }

    {
        FrameWriter writer;
        const std::vector<Setting> settings = { { kSettingMaxConcurrentStreams, 100 },
                                                { kSettingInitialWindowSize, 1048576 } };
        writer.settings(settings);

        FrameParser parser;
        std::size_t offset = 0;
        Frame frame;
        CHECK(parser.next(writer.buffer(), offset, frame) == ParseResult::Ok);
        CHECK(frame.header.type == FrameType::Settings);
        CHECK((frame.header.flags & kFlagAck) == 0);
        CHECK(frame.header.length == 12);
    }

    {
        FrameWriter writer;
        writer.goaway(5, ErrorCode::Protocol, "bad");
        FrameParser parser;
        std::size_t offset = 0;
        Frame frame;
        CHECK(parser.next(writer.buffer(), offset, frame) == ParseResult::Ok);
        CHECK(frame.header.type == FrameType::GoAway);
        CHECK(frame.header.length == 11);
        CHECK(frame.payload.substr(8) == "bad");
    }

    {
        FrameParser parser;
        std::size_t offset = 0;
        Frame frame;
        const std::string short_header(8, '\0');
        CHECK(parser.next(short_header, offset, frame) == ParseResult::NeedMore);
        CHECK(offset == 0);

        std::string oversized(9, '\0');
        oversized[0] = 0x00;
        oversized[1] = 0x40;
        oversized[2] = 0x01; // 16385, above the default maximum
        CHECK(parser.next(oversized, offset, frame) == ParseResult::Error);
    }

    return test::summary();
}
