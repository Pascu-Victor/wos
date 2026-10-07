#include <gtest/gtest.h>

#include <array>
#include <net/wki/job_stream.hpp>
#include <optional>
#include <random>
#include <span>
#include <vector>

namespace jobs = ker::net::wki::jobs;
using Status = jobs::StreamStatus;
namespace {
constexpr std::array<uint8_t, 8> SOURCE{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
}

TEST(WkiJobStream, FullProducerReportsPartialWriteUntilApplicationAcknowledges) {
    jobs::SendStream<5> stream;
    auto const write = stream.append(std::span(SOURCE).first(6));
    EXPECT_EQ(write.status, Status::BLOCKED);
    EXPECT_EQ(write.bytes, 5U);
    ASSERT_EQ(stream.attach(1, 0, 5), Status::OK);
    std::array<uint8_t, 2> packet{};
    auto const prepared = stream.prepare(1, packet);
    ASSERT_EQ(prepared.status, Status::READY);
    EXPECT_EQ(packet, (std::array<uint8_t, 2>{'a', 'b'}));
    EXPECT_EQ(stream.prepare(1, packet).status, Status::BUSY);
    EXPECT_TRUE(stream.finish(prepared.frame));
    EXPECT_EQ(stream.buffered(), 5U);
    EXPECT_EQ(stream.credit({1, 1, 0, 5}), Status::OK);
    EXPECT_EQ(stream.append(std::span(SOURCE).subspan(5, 1)).bytes, 0U);
    EXPECT_EQ(stream.credit({1, 2, 2, 5}), Status::OK);
    EXPECT_EQ(stream.buffered(), 3U);
    EXPECT_EQ(stream.append(std::span(SOURCE).subspan(5, 1)).bytes, 1U);
    EXPECT_EQ(stream.produced(), 6U);
}

TEST(WkiJobStream, DuplicateReorderedAndShrunkenCreditsCannotRestoreSpentWindow) {
    jobs::SendStream<16> stream;
    ASSERT_EQ(stream.append(SOURCE).bytes, SOURCE.size());
    ASSERT_EQ(stream.attach(1, 0, 4), Status::OK);
    std::array<uint8_t, 8> packet{};
    auto const first = stream.prepare(1, packet);
    ASSERT_EQ(first.frame.bytes, 4U);
    ASSERT_TRUE(stream.finish(first.frame));
    EXPECT_EQ(stream.credit({1, 1, 0, 4}), Status::OK);
    EXPECT_EQ(stream.credit({1, 1, 0, 4}), Status::REPLAY);
    EXPECT_EQ(stream.prepare(1, packet).status, Status::BLOCKED);
    EXPECT_EQ(stream.credit({1, 2, 0, 0}), Status::OK);
    EXPECT_EQ(stream.credit({1, 1, 0, 4}), Status::STALE);
    EXPECT_EQ(stream.credit({1, 2, 0, 4}), Status::CONFLICT);
    EXPECT_EQ(stream.credit({1, 3, 5, 4}), Status::INVALID);
    EXPECT_EQ(stream.credit({1, 3, 0, 17}), Status::INVALID);
    EXPECT_EQ(stream.credit({1, 0, 0, 4}), Status::INVALID);
    EXPECT_EQ(stream.prepare(1, packet).status, Status::BLOCKED);
    EXPECT_EQ(stream.retained(), 0U);
    EXPECT_EQ(stream.credit({1, 3, 4, 2}), Status::OK);
    auto const second = stream.prepare(1, packet);
    ASSERT_EQ(second.status, Status::READY);
    EXPECT_EQ(second.frame.offset, 4U);
    EXPECT_EQ(second.frame.bytes, 2U);
    EXPECT_EQ(packet[0], 'e');
    EXPECT_EQ(packet[1], 'f');
    EXPECT_EQ(stream.credit({1, 2, 0, 0}), Status::STALE);
}

TEST(WkiJobStream, ReattachReplaysUnconsumedBytesAndFencesOldOwnedPackets) {
    jobs::SendStream<8> sender;
    jobs::ReceiveStream<8> receiver;
    ASSERT_EQ(sender.append(SOURCE).bytes, SOURCE.size());
    sender.close();
    ASSERT_EQ(sender.attach(1, 0, 8), Status::OK);
    ASSERT_EQ(receiver.attach(1), Status::OK);
    std::array<uint8_t, 4> packet{};
    auto const old = sender.prepare(1, packet);
    ASSERT_EQ(receiver.receive(1, old.frame.offset, packet, old.frame.end), Status::OK);
    std::array<uint8_t, 2> prefix{};
    ASSERT_EQ(receiver.read(prefix).bytes, 2U);
    EXPECT_EQ(prefix, (std::array<uint8_t, 2>{'a', 'b'}));
    // The application consumed two bytes but its acknowledgement was lost.
    ASSERT_TRUE(sender.detach(1));
    ASSERT_EQ(sender.attach(2, receiver.consumed(), 8), Status::OK);
    ASSERT_EQ(receiver.attach(2), Status::OK);
    EXPECT_EQ(receiver.buffered(), 0U);
    EXPECT_FALSE(sender.finish(old.frame));
    EXPECT_EQ(sender.credit({1, 1, 4, 8}), Status::STALE);
    EXPECT_EQ(receiver.receive(1, old.frame.offset, packet, old.frame.end), Status::STALE);
    auto const current = sender.prepare(2, packet);
    ASSERT_EQ(current.status, Status::READY);
    EXPECT_EQ(current.frame.offset, 2U);
    EXPECT_FALSE(sender.finish(old.frame));
    EXPECT_EQ(sender.prepare(2, packet).status, Status::BUSY);
    ASSERT_EQ(receiver.receive(2, current.frame.offset, packet, current.frame.end), Status::OK);
    ASSERT_TRUE(sender.finish(current.frame));
    std::array<uint8_t, 4> replay{};
    ASSERT_EQ(receiver.read(replay).bytes, 4U);
    EXPECT_EQ(replay, (std::array<uint8_t, 4>{'c', 'd', 'e', 'f'}));
    ASSERT_EQ(sender.credit({2, 1, 6, 8}), Status::OK);
    auto const tail = sender.prepare(2, packet);
    ASSERT_EQ(tail.frame.bytes, 2U);
    ASSERT_TRUE(tail.frame.end);
    ASSERT_EQ(receiver.receive(2, tail.frame.offset, std::span(packet).first(tail.frame.bytes), true), Status::OK);
    ASSERT_TRUE(sender.finish(tail.frame));
    EXPECT_TRUE(receiver.read(prefix).end);
    EXPECT_EQ(prefix, (std::array<uint8_t, 2>{'g', 'h'}));
}

TEST(WkiJobStream, AttachRejectsUnknownConsumptionAndReportsRetiredPrefixGap) {
    jobs::SendStream<8> stream;
    ASSERT_EQ(stream.append(SOURCE).bytes, SOURCE.size());
    EXPECT_EQ(stream.attach(1, 1, 8), Status::INVALID);
    ASSERT_EQ(stream.attach(1, 0, 8), Status::OK);
    std::array<uint8_t, 2> packet{};
    auto const prepared = stream.prepare(1, packet);
    ASSERT_TRUE(stream.finish(prepared.frame));
    ASSERT_EQ(stream.credit({1, 1, 2, 8}), Status::OK);
    EXPECT_EQ(stream.attach(2, 0, 8), Status::GAP);
    EXPECT_EQ(stream.attach(2, 3, 8), Status::INVALID);
    EXPECT_EQ(stream.attach(0, 2, 8), Status::INVALID);
    EXPECT_EQ(stream.retained(), 2U);
    EXPECT_EQ(stream.check_attach(2, 2, 8), Status::OK);
    ASSERT_EQ(stream.attach(2, 2, 8), Status::OK);
    EXPECT_EQ(stream.attach(1, 2, 8), Status::STALE);
    EXPECT_EQ(stream.attach(UINT64_MAX, 2, 8), Status::OK);
    EXPECT_EQ(stream.attach(3, 2, 8), Status::STALE);
}

TEST(WkiJobStream, EofNeedsNoCreditsAndReplaysAtItsExactFinalOffset) {
    jobs::SendStream<4> sender;
    jobs::ReceiveStream<4> receiver;
    ASSERT_EQ(sender.append(std::span(SOURCE).first(4)).bytes, 4U);
    ASSERT_EQ(sender.attach(1, 0, 4), Status::OK);
    ASSERT_EQ(receiver.attach(1), Status::OK);
    std::array<uint8_t, 4> packet{};
    auto const data = sender.prepare(1, packet);
    ASSERT_FALSE(data.frame.end);
    ASSERT_EQ(receiver.receive(1, 0, packet, false), Status::OK);
    ASSERT_TRUE(sender.finish(data.frame));
    sender.close();
    ASSERT_EQ(sender.credit({1, 1, 0, 0}), Status::OK);
    auto const end = sender.prepare(1, {});
    ASSERT_EQ(end.status, Status::READY);
    EXPECT_EQ(end.frame.offset, 4U);
    EXPECT_EQ(end.frame.bytes, 0U);
    ASSERT_TRUE(end.frame.end);
    ASSERT_EQ(receiver.receive(1, 4, {}, true), Status::OK);
    EXPECT_EQ(receiver.receive(1, 4, {}, true), Status::REPLAY);
    ASSERT_TRUE(sender.finish(end.frame));
    EXPECT_EQ(sender.prepare(1, packet).status, Status::END);
    EXPECT_EQ(sender.append(SOURCE).status, Status::CLOSED);
    ASSERT_EQ(sender.attach(2, 0, 4), Status::OK);
    ASSERT_EQ(receiver.attach(2), Status::OK);
    auto const replay = sender.prepare(2, packet);
    ASSERT_TRUE(replay.frame.end);
    ASSERT_EQ(receiver.receive(2, 0, packet, true), Status::OK);
    EXPECT_TRUE(receiver.read(packet).end);
    EXPECT_EQ(packet, (std::array<uint8_t, 4>{'a', 'b', 'c', 'd'}));
}

TEST(WkiJobStream, ReceiverNeverPartiallyAdmitsGapsOverlapsConflictsOrOverQuotaFrames) {
    jobs::ReceiveStream<4> stream;
    ASSERT_EQ(stream.attach(1), Status::OK);
    auto const first = std::span(SOURCE).first(4);
    EXPECT_EQ(stream.receive(1, 1, first, false), Status::OUT_OF_ORDER);
    ASSERT_EQ(stream.receive(1, 0, first, false), Status::OK);
    EXPECT_EQ(stream.receive(1, 2, first, false), Status::OUT_OF_ORDER);
    auto changed = SOURCE;
    ++changed[2];
    EXPECT_EQ(stream.receive(1, 0, std::span(changed).first(4), false), Status::CONFLICT);
    EXPECT_EQ(stream.receive(1, 0, first, false), Status::REPLAY);
    EXPECT_EQ(stream.receive(1, 4, std::span(SOURCE).subspan(4, 2), false), Status::BLOCKED);
    EXPECT_EQ(stream.buffered(), 4U);
    std::array<uint8_t, 2> prefix{};
    EXPECT_EQ(stream.read(prefix).bytes, 2U);
    EXPECT_EQ(prefix, (std::array<uint8_t, 2>{'a', 'b'}));
    ASSERT_EQ(stream.grant(1, stream.window()), Status::OK);
    EXPECT_EQ(stream.receive(1, 4, std::span(SOURCE).subspan(4, 2), false), Status::OK);
    EXPECT_EQ(stream.receive(1, 0, first, false), Status::REPLAY);
    std::array<uint8_t, 4> rest{};
    EXPECT_EQ(stream.read(rest).bytes, 4U);
    EXPECT_EQ(rest, (std::array<uint8_t, 4>{'c', 'd', 'e', 'f'}));
    EXPECT_EQ(stream.receive(1, 6, {}, true), Status::OK);
    EXPECT_TRUE(stream.ended());
    EXPECT_EQ(stream.receive(1, 6, std::span(SOURCE).first(1), false), Status::CLOSED);
    EXPECT_EQ(stream.receive(1, 4, {}, true), Status::CONFLICT);
    EXPECT_EQ(stream.receive(1, 6, {}, false), Status::INVALID);
}

TEST(WkiJobStream, ReadsDoNotGrantCreditAndShrinkingHonorsAlreadyAuthorizedFrames) {
    jobs::ReceiveStream<4> stream;
    ASSERT_EQ(stream.attach(1, 2), Status::OK);
    ASSERT_EQ(stream.receive(1, 0, std::span(SOURCE).first(2), false), Status::OK);
    std::array<uint8_t, 4> read{};
    ASSERT_EQ(stream.read(read).bytes, 2U);
    EXPECT_EQ(stream.receive(1, 2, std::span(SOURCE).subspan(2, 2), false), Status::BLOCKED);
    ASSERT_EQ(stream.grant(1, 4), Status::OK);
    EXPECT_EQ(stream.grant(1, 4), Status::REPLAY);
    EXPECT_EQ(stream.grant(1, 3), Status::CONFLICT);
    ASSERT_EQ(stream.grant(2, 0), Status::OK);
    EXPECT_EQ(stream.grant(1, 4), Status::STALE);
    EXPECT_EQ(stream.grant(3, 5), Status::INVALID);
    // These bytes fit the earlier grant, even though the newer grant shrank.
    ASSERT_EQ(stream.receive(1, 2, std::span(SOURCE).subspan(2, 4), true), Status::OK);
    EXPECT_TRUE(stream.read(read).end);
    EXPECT_EQ(read, (std::array<uint8_t, 4>{'c', 'd', 'e', 'f'}));
    EXPECT_EQ(stream.grant(3, 0), Status::OK);
    EXPECT_EQ(stream.advertised_credit(), (jobs::StreamCredit{1, 3, 6, 0}));
}

TEST(WkiJobStream, EarlyApplicationAcknowledgementCannotOverwriteOwnedPacketPayload) {
    jobs::SendStream<4> stream;
    ASSERT_EQ(stream.append(std::span(SOURCE).first(4)).bytes, 4U);
    ASSERT_EQ(stream.attach(1, 0, 4), Status::OK);
    std::array<uint8_t, 4> owned{};
    auto const frame = stream.prepare(1, owned);
    // The receiver's application ACK can race the TX owner's final callback.
    ASSERT_EQ(stream.credit({1, 1, 4, 4}), Status::OK);
    ASSERT_EQ(stream.append(std::span(SOURCE).subspan(4, 4)).bytes, 4U);
    EXPECT_EQ(owned, (std::array<uint8_t, 4>{'a', 'b', 'c', 'd'}));
    ASSERT_TRUE(stream.finish(frame.frame));
    auto const next = stream.prepare(1, owned);
    EXPECT_EQ(next.frame.offset, 4U);
    EXPECT_EQ(owned, (std::array<uint8_t, 4>{'e', 'f', 'g', 'h'}));
}

TEST(WkiJobStream, ByteOffsetsAndCreditWindowsFailClosedAtUint64Boundary) {
    jobs::SendStream<4> sender(UINT64_MAX - 2);
    jobs::ReceiveStream<4> receiver(UINT64_MAX - 2);
    auto const write = sender.append(std::span(SOURCE).first(3));
    EXPECT_EQ(write.status, Status::LIMIT);
    EXPECT_EQ(write.bytes, 2U);
    EXPECT_EQ(sender.produced(), UINT64_MAX);
    EXPECT_EQ(sender.attach(1, UINT64_MAX - 2, 3), Status::INVALID);
    ASSERT_EQ(sender.attach(1, UINT64_MAX - 2, 2), Status::OK);
    ASSERT_EQ(receiver.attach(1), Status::OK);
    EXPECT_EQ(receiver.window(), 2U);
    sender.close();
    std::array<uint8_t, 4> packet{};
    auto const prepared = sender.prepare(1, packet);
    ASSERT_EQ(prepared.frame.bytes, 2U);
    ASSERT_TRUE(prepared.frame.end);
    ASSERT_EQ(receiver.receive(1, prepared.frame.offset, std::span(packet).first(2), true), Status::OK);
    EXPECT_EQ(receiver.read(packet).bytes, 2U);
    EXPECT_TRUE(receiver.ended());
    EXPECT_EQ(receiver.window(), 0U);
    EXPECT_EQ(receiver.receive(1, UINT64_MAX, std::span(SOURCE).first(1), false), Status::INVALID);
    EXPECT_EQ(sender.credit({1, 1, UINT64_MAX, 1}), Status::INVALID);
    EXPECT_EQ(sender.credit({1, 1, UINT64_MAX, 0}), Status::OK);
    EXPECT_EQ(sender.buffered(), 0U);
}

TEST(WkiJobStream, FrameGenerationExhaustionCannotReuseAnOldCallback) {
    jobs::SendStream<1> stream(0, UINT64_MAX);
    ASSERT_EQ(stream.append(std::span(SOURCE).first(1)).bytes, 1U);
    ASSERT_EQ(stream.attach(1, 0, 1), Status::OK);
    std::array<uint8_t, 1> packet{};
    auto const last = stream.prepare(1, packet);
    ASSERT_EQ(last.frame.generation, UINT64_MAX);
    auto forged = last.frame;
    ++forged.offset;
    EXPECT_FALSE(stream.finish(forged));
    ASSERT_TRUE(stream.finish(last.frame));
    EXPECT_FALSE(stream.finish(last.frame));
    ASSERT_EQ(stream.credit({1, 1, 1, 1}), Status::OK);
    ASSERT_EQ(stream.append(std::span(SOURCE).subspan(1, 1)).bytes, 1U);
    EXPECT_EQ(stream.prepare(1, packet).status, Status::LIMIT);
    ASSERT_EQ(stream.attach(2, 1, 1), Status::OK);
    EXPECT_EQ(stream.prepare(2, packet).status, Status::LIMIT);
    EXPECT_FALSE(stream.finish(last.frame));
}

TEST(WkiJobStream, AFullStdoutDoesNotConsumeStderrStorageOrCredit) {
    jobs::SendStream<3> out;
    jobs::SendStream<5> err;
    EXPECT_EQ(out.append(SOURCE).bytes, 3U);
    EXPECT_EQ(out.append(SOURCE).bytes, 0U);
    EXPECT_EQ(err.append(std::span(SOURCE).subspan(3, 5)).bytes, 5U);
    ASSERT_EQ(out.attach(1, 0, 0), Status::OK);
    ASSERT_EQ(err.attach(1, 0, 5), Status::OK);
    std::array<uint8_t, 5> packet{};
    EXPECT_EQ(out.prepare(1, packet).status, Status::BLOCKED);
    auto const stderr_frame = err.prepare(1, packet);
    EXPECT_EQ(stderr_frame.frame.bytes, 5U);
    EXPECT_EQ(packet, (std::array<uint8_t, 5>{'d', 'e', 'f', 'g', 'h'}));
    EXPECT_EQ(out.buffered(), 3U);
}

namespace {
struct OwnedPacket {
    jobs::StreamFrame frame;
    std::vector<uint8_t> payload;
};

// An independent byte-source oracle drives the actual production rings. A
// prepared packet survives simulated TX refusals; callbacks/credits from old
// attachments and reordered frames are deliberately offered to the helpers.
template <size_t Capacity>
class StreamTrace {
   public:
    explicit StreamTrace(size_t size, unsigned salt) : source(size) {
        for (size_t index = 0; index < size; ++index) source[index] = static_cast<uint8_t>((index * 73 + salt) % 251);
        EXPECT_EQ(sender.attach(attachment, 0, Capacity), Status::OK);
        EXPECT_EQ(receiver.attach(attachment), Status::OK);
    }

    void step(std::mt19937& random) {
        if (written != source.size() && random() % 3 != 0) {
            auto const wanted = std::min<size_t>(source.size() - written, 1 + random() % 73);
            auto const expected = std::min(wanted, Capacity - sender.buffered());
            auto const write = sender.append(std::span(source).subspan(written, wanted));
            ASSERT_EQ(write.bytes, expected);
            written += write.bytes;
        }
        if (written == source.size()) sender.close();

        if (pending && random() % 5 != 0) {
            auto const& frame = pending->frame;
            if (!pending->payload.empty()) {
                EXPECT_EQ(receiver.receive(attachment, frame.offset + 1, pending->payload, frame.end), Status::OUT_OF_ORDER);
            }
            ASSERT_EQ(receiver.receive(attachment, frame.offset, pending->payload, frame.end), Status::OK);
            ASSERT_EQ(receiver.receive(attachment, frame.offset, pending->payload, frame.end), Status::REPLAY);
            ASSERT_TRUE(sender.finish(frame));
            pending.reset();
        } else if (pending) {
            std::array<uint8_t, 47> scratch{};
            EXPECT_EQ(sender.prepare(attachment, scratch).status, Status::BUSY);
            ++tx_refusals;
        }

        if (random() % 2 == 0) {
            std::array<uint8_t, 29> read{};
            auto const count = receiver.read(std::span(read).first(1 + random() % read.size())).bytes;
            delivered.insert(delivered.end(), read.begin(), read.begin() + static_cast<ptrdiff_t>(count));
            ASSERT_LE(delivered.size(), source.size());
            ASSERT_TRUE(std::equal(delivered.begin(), delivered.end(), source.begin()));
            ASSERT_EQ(receiver.grant(++revision, receiver.window()), Status::OK);
            auto const next = receiver.advertised_credit();
            if (random() % 5 != 0) {
                ASSERT_EQ(sender.credit(next), Status::OK);
                ASSERT_EQ(sender.credit(next), Status::REPLAY);
                if (prior_credit) EXPECT_EQ(sender.credit(*prior_credit), Status::STALE);
                prior_credit = next;
            } else {
                ++lost_credits;
            }
        }

        if (random() % 47 == 0) {
            auto const old_attachment = attachment++;
            ASSERT_TRUE(sender.detach(old_attachment));
            ASSERT_EQ(sender.attach(attachment, receiver.consumed(), receiver.window()), Status::OK);
            ASSERT_EQ(receiver.attach(attachment), Status::OK);
            if (pending) EXPECT_FALSE(sender.finish(pending->frame));
            EXPECT_EQ(sender.credit({old_attachment, 1, 0, Capacity}), Status::STALE);
            EXPECT_EQ(receiver.receive(old_attachment, 0, SOURCE, false), Status::STALE);
            pending.reset();
            prior_credit.reset();
            revision = 0;
            ++reattachments;
        }

        if (!pending) {
            std::array<uint8_t, 47> packet{};
            auto const prepared = sender.prepare(attachment, packet);
            if (prepared.status == Status::READY) {
                pending = OwnedPacket{prepared.frame, {packet.begin(), packet.begin() + static_cast<ptrdiff_t>(prepared.frame.bytes)}};
            } else {
                EXPECT_TRUE(prepared.status == Status::BLOCKED || prepared.status == Status::EMPTY || prepared.status == Status::END);
            }
        }
        EXPECT_LE(sender.buffered(), Capacity);
        EXPECT_LE(receiver.buffered(), Capacity);
        EXPECT_EQ(receiver.consumed(), delivered.size());
        EXPECT_LE(sender.retained(), receiver.consumed());
    }

    auto done() const -> bool { return receiver.ended() && delivered == source && sender.retained() == source.size(); }
    unsigned reattachments{};
    unsigned tx_refusals{};
    unsigned lost_credits{};

   private:
    jobs::SendStream<Capacity> sender;
    jobs::ReceiveStream<Capacity> receiver;
    std::vector<uint8_t> source;
    std::vector<uint8_t> delivered;
    size_t written{};
    uint64_t attachment{1};
    uint64_t revision{};
    std::optional<OwnedPacket> pending;
    std::optional<jobs::StreamCredit> prior_credit;
};
}  // namespace

TEST(WkiJobStream, LargeIndependentStreamsSurviveLossDuplicatesReorderingAndReattachment) {
    for (unsigned seed = 0; seed < 16; ++seed) {
        std::mt19937 random(seed);
        StreamTrace<257> out(8193, 17);
        StreamTrace<113> err(12007, 91);
        for (unsigned event = 0; event < 100000 && !(out.done() && err.done()); ++event) {
            if (!out.done()) out.step(random);
            if (!err.done()) err.step(random);
        }
        ASSERT_TRUE(out.done());
        ASSERT_TRUE(err.done());
        EXPECT_GT(out.reattachments, 0U);
        EXPECT_GT(err.reattachments, 0U);
        EXPECT_GT(out.tx_refusals, 0U);
        EXPECT_GT(err.tx_refusals, 0U);
        EXPECT_GT(out.lost_credits, 0U);
        EXPECT_GT(err.lost_credits, 0U);
    }
}
