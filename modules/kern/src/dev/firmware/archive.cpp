#include "archive.hpp"

#include <array>
#include <optional>

namespace ker::dev::firmware {
namespace {
constexpr size_t HEADER_BYTES = 110;
constexpr uint32_t TYPE_MASK = 0170000;
constexpr uint32_t REGULAR = 0100000;
constexpr uint32_t DIRECTORY = 0040000;
constexpr uint32_t SYMLINK = 0120000;
constexpr std::string_view FIRMWARE_PREFIX = "lib/firmware/";

auto fail(Error error, size_t offset) -> ArchiveResult { return {.error = error, .offset = offset}; }

auto hex_field(std::span<const uint8_t> source, size_t offset, uint32_t& value) -> bool {
    if (offset > source.size() || source.size() - offset < 8) return false;
    value = 0;
    for (size_t i = 0; i < 8; ++i) {
        uint8_t const BYTE = source[offset + i];
        uint32_t digit = 0;
        if (BYTE >= '0' && BYTE <= '9')
            digit = BYTE - '0';
        else if (BYTE >= 'a' && BYTE <= 'f')
            digit = BYTE - 'a' + 10U;
        else if (BYTE >= 'A' && BYTE <= 'F')
            digit = BYTE - 'A' + 10U;
        else
            return false;
        value = (value << 4U) | digit;
    }
    return true;
}

auto components(std::string_view name) -> bool {
    if (name.empty() || name.front() == '/' || name.find('\0') != std::string_view::npos) return false;
    size_t begin = 0;
    for (size_t i = 0; i <= name.size(); ++i) {
        if (i != name.size() && name[i] != '/') continue;
        auto const PART = name.substr(begin, i - begin);
        if (PART.empty() || PART == "." || PART == "..") return false;
        begin = i + 1;
    }
    return true;
}

auto normalized(std::string_view name) -> std::string_view { return name.starts_with("./") ? name.substr(2) : name; }

auto aligned(std::span<const uint8_t> source, size_t offset) -> std::optional<size_t> {
    if (offset > source.size()) return std::nullopt;
    size_t const PADDING = (4U - (offset & 3U)) & 3U;
    if (PADDING > source.size() - offset) return std::nullopt;
    return offset + PADDING;
}

auto zero(std::span<const uint8_t> source, size_t begin, size_t end) -> bool {
    for (size_t i = begin; i < end; ++i)
        if (source[i] != 0) return false;
    return true;
}

struct Record {
    std::string_view name{};
    std::span<const uint8_t> bytes{};
    uint32_t mode = 0;
    uint32_t links = 0;
    size_t next = 0;
};

auto read_record(std::span<const uint8_t> source, size_t offset, Record& record) -> ArchiveResult {
    if (offset > source.size() || source.size() - offset < HEADER_BYTES) return fail(Error::truncated, offset);
    constexpr std::string_view MAGIC = "070701";
    for (size_t i = 0; i < MAGIC.size(); ++i)
        if (source[offset + i] != static_cast<uint8_t>(MAGIC[i])) return fail(Error::magic, offset + i);
    std::array<uint32_t, 13> fields{};
    for (size_t i = 0; i < fields.size(); ++i)
        if (!hex_field(source, offset + 6 + i * 8, fields[i])) return fail(Error::header, offset + 6 + i * 8);
    if (fields[12] != 0 || fields[4] == 0) return fail(Error::header, offset);
    size_t const NAME_SIZE = fields[11];
    if (NAME_SIZE == 0) return fail(Error::header, offset + 94);
    if (NAME_SIZE > MAX_ARCHIVE_NAME_BYTES) return fail(Error::limit, offset + 94);
    size_t const NAME_BEGIN = offset + HEADER_BYTES;
    if (NAME_SIZE > source.size() - NAME_BEGIN) return fail(Error::truncated, NAME_BEGIN);
    size_t const NAME_END = NAME_BEGIN + NAME_SIZE;
    if (source[NAME_END - 1] != 0) return fail(Error::path, NAME_END - 1);
    std::string_view const NAME{reinterpret_cast<const char*>(source.data() + NAME_BEGIN), NAME_SIZE - 1};
    if (NAME.find('\0') != std::string_view::npos) return fail(Error::path, NAME_BEGIN);
    auto const DATA_BEGIN = aligned(source, NAME_END);
    if (!DATA_BEGIN) return fail(Error::truncated, NAME_END);
    if (!zero(source, NAME_END, *DATA_BEGIN)) return fail(Error::header, NAME_END);
    size_t const FILE_SIZE = fields[6];
    if (FILE_SIZE > source.size() - *DATA_BEGIN) return fail(Error::truncated, *DATA_BEGIN);
    size_t const DATA_END = *DATA_BEGIN + FILE_SIZE;
    auto const NEXT = aligned(source, DATA_END);
    if (!NEXT) return fail(Error::truncated, DATA_END);
    if (!zero(source, DATA_END, *NEXT)) return fail(Error::header, DATA_END);
    record = {NAME, source.subspan(*DATA_BEGIN, FILE_SIZE), fields[1], fields[4], *NEXT};
    return {};
}

// All earlier records passed read_record. Only fields needed to advance and
// compare names are decoded again; bounds remain checked before view creation.
auto previous_name(std::span<const uint8_t> prior, size_t offset, std::string_view& name, size_t& next) -> bool {
    if (offset > prior.size() || prior.size() - offset < HEADER_BYTES) return false;
    uint32_t name_size = 0, file_size = 0;
    if (!hex_field(prior, offset + 94, name_size) || !hex_field(prior, offset + 54, file_size) || name_size == 0) return false;
    size_t const NAME_BEGIN = offset + HEADER_BYTES;
    if (name_size > prior.size() - NAME_BEGIN) return false;
    auto const DATA_BEGIN = aligned(prior, NAME_BEGIN + name_size);
    if (!DATA_BEGIN || file_size > prior.size() - *DATA_BEGIN) return false;
    auto const NEXT = aligned(prior, *DATA_BEGIN + file_size);
    if (!NEXT) return false;
    name = normalized({reinterpret_cast<const char*>(prior.data() + NAME_BEGIN), name_size - 1});
    next = *NEXT;
    return true;
}

auto known_type(uint32_t mode) -> bool {
    if ((mode & ~(TYPE_MASK | 07777U)) != 0) return false;
    auto const TYPE = mode & TYPE_MASK;
    return TYPE == 0010000 || TYPE == 0020000 || TYPE == DIRECTORY || TYPE == 0060000 || TYPE == REGULAR || TYPE == 0120000 ||
           TYPE == 0140000;
}
}  // namespace

auto canonical_name(std::string_view name) -> bool { return name.size() < MAX_NAME_BYTES && components(name); }

auto scan_archive(std::span<const uint8_t> source, std::span<ArchiveEntry> private_output) -> ArchiveResult {
    if (source.size() > MAX_SOURCE_BYTES) return fail(Error::source_limit, 0);
    if (source.empty() || source.data() == nullptr) return fail(Error::truncated, 0);
    if (!private_output.empty() && private_output.data() == nullptr) return fail(Error::limit, 0);
    size_t offset = 0, count = 0, records = 0;
    while (offset < source.size()) {
        if (records++ == MAX_RECORDS) return fail(Error::limit, offset);
        Record record;
        auto const PARSED = read_record(source, offset, record);
        if (!PARSED.valid()) return PARSED;
        if (record.name == "TRAILER!!!") {
            if (record.mode != 0 || record.links != 1 || !record.bytes.empty()) return fail(Error::trailer, offset);
            size_t const TAIL = source.size() - record.next;
            if (TAIL > MAX_TRAILER_PADDING || !zero(source, record.next, source.size())) return fail(Error::trailer, record.next);
            return {.count = count};
        }
        auto const NAME = normalized(record.name);
        if (NAME == "TRAILER!!!") return fail(Error::trailer, offset);
        auto const TYPE = record.mode & TYPE_MASK;
        if (!known_type(record.mode) || (TYPE != REGULAR && TYPE != SYMLINK && !record.bytes.empty()))
            return fail(Error::entry_type, offset);
        if (NAME != "." && !components(NAME)) return fail(Error::path, offset);
        if (NAME == "." && TYPE != DIRECTORY) return fail(Error::entry_type, offset);
        for (size_t previous = 0; previous < offset;) {
            std::string_view prior_name;
            size_t next = 0;
            if (!previous_name(source.first(offset), previous, prior_name, next)) return fail(Error::header, previous);
            if (NAME == prior_name) return fail(Error::duplicate, offset);
            previous = next;
        }
        if (NAME.starts_with(FIRMWARE_PREFIX)) {
            auto const FIRMWARE_NAME = NAME.substr(FIRMWARE_PREFIX.size());
            if (!canonical_name(FIRMWARE_NAME)) return fail(Error::path, offset);
            if (TYPE != DIRECTORY) {
                if (TYPE != REGULAR || record.links != 1) return fail(Error::entry_type, offset);
                if (record.bytes.size() > MAX_BLOB_BYTES) return fail(Error::length, offset);
                if (count == MAX_INDEX_ENTRIES || count == private_output.size()) return fail(Error::limit, offset);
                private_output[count++] = {FIRMWARE_NAME, record.bytes};
            }
        }
        offset = record.next;
    }
    return fail(Error::trailer, offset);
}

}  // namespace ker::dev::firmware
