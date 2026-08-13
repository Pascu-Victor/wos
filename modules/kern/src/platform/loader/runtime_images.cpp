#include "runtime_images.hpp"

#include <extern/elf.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

namespace ker::loader::runtime {
namespace {

constexpr uint64_t USER_ADDRESS_TOP = 0x0000800000000000ULL;
constexpr uint64_t PAGE_SIZE = 4096;
constexpr size_t MAX_PROGRAM_HEADERS = 128;
constexpr size_t MAX_DYNAMIC_ENTRIES = 4096;
constexpr size_t MAX_DYNAMIC_SYMBOLS = 4096;
constexpr size_t MAX_GNU_HASH_BUCKETS = 4096;
constexpr size_t MAX_GNU_HASH_BLOOM_WORDS = 4096;
constexpr size_t MAX_NOTE_BYTES = 64ULL * 1024;
constexpr uint32_t NT_GNU_BUILD_ID_VALUE = 3;
constexpr int64_t DT_GNU_HASH_VALUE = 0x6ffffef5;
constexpr std::string_view DEBUG_POINTER_SYMBOL = "_dl_debug_addr";

// These layouts are the GDB ABI used by mlibc's DebugInterface and LinkMap.
struct TargetDebugInterface {
    int32_t version{};
    uint32_t padding0{};
    uint64_t head{};
    uint64_t breakpoint{};
    int32_t state{};
    uint32_t padding1{};
    uint64_t loader_base{};
};
static_assert(sizeof(TargetDebugInterface) == 40);

struct TargetLinkMap {
    uint64_t base{};
    uint64_t name{};
    uint64_t dynamic{};
    uint64_t next{};
    uint64_t previous{};
};
static_assert(sizeof(TargetLinkMap) == 40);

struct TargetNoteHeader {
    uint32_t name_size{};
    uint32_t descriptor_size{};
    uint32_t type{};
};
static_assert(sizeof(TargetNoteHeader) == 12);

struct TargetGnuHashHeader {
    uint32_t bucket_count{};
    uint32_t symbol_offset{};
    uint32_t bloom_word_count{};
    uint32_t bloom_shift{};
};
static_assert(sizeof(TargetGnuHashHeader) == 16);

[[nodiscard]] auto checked_add(uint64_t left, uint64_t right, uint64_t& result) -> bool {
    if (right > std::numeric_limits<uint64_t>::max() - left) {
        return false;
    }
    result = left + right;
    return true;
}

[[nodiscard]] auto checked_mul(uint64_t left, uint64_t right, uint64_t& result) -> bool {
    if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
        return false;
    }
    result = left * right;
    return true;
}

[[nodiscard]] auto align_up_4(uint64_t value, uint64_t& result) -> bool {
    if (!checked_add(value, 3, result)) {
        return false;
    }
    result &= ~3ULL;
    return true;
}

[[nodiscard]] auto page_align_down(uint64_t value) -> uint64_t { return value & ~(PAGE_SIZE - 1); }

[[nodiscard]] auto page_align_up(uint64_t value, uint64_t& result) -> bool {
    if (!checked_add(value, PAGE_SIZE - 1, result)) {
        return false;
    }
    result &= ~(PAGE_SIZE - 1);
    return true;
}

[[nodiscard]] auto user_range_valid(uint64_t address, size_t size) -> bool {
    return address < USER_ADDRESS_TOP && size <= USER_ADDRESS_TOP && address <= USER_ADDRESS_TOP - size;
}

[[nodiscard]] auto read_target(TargetReader reader, void* opaque, uint64_t address, void* destination, size_t size) -> bool {
    return reader != nullptr && destination != nullptr && user_range_valid(address, size) && reader(opaque, address, destination, size);
}

void copy_path(std::array<char, IMAGE_PATH_MAX>& destination, const char* source) {
    destination.fill('\0');
    if (source != nullptr) {
        std::strncpy(destination.data(), source, destination.size() - 1);
    }
}

[[nodiscard]] auto read_path(TargetReader reader, void* opaque, uint64_t address, std::array<char, IMAGE_PATH_MAX>& destination) -> bool {
    destination.fill('\0');
    if (address == 0) {
        return true;
    }
    for (size_t i = 0; i + 1 < destination.size(); ++i) {
        if (!read_target(reader, opaque, address + i, &destination.at(i), 1)) {
            destination.fill('\0');
            return false;
        }
        if (destination.at(i) == '\0') {
            return true;
        }
    }
    destination.back() = '\0';
    return false;
}

[[nodiscard]] constexpr auto gnu_symbol_hash(std::string_view name) -> uint32_t {
    uint32_t hash = 5381;
    for (unsigned char byte : name) {
        hash = (hash * 33U) + byte;
    }
    return hash;
}

[[nodiscard]] auto target_string_equals(TargetReader reader, void* opaque, uint64_t address, uint64_t maximum_size,
                                        std::string_view expected) -> bool {
    if (maximum_size <= expected.size()) {
        return false;
    }
    for (size_t i = 0; i <= expected.size(); ++i) {
        char observed = '\0';
        if (!read_target(reader, opaque, address + i, &observed, sizeof(observed))) {
            return false;
        }
        char const WANTED = i == expected.size() ? '\0' : expected.at(i);
        if (observed != WANTED) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] auto image_index_for_base(const std::array<RuntimeImage, MAX_IMAGES>& images, size_t count, uint64_t base) -> size_t {
    for (size_t i = 0; i < count; ++i) {
        if (images.at(i).load_base == base) {
            return i;
        }
    }
    return count;
}

[[nodiscard]] auto append_fallback(std::array<RuntimeImage, MAX_IMAGES>& images, size_t& count, uint64_t base, uint64_t start, uint64_t end,
                                   uint64_t entry, const char* path, uint32_t flags) -> bool {
    if (start >= end || end > USER_ADDRESS_TOP || count >= images.size()) {
        return false;
    }
    RuntimeImage image{};
    image.load_base = base;
    image.image_start = start;
    image.image_end = end;
    image.entry = entry;
    image.flags = flags | IMAGE_EXACT_MAPPING;
    copy_path(image.path, path);
    images.at(count++) = image;
    return true;
}

[[nodiscard]] auto read_build_id(TargetReader reader, void* opaque, uint64_t note_address, uint64_t note_size, RuntimeImage& image)
    -> bool {
    if (note_size > MAX_NOTE_BYTES || !user_range_valid(note_address, static_cast<size_t>(note_size))) {
        return false;
    }
    uint64_t offset = 0;
    while (offset < note_size) {
        if (note_size - offset < sizeof(TargetNoteHeader)) {
            return false;
        }
        TargetNoteHeader header{};
        if (!read_target(reader, opaque, note_address + offset, &header, sizeof(header))) {
            return false;
        }
        offset += sizeof(header);
        uint64_t aligned_name = 0;
        uint64_t aligned_descriptor = 0;
        if (!align_up_4(header.name_size, aligned_name) || !align_up_4(header.descriptor_size, aligned_descriptor) ||
            aligned_name > note_size - offset) {
            return false;
        }
        uint64_t const NAME_ADDRESS = note_address + offset;
        offset += aligned_name;
        if (aligned_descriptor > note_size - offset) {
            return false;
        }
        uint64_t const DESCRIPTOR_ADDRESS = note_address + offset;
        offset += aligned_descriptor;

        std::array<char, 4> name{};
        bool const GNU_NAME = header.name_size == 4 && read_target(reader, opaque, NAME_ADDRESS, name.data(), name.size()) &&
                              name.at(0) == 'G' && name.at(1) == 'N' && name.at(2) == 'U' && name.at(3) == '\0';
        if (GNU_NAME && header.type == NT_GNU_BUILD_ID_VALUE && header.descriptor_size != 0 &&
            header.descriptor_size <= image.build_id.size()) {
            if (!read_target(reader, opaque, DESCRIPTOR_ADDRESS, image.build_id.data(), header.descriptor_size)) {
                return false;
            }
            image.build_id_size = static_cast<uint8_t>(header.descriptor_size);
            image.flags |= IMAGE_BUILD_ID_PRESENT;
            return true;
        }
    }
    return true;
}

[[nodiscard]] auto inspect_runtime_elf(TargetReader reader, void* opaque, uint64_t header_address, uint64_t load_base, RuntimeImage& image)
    -> bool {
    Elf64_Ehdr header{};
    if (!read_target(reader, opaque, header_address, &header, sizeof(header)) || header.e_ident[EI_MAG0] != ELFMAG0 ||
        header.e_ident[EI_MAG1] != ELFMAG1 || header.e_ident[EI_MAG2] != ELFMAG2 || header.e_ident[EI_MAG3] != ELFMAG3 ||
        header.e_ident[EI_CLASS] != ELFCLASS64 || header.e_phentsize != sizeof(Elf64_Phdr) || header.e_phnum == 0 ||
        header.e_phnum > MAX_PROGRAM_HEADERS) {
        return false;
    }

    uint64_t phdr_bytes = 0;
    uint64_t phdr_address = 0;
    if (!checked_mul(header.e_phnum, sizeof(Elf64_Phdr), phdr_bytes) || !checked_add(header_address, header.e_phoff, phdr_address) ||
        !user_range_valid(phdr_address, static_cast<size_t>(phdr_bytes))) {
        return false;
    }

    uint64_t image_start = std::numeric_limits<uint64_t>::max();
    uint64_t image_end = 0;
    uint64_t text_start = std::numeric_limits<uint64_t>::max();
    uint64_t text_end = 0;
    bool saw_load = false;
    for (Elf64_Half i = 0; i < header.e_phnum; ++i) {
        Elf64_Phdr ph{};
        uint64_t address = 0;
        if (!checked_add(phdr_address, static_cast<uint64_t>(i) * sizeof(Elf64_Phdr), address) ||
            !read_target(reader, opaque, address, &ph, sizeof(ph))) {
            return false;
        }
        if (ph.p_type == PT_LOAD && ph.p_memsz != 0) {
            uint64_t runtime_start = 0;
            uint64_t runtime_limit = 0;
            uint64_t runtime_end = 0;
            if (!checked_add(load_base, ph.p_vaddr, runtime_start) || !checked_add(runtime_start, ph.p_memsz, runtime_limit) ||
                !page_align_up(runtime_limit, runtime_end) || runtime_end > USER_ADDRESS_TOP) {
                return false;
            }
            runtime_start = page_align_down(runtime_start);
            image_start = std::min(image_start, runtime_start);
            image_end = std::max(image_end, runtime_end);
            saw_load = true;
            if ((ph.p_flags & PF_X) != 0U) {
                text_start = std::min(text_start, runtime_start);
                text_end = std::max(text_end, runtime_end);
            }
        } else if (ph.p_type == PT_DYNAMIC) {
            if (!checked_add(load_base, ph.p_vaddr, image.dynamic_addr)) {
                return false;
            }
        } else if (ph.p_type == PT_NOTE && ph.p_filesz != 0 && image.build_id_size == 0) {
            uint64_t note_address = 0;
            if (!checked_add(load_base, ph.p_vaddr, note_address) || !read_build_id(reader, opaque, note_address, ph.p_filesz, image)) {
                return false;
            }
        }
    }
    if (!saw_load || image_start >= image_end) {
        return false;
    }
    uint64_t entry = 0;
    if (!checked_add(load_base, header.e_entry, entry) || entry >= USER_ADDRESS_TOP) {
        return false;
    }
    image.image_start = image_start;
    image.image_end = image_end;
    image.text_start = text_start == std::numeric_limits<uint64_t>::max() ? 0 : text_start;
    image.text_end = text_end;
    image.entry = entry;
    image.flags |= IMAGE_EXACT_MAPPING;
    return true;
}

[[nodiscard]] auto find_rodynamic_debug_interface(TargetReader reader, void* opaque, uint64_t interpreter_base, uint64_t& address,
                                                  uint64_t& pointer_address) -> bool {
    Elf64_Ehdr header{};
    if (!read_target(reader, opaque, interpreter_base, &header, sizeof(header)) || header.e_ident[EI_MAG0] != ELFMAG0 ||
        header.e_ident[EI_MAG1] != ELFMAG1 || header.e_ident[EI_MAG2] != ELFMAG2 || header.e_ident[EI_MAG3] != ELFMAG3 ||
        header.e_ident[EI_CLASS] != ELFCLASS64 || header.e_type != ET_DYN || header.e_phentsize != sizeof(Elf64_Phdr) ||
        header.e_phnum == 0 || header.e_phnum > MAX_PROGRAM_HEADERS) {
        return false;
    }

    uint64_t phdr_address = 0;
    if (!checked_add(interpreter_base, header.e_phoff, phdr_address)) {
        return false;
    }
    Elf64_Phdr dynamic_header{};
    bool found_dynamic = false;
    for (Elf64_Half i = 0; i < header.e_phnum; ++i) {
        Elf64_Phdr ph{};
        uint64_t entry_address = 0;
        if (!checked_add(phdr_address, static_cast<uint64_t>(i) * sizeof(Elf64_Phdr), entry_address) ||
            !read_target(reader, opaque, entry_address, &ph, sizeof(ph))) {
            return false;
        }
        if (ph.p_type == PT_DYNAMIC) {
            if (found_dynamic || ph.p_memsz < sizeof(Elf64_Dyn)) {
                return false;
            }
            dynamic_header = ph;
            found_dynamic = true;
        }
    }
    if (!found_dynamic) {
        return false;
    }

    uint64_t dynamic_address = 0;
    if (!checked_add(interpreter_base, dynamic_header.p_vaddr, dynamic_address)) {
        return false;
    }
    uint64_t symbol_table_value = 0;
    uint64_t symbol_entry_size = 0;
    uint64_t string_table_value = 0;
    uint64_t string_table_size = 0;
    uint64_t gnu_hash_value = 0;
    bool terminated = false;
    size_t const ENTRY_COUNT = static_cast<size_t>(std::min<uint64_t>(dynamic_header.p_memsz / sizeof(Elf64_Dyn), MAX_DYNAMIC_ENTRIES));
    for (size_t i = 0; i < ENTRY_COUNT; ++i) {
        Elf64_Dyn entry{};
        uint64_t entry_address = 0;
        if (!checked_add(dynamic_address, i * sizeof(Elf64_Dyn), entry_address) ||
            !read_target(reader, opaque, entry_address, &entry, sizeof(entry))) {
            return false;
        }
        if (entry.d_tag == DT_NULL) {
            terminated = true;
            break;
        }
        switch (entry.d_tag) {
            case DT_SYMTAB:
                symbol_table_value = entry.d_un.d_val;
                break;
            case DT_SYMENT:
                symbol_entry_size = entry.d_un.d_val;
                break;
            case DT_STRTAB:
                string_table_value = entry.d_un.d_val;
                break;
            case DT_STRSZ:
                string_table_size = entry.d_un.d_val;
                break;
            case DT_GNU_HASH_VALUE:
                gnu_hash_value = entry.d_un.d_val;
                break;
            default:
                break;
        }
    }
    if (!terminated || symbol_table_value == 0 || symbol_entry_size != sizeof(Elf64_Sym) || string_table_value == 0 ||
        string_table_size == 0 || gnu_hash_value == 0) {
        return false;
    }

    uint64_t symbol_table = 0;
    uint64_t string_table = 0;
    uint64_t gnu_hash_address = 0;
    if (!checked_add(interpreter_base, symbol_table_value, symbol_table) ||
        !checked_add(interpreter_base, string_table_value, string_table) ||
        !checked_add(interpreter_base, gnu_hash_value, gnu_hash_address)) {
        return false;
    }

    TargetGnuHashHeader hash_header{};
    if (!read_target(reader, opaque, gnu_hash_address, &hash_header, sizeof(hash_header)) || hash_header.bucket_count == 0 ||
        hash_header.bucket_count > MAX_GNU_HASH_BUCKETS || hash_header.bloom_word_count == 0 ||
        hash_header.bloom_word_count > MAX_GNU_HASH_BLOOM_WORDS || hash_header.symbol_offset >= MAX_DYNAMIC_SYMBOLS) {
        return false;
    }

    uint64_t bloom_bytes = 0;
    uint64_t buckets_address = 0;
    if (!checked_mul(hash_header.bloom_word_count, sizeof(uint64_t), bloom_bytes) ||
        !checked_add(gnu_hash_address, sizeof(TargetGnuHashHeader), buckets_address) ||
        !checked_add(buckets_address, bloom_bytes, buckets_address)) {
        return false;
    }
    uint32_t const WANTED_HASH = gnu_symbol_hash(DEBUG_POINTER_SYMBOL);
    uint64_t bucket_address = 0;
    if (!checked_add(buckets_address, static_cast<uint64_t>(WANTED_HASH % hash_header.bucket_count) * sizeof(uint32_t), bucket_address)) {
        return false;
    }
    uint32_t symbol_index = 0;
    if (!read_target(reader, opaque, bucket_address, &symbol_index, sizeof(symbol_index)) || symbol_index < hash_header.symbol_offset ||
        symbol_index >= MAX_DYNAMIC_SYMBOLS) {
        return false;
    }

    uint64_t bucket_bytes = 0;
    uint64_t chains_address = 0;
    if (!checked_mul(hash_header.bucket_count, sizeof(uint32_t), bucket_bytes) ||
        !checked_add(buckets_address, bucket_bytes, chains_address)) {
        return false;
    }
    for (size_t step = 0; step < MAX_DYNAMIC_SYMBOLS; ++step) {
        uint64_t chain_address = 0;
        if (!checked_add(chains_address, static_cast<uint64_t>(symbol_index - hash_header.symbol_offset) * sizeof(uint32_t),
                         chain_address)) {
            return false;
        }
        uint32_t chain_hash = 0;
        if (!read_target(reader, opaque, chain_address, &chain_hash, sizeof(chain_hash))) {
            return false;
        }
        if ((chain_hash | 1U) == (WANTED_HASH | 1U)) {
            uint64_t symbol_address = 0;
            Elf64_Sym symbol{};
            if (!checked_add(symbol_table, static_cast<uint64_t>(symbol_index) * sizeof(Elf64_Sym), symbol_address) ||
                !read_target(reader, opaque, symbol_address, &symbol, sizeof(symbol)) || symbol.st_name >= string_table_size) {
                return false;
            }
            uint64_t name_address = 0;
            if (!checked_add(string_table, symbol.st_name, name_address)) {
                return false;
            }
            if (target_string_equals(reader, opaque, name_address, string_table_size - symbol.st_name, DEBUG_POINTER_SYMBOL)) {
                uint8_t const BINDING = ELF64_ST_BIND(symbol.st_info);
                if ((BINDING != STB_GLOBAL && BINDING != STB_WEAK) || ELF64_ST_TYPE(symbol.st_info) != STT_OBJECT ||
                    symbol.st_shndx == SHN_UNDEF || symbol.st_size < sizeof(uint64_t) ||
                    !checked_add(interpreter_base, symbol.st_value, pointer_address)) {
                    return false;
                }
                return read_target(reader, opaque, pointer_address, &address, sizeof(address)) && user_range_valid(address, 1);
            }
        }
        if ((chain_hash & 1U) != 0U || symbol_index + 1 >= MAX_DYNAMIC_SYMBOLS) {
            return false;
        }
        ++symbol_index;
    }
    return false;
}

[[nodiscard]] auto find_debug_interface(TargetReader reader, void* opaque, const SnapshotSource& source, uint64_t& address,
                                        uint64_t& pointer_address, bool& dynamic_image) -> bool {
    address = 0;
    pointer_address = 0;
    dynamic_image = source.interpreter_base != 0;
    if (!dynamic_image) {
        return true;
    }
    if (source.program_header_count > MAX_PROGRAM_HEADERS ||
        (source.program_header_count != 0 && source.program_header_ent_size != sizeof(Elf64_Phdr))) {
        return false;
    }
    for (uint64_t i = 0; i < source.program_header_count; ++i) {
        uint64_t phdr_address = 0;
        if (!checked_add(source.program_header_addr, i * sizeof(Elf64_Phdr), phdr_address)) {
            return false;
        }
        Elf64_Phdr ph{};
        if (!read_target(reader, opaque, phdr_address, &ph, sizeof(ph))) {
            return false;
        }
        if (ph.p_type != PT_DYNAMIC) {
            continue;
        }
        if (ph.p_memsz == 0) {
            continue;
        }
        uint64_t dynamic_address = 0;
        if (!checked_add(source.main_load_base, ph.p_vaddr, dynamic_address)) {
            return false;
        }
        size_t const ENTRY_COUNT = static_cast<size_t>(std::min<uint64_t>(ph.p_memsz / sizeof(Elf64_Dyn), MAX_DYNAMIC_ENTRIES));
        for (size_t index = 0; index < ENTRY_COUNT; ++index) {
            Elf64_Dyn entry{};
            uint64_t entry_address = 0;
            if (!checked_add(dynamic_address, index * sizeof(Elf64_Dyn), entry_address) ||
                !read_target(reader, opaque, entry_address, &entry, sizeof(entry))) {
                return false;
            }
            if (entry.d_tag == DT_NULL) {
                break;
            }
            if (entry.d_tag == DT_DEBUG) {
                address = entry.d_un.d_ptr;
                return true;
            }
        }
        break;
    }
    // WOS deliberately links PIEs with -z rodynamic to keep the complete
    // dynamic section read-only. LLD therefore omits DT_DEBUG. mlibc exposes
    // the equivalent GDB rendezvous through this fixed exported pointer; find
    // it through the interpreter's bounded in-memory GNU hash table.
    return find_rodynamic_debug_interface(reader, opaque, source.interpreter_base, address, pointer_address);
}

}  // namespace

auto snapshot_images(TargetReader reader, void* opaque, const SnapshotSource& source, std::array<RuntimeImage, MAX_IMAGES>& images,
                     size_t& count) -> SnapshotStatus {
    images = {};
    count = 0;
    (void)append_fallback(images, count, source.main_load_base, source.main_image_start, source.main_image_end, source.main_entry,
                          source.main_path, IMAGE_MAIN);
    if (source.interpreter_base != 0) {
        (void)append_fallback(images, count, source.interpreter_base, source.interpreter_image_start, source.interpreter_image_end, 0,
                              source.interpreter_path, IMAGE_INTERPRETER);
    }
    size_t const FALLBACK_COUNT = count;

    uint64_t debug_address = 0;
    uint64_t debug_pointer_address = 0;
    bool dynamic_image = false;
    if (!find_debug_interface(reader, opaque, source, debug_address, debug_pointer_address, dynamic_image)) {
        return SnapshotStatus::UNAVAILABLE;
    }
    if (!dynamic_image) {
        if (count != 0 && source.main_elf_header_addr != 0) {
            (void)inspect_runtime_elf(reader, opaque, source.main_elf_header_addr, source.main_load_base, images.at(0));
        }
        return SnapshotStatus::STATIC_IMAGE;
    }
    if (debug_address == 0) {
        return SnapshotStatus::UNAVAILABLE;
    }

    TargetDebugInterface debug{};
    if (!read_target(reader, opaque, debug_address, &debug, sizeof(debug)) || debug.version != 1) {
        return SnapshotStatus::UNAVAILABLE;
    }
    if (debug.state != 0) {
        return SnapshotStatus::INCONSISTENT;
    }

    std::array<uint64_t, MAX_IMAGES> visited{};
    std::array<TargetLinkMap, MAX_IMAGES> observed_links{};
    std::array<uint64_t, MAX_IMAGES> observed_bases{};
    size_t visited_count = 0;
    uint64_t cursor = debug.head;
    uint64_t previous_cursor = 0;
    while (cursor != 0) {
        if (visited_count >= visited.size()) {
            count = FALLBACK_COUNT;
            return SnapshotStatus::TRUNCATED;
        }
        if (std::find(visited.begin(), visited.begin() + static_cast<ptrdiff_t>(visited_count), cursor) !=
            visited.begin() + static_cast<ptrdiff_t>(visited_count)) {
            count = FALLBACK_COUNT;
            return SnapshotStatus::INCONSISTENT;
        }
        size_t const OBSERVED_INDEX = visited_count;
        visited.at(visited_count++) = cursor;

        TargetLinkMap link{};
        if (!read_target(reader, opaque, cursor, &link, sizeof(link))) {
            count = FALLBACK_COUNT;
            return SnapshotStatus::UNAVAILABLE;
        }
        if (link.previous != previous_cursor ||
            std::find(observed_bases.begin(), observed_bases.begin() + static_cast<ptrdiff_t>(OBSERVED_INDEX), link.base) !=
                observed_bases.begin() + static_cast<ptrdiff_t>(OBSERVED_INDEX)) {
            count = FALLBACK_COUNT;
            return SnapshotStatus::INCONSISTENT;
        }
        observed_links.at(OBSERVED_INDEX) = link;
        observed_bases.at(OBSERVED_INDEX) = link.base;
        size_t index = image_index_for_base(images, count, link.base);
        if (index == count) {
            if (count >= images.size()) {
                count = FALLBACK_COUNT;
                return SnapshotStatus::TRUNCATED;
            }
            images.at(count).load_base = link.base;
            images.at(count).flags = link.base == source.interpreter_base ? IMAGE_INTERPRETER : IMAGE_SHARED_OBJECT;
            index = count++;
        }
        auto& image = images.at(index);
        image.dynamic_addr = link.dynamic;
        if (link.name != 0) {
            std::array<char, IMAGE_PATH_MAX> observed_path{};
            if (!read_path(reader, opaque, link.name, observed_path)) {
                count = FALLBACK_COUNT;
                return SnapshotStatus::TRUNCATED;
            }
            if (observed_path.front() != '\0') {
                image.path = observed_path;
            }
        }
        uint64_t header_address = link.base;
        if ((image.flags & IMAGE_MAIN) != 0U) {
            header_address = source.main_elf_header_addr;
        }
        if (header_address == 0 || !inspect_runtime_elf(reader, opaque, header_address, link.base, image)) {
            // Never publish a guessed or half-populated DSO record. Main and
            // interpreter fallback extents are kernel-owned metadata, but the
            // link_map alone cannot establish a shared object's full mapping.
            count = FALLBACK_COUNT;
            return SnapshotStatus::UNAVAILABLE;
        }
        previous_cursor = cursor;
        cursor = link.next;
    }

    for (size_t i = 0; i < visited_count; ++i) {
        TargetLinkMap verified_link{};
        if (!read_target(reader, opaque, visited.at(i), &verified_link, sizeof(verified_link))) {
            count = FALLBACK_COUNT;
            return SnapshotStatus::UNAVAILABLE;
        }
        if (std::memcmp(&verified_link, &observed_links.at(i), sizeof(verified_link)) != 0) {
            count = FALLBACK_COUNT;
            return SnapshotStatus::INCONSISTENT;
        }
    }

    TargetDebugInterface verified{};
    uint64_t verified_debug_address = debug_address;
    if (debug_pointer_address != 0 &&
        (!read_target(reader, opaque, debug_pointer_address, &verified_debug_address, sizeof(verified_debug_address)) ||
         verified_debug_address != debug_address)) {
        count = FALLBACK_COUNT;
        return SnapshotStatus::INCONSISTENT;
    }
    if (!read_target(reader, opaque, debug_address, &verified, sizeof(verified))) {
        count = FALLBACK_COUNT;
        return SnapshotStatus::UNAVAILABLE;
    }
    if (std::memcmp(&verified, &debug, sizeof(verified)) != 0 || verified.state != 0) {
        count = FALLBACK_COUNT;
        return SnapshotStatus::INCONSISTENT;
    }

    return SnapshotStatus::COMPLETE;
}

#ifdef WOS_SELFTEST
auto selftest_find_debug_interface(TargetReader reader, void* opaque, const SnapshotSource& source, uint64_t& address) -> bool {
    uint64_t pointer_address = 0;
    bool dynamic_image = false;
    return find_debug_interface(reader, opaque, source, address, pointer_address, dynamic_image) && dynamic_image && address != 0;
}
#endif

}  // namespace ker::loader::runtime
