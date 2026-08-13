#include <extern/elf.h>

#include <array>
#include <cstdint>
#include <cstring>

#include "platform/loader/debug_info.hpp"
#include "platform/loader/elf_loader.hpp"
#include "platform/loader/runtime_images.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/user_layout.hpp"
#include "platform/mm/virt.hpp"
#include "platform/random/entropy.hpp"
#include "test/ktest.hpp"

namespace entropy = ker::mod::random::entropy;
namespace elf = ker::loader::elf;
namespace elf_debug = ker::loader::debug;
namespace runtime_images = ker::loader::runtime;
namespace paging = ker::mod::mm::paging;
namespace layout = ker::mod::mm::user_layout;
namespace virt = ker::mod::mm::virt;

namespace {

constexpr size_t ELF_FIXTURE_SIZE = paging::PAGE_SIZE;

struct ElfFixture {
    std::array<uint8_t, ELF_FIXTURE_SIZE> bytes{};
    Elf64_Ehdr header{};
    std::array<Elf64_Phdr, 8> program_headers{};
    size_t program_header_count{};
};

auto make_executable_fixture(uint16_t type, uint64_t image_vaddr) -> ElfFixture {
    ElfFixture fixture{};
    fixture.header.e_ident[EI_MAG0] = ELFMAG0;
    fixture.header.e_ident[EI_MAG1] = ELFMAG1;
    fixture.header.e_ident[EI_MAG2] = ELFMAG2;
    fixture.header.e_ident[EI_MAG3] = ELFMAG3;
    fixture.header.e_ident[EI_CLASS] = ELFCLASS64;
    fixture.header.e_ident[EI_DATA] = ELFDATA2LSB;
    fixture.header.e_ident[EI_VERSION] = EV_CURRENT;
    fixture.header.e_ident[EI_OSABI] = ELFOSABI_NONE;
    fixture.header.e_type = type;
    fixture.header.e_machine = EM_X86_64;
    fixture.header.e_version = EV_CURRENT;
    fixture.header.e_entry = image_vaddr + 0x200;
    fixture.header.e_phoff = sizeof(Elf64_Ehdr);
    fixture.header.e_ehsize = sizeof(Elf64_Ehdr);
    fixture.header.e_phentsize = sizeof(Elf64_Phdr);

    fixture.program_headers.at(0) = {
        .p_type = PT_LOAD,
        .p_flags = PF_R | PF_X,
        .p_offset = 0,
        .p_vaddr = image_vaddr,
        .p_paddr = image_vaddr,
        .p_filesz = ELF_FIXTURE_SIZE,
        .p_memsz = ELF_FIXTURE_SIZE,
        .p_align = paging::PAGE_SIZE,
    };
    fixture.program_headers.at(1) = {
        .p_type = PT_GNU_STACK,
        .p_flags = PF_R | PF_W,
        .p_offset = 0,
        .p_vaddr = 0,
        .p_paddr = 0,
        .p_filesz = 0,
        .p_memsz = 0,
        .p_align = 16,
    };
    fixture.program_header_count = 2;
    return fixture;
}

void sync_fixture(ElfFixture& fixture) {
    fixture.header.e_phnum = static_cast<Elf64_Half>(fixture.program_header_count);
    std::memcpy(fixture.bytes.data(), &fixture.header, sizeof(fixture.header));
    std::memcpy(fixture.bytes.data() + fixture.header.e_phoff, fixture.program_headers.data(),
                fixture.program_header_count * sizeof(Elf64_Phdr));
}

template <typename Load>
auto load_with_temporary_pagemap(uint64_t pid, Load&& load) -> elf::ElfLoadResult {
    auto* page_table = virt::create_pagemap();
    if (page_table == nullptr) {
        return {};
    }
    elf::ElfLoadResult const RESULT = load(page_table);
    elf_debug::unregister_process(pid);
    virt::destroy_user_space(page_table, pid, "address_space_ktest", "elf-fixture");
    virt::release_pagemap(page_table);
    return RESULT;
}

auto load_fixture(ElfFixture& fixture, uint64_t pid) -> elf::ElfLoadResult {
    sync_fixture(fixture);

    elf::ElfFileView const VIEW{
        .elf_header = fixture.header,
        .program_headers = fixture.program_headers.data(),
        .section_headers = nullptr,
        .section_names = nullptr,
        .section_names_size = 0,
        .read_at = nullptr,
        .read_context = nullptr,
        .logical_size = fixture.bytes.size(),
        .contiguous_base = fixture.bytes.data(),
    };
    return load_with_temporary_pagemap(
        pid, [&](virt::PageTable* page_table) { return elf::load_elf(VIEW, page_table, pid, "address_space_ktest"); });
}

auto load_contiguous_fixture(ElfFixture& fixture, uint64_t pid, size_t size = ELF_FIXTURE_SIZE) -> elf::ElfLoadResult {
    sync_fixture(fixture);
    return load_with_temporary_pagemap(pid, [&](virt::PageTable* page_table) {
        return elf::load_elf(fixture.bytes.data(), size, page_table, pid, "address_space_ktest");
    });
}

constexpr uint64_t RUNTIME_FIXTURE_BASE = 0x0000000030000000ULL;
constexpr size_t RUNTIME_FIXTURE_SIZE = paging::PAGE_SIZE;
constexpr size_t RUNTIME_DYNAMIC_OFFSET = 0x200;
constexpr size_t RUNTIME_GNU_HASH_OFFSET = 0x300;
constexpr size_t RUNTIME_SYMBOL_TABLE_OFFSET = 0x400;
constexpr size_t RUNTIME_STRING_TABLE_OFFSET = 0x500;
constexpr size_t RUNTIME_DEBUG_POINTER_OFFSET = 0x600;
constexpr size_t RUNTIME_DEBUG_INTERFACE_OFFSET = 0x700;
constexpr int64_t DT_GNU_HASH_VALUE = 0x6ffffef5;
constexpr char RUNTIME_DEBUG_SYMBOL[] = "\0_dl_debug_addr";

struct RuntimeResolverFixture {
    std::array<uint8_t, RUNTIME_FIXTURE_SIZE> bytes{};

    template <typename T>
    void store(size_t offset, const T& value) {
        std::memcpy(bytes.data() + offset, &value, sizeof(value));
    }
};

[[nodiscard]] constexpr auto test_gnu_hash(const char* name) -> uint32_t {
    uint32_t hash = 5381;
    while (*name != '\0') {
        hash = (hash * 33U) + static_cast<unsigned char>(*name++);
    }
    return hash;
}

auto make_runtime_resolver_fixture() -> RuntimeResolverFixture {
    RuntimeResolverFixture fixture{};
    Elf64_Ehdr header{};
    header.e_ident[EI_MAG0] = ELFMAG0;
    header.e_ident[EI_MAG1] = ELFMAG1;
    header.e_ident[EI_MAG2] = ELFMAG2;
    header.e_ident[EI_MAG3] = ELFMAG3;
    header.e_ident[EI_CLASS] = ELFCLASS64;
    header.e_type = ET_DYN;
    header.e_phoff = sizeof(Elf64_Ehdr);
    header.e_phentsize = sizeof(Elf64_Phdr);
    header.e_phnum = 1;
    fixture.store(0, header);

    std::array<Elf64_Dyn, 6> dynamic{};
    dynamic.at(0).d_tag = DT_SYMTAB;
    dynamic.at(0).d_un.d_val = RUNTIME_SYMBOL_TABLE_OFFSET;
    dynamic.at(1).d_tag = DT_SYMENT;
    dynamic.at(1).d_un.d_val = sizeof(Elf64_Sym);
    dynamic.at(2).d_tag = DT_STRTAB;
    dynamic.at(2).d_un.d_val = RUNTIME_STRING_TABLE_OFFSET;
    dynamic.at(3).d_tag = DT_STRSZ;
    dynamic.at(3).d_un.d_val = sizeof(RUNTIME_DEBUG_SYMBOL);
    dynamic.at(4).d_tag = DT_GNU_HASH_VALUE;
    dynamic.at(4).d_un.d_val = RUNTIME_GNU_HASH_OFFSET;
    dynamic.at(5).d_tag = DT_NULL;

    Elf64_Phdr program_header{
        .p_type = PT_DYNAMIC,
        .p_flags = PF_R,
        .p_offset = RUNTIME_DYNAMIC_OFFSET,
        .p_vaddr = RUNTIME_DYNAMIC_OFFSET,
        .p_paddr = RUNTIME_DYNAMIC_OFFSET,
        .p_filesz = sizeof(dynamic),
        .p_memsz = sizeof(dynamic),
        .p_align = alignof(Elf64_Dyn),
    };
    fixture.store(sizeof(Elf64_Ehdr), program_header);
    fixture.store(RUNTIME_DYNAMIC_OFFSET, dynamic);

    constexpr std::array<uint32_t, 4> GNU_HEADER{1, 1, 1, 0};
    fixture.store(RUNTIME_GNU_HASH_OFFSET, GNU_HEADER);
    uint64_t const BLOOM = 0;
    uint32_t const BUCKET = 1;
    uint32_t const CHAIN = test_gnu_hash(RUNTIME_DEBUG_SYMBOL + 1) | 1U;
    fixture.store(RUNTIME_GNU_HASH_OFFSET + sizeof(GNU_HEADER), BLOOM);
    fixture.store(RUNTIME_GNU_HASH_OFFSET + sizeof(GNU_HEADER) + sizeof(BLOOM), BUCKET);
    fixture.store(RUNTIME_GNU_HASH_OFFSET + sizeof(GNU_HEADER) + sizeof(BLOOM) + sizeof(BUCKET), CHAIN);

    std::array<Elf64_Sym, 2> symbols{};
    symbols.at(1).st_name = 1;
    symbols.at(1).st_info = static_cast<unsigned char>((STB_GLOBAL << 4U) | STT_OBJECT);
    symbols.at(1).st_shndx = 1;
    symbols.at(1).st_value = RUNTIME_DEBUG_POINTER_OFFSET;
    symbols.at(1).st_size = sizeof(uint64_t);
    fixture.store(RUNTIME_SYMBOL_TABLE_OFFSET, symbols);
    std::memcpy(fixture.bytes.data() + RUNTIME_STRING_TABLE_OFFSET, RUNTIME_DEBUG_SYMBOL, sizeof(RUNTIME_DEBUG_SYMBOL));

    uint64_t const DEBUG_ADDRESS = RUNTIME_FIXTURE_BASE + RUNTIME_DEBUG_INTERFACE_OFFSET;
    fixture.store(RUNTIME_DEBUG_POINTER_OFFSET, DEBUG_ADDRESS);
    return fixture;
}

auto read_runtime_resolver_fixture(void* opaque, uint64_t address, void* destination, size_t size) -> bool {
    auto& fixture = *static_cast<RuntimeResolverFixture*>(opaque);
    if (destination == nullptr || address < RUNTIME_FIXTURE_BASE) {
        return false;
    }
    uint64_t const OFFSET = address - RUNTIME_FIXTURE_BASE;
    if (OFFSET > fixture.bytes.size() || size > fixture.bytes.size() - static_cast<size_t>(OFFSET)) {
        return false;
    }
    std::memcpy(destination, fixture.bytes.data() + OFFSET, size);
    return true;
}

}  // namespace

KTEST(AddressSpace, ChaCha20KnownAnswer) { KEXPECT_TRUE(entropy::selftest_chacha20_known_answer()); }

KTEST(AddressSpace, UniformSelectionRejectsModuloBiasTail) {
    uint64_t output = UINT64_MAX;
    KEXPECT_FALSE(entropy::selftest_uniform_from_sample(0, 10, output));
    KEXPECT_FALSE(entropy::selftest_uniform_from_sample(5, 10, output));
    KEXPECT_TRUE(entropy::selftest_uniform_from_sample(6, 10, output));
    KEXPECT_EQ(output, 6U);
    KEXPECT_TRUE(entropy::selftest_uniform_from_sample(UINT64_MAX, 10, output));
    KEXPECT_EQ(output, 5U);
    KEXPECT_FALSE(entropy::selftest_uniform_from_sample(42, 0, output));
}

KTEST(AddressSpace, LiveDrbgIsReadyAndProducesDistinctOutput) {
    uint64_t first = 0;
    uint64_t second = 0;
    KREQUIRE_TRUE(entropy::is_ready());
    KREQUIRE_TRUE(entropy::get_bytes(&first, sizeof(first)));
    KREQUIRE_TRUE(entropy::get_bytes(&second, sizeof(second)));
    KEXPECT_NE(first, second);
    KEXPECT_FALSE(entropy::get_bytes(nullptr, 1));
    KEXPECT_TRUE(entropy::get_bytes(nullptr, 0));
}

KTEST(AddressSpace, PlacementWindowsAreOrderedAndDisjoint) {
    KEXPECT_TRUE(layout::MAIN_IMAGE_WINDOW.begin >= 0x08000000ULL);
    KEXPECT_TRUE(layout::MAIN_IMAGE_WINDOW.end <= layout::INTERPRETER_WINDOW.begin);
    KEXPECT_TRUE(layout::INTERPRETER_WINDOW.end <= layout::MMAP_WINDOW.begin);
    KEXPECT_TRUE(layout::MMAP_WINDOW.end <= layout::THREAD_WINDOW.begin);
    KEXPECT_TRUE(layout::THREAD_WINDOW.end <= 0x0000800000000000ULL);
}

KTEST(AddressSpace, DeterministicPlacementHonorsHalfOpenExtent) {
    constexpr layout::AddressWindow WINDOW{0x2000, 0xA000, 0x2000};
    uint64_t start = 0;
    KEXPECT_TRUE(layout::selftest_choose_aligned(WINDOW, 0x3000, 0, start));
    KEXPECT_EQ(start, 0x2000U);
    KEXPECT_TRUE(layout::selftest_choose_aligned(WINDOW, 0x3000, 2, start));
    KEXPECT_EQ(start, 0x6000U);
    KEXPECT_TRUE(layout::selftest_choose_aligned(WINDOW, 0x3000, 3, start));
    KEXPECT_EQ(start, 0x2000U);
    KEXPECT_FALSE(layout::selftest_choose_aligned(WINDOW, 0, 0, start));
    KEXPECT_FALSE(layout::selftest_choose_aligned(WINDOW, 0x9000, 0, start));
}

KTEST(AddressSpace, CollisionCheckIncludesReservedPages) {
    auto* page_table = virt::create_pagemap();
    KREQUIRE_NE(page_table, nullptr);
    constexpr uint64_t START = 0x0000000018000000ULL;
    constexpr uint64_t SPAN = 3 * paging::PAGE_SIZE;
    KEXPECT_TRUE(layout::range_is_free(page_table, START, SPAN));
    virt::reserve_page_range(page_table, START + paging::PAGE_SIZE, 1);
    KEXPECT_FALSE(layout::range_is_free(page_table, START, SPAN));
    KEXPECT_FALSE(layout::range_is_free(page_table, START + 1, SPAN));
    KEXPECT_FALSE(layout::range_is_free(page_table, UINT64_MAX - paging::PAGE_SIZE + 1, paging::PAGE_SIZE));
    virt::destroy_user_space(page_table, 0, "address_space_ktest", "reserved-collision");
    virt::release_pagemap(page_table);
}

KTEST(AddressSpace, RodynamicInterpreterUsesExportedDebugRendezvous) {
    auto fixture = make_runtime_resolver_fixture();
    runtime_images::SnapshotSource const SOURCE{.interpreter_base = RUNTIME_FIXTURE_BASE};
    uint64_t address = 0;
    KEXPECT_TRUE(runtime_images::selftest_find_debug_interface(read_runtime_resolver_fixture, &fixture, SOURCE, address));
    KEXPECT_EQ(address, RUNTIME_FIXTURE_BASE + RUNTIME_DEBUG_INTERFACE_OFFSET);
}

KTEST(AddressSpace, RodynamicInterpreterRejectsMalformedGnuHash) {
    auto fixture = make_runtime_resolver_fixture();
    uint32_t const ZERO = 0;
    fixture.store(RUNTIME_GNU_HASH_OFFSET + (2 * sizeof(uint32_t)), ZERO);
    runtime_images::SnapshotSource const SOURCE{.interpreter_base = RUNTIME_FIXTURE_BASE};
    uint64_t address = 0;
    KEXPECT_FALSE(runtime_images::selftest_find_debug_interface(read_runtime_resolver_fixture, &fixture, SOURCE, address));
}

KTEST(AddressSpace, LoaderAcceptsRandomizedPieAndFixedStaticExecutable) {
    auto pie = make_executable_fixture(ET_DYN, 0);
    elf::ElfLoadResult const PIE_RESULT = load_fixture(pie, 0xA51001);
    KREQUIRE_NE(PIE_RESULT.entry_point, 0U);
    KEXPECT_EQ(PIE_RESULT.entry_point, PIE_RESULT.load_base + 0x200);
    KEXPECT_TRUE(PIE_RESULT.load_base >= layout::MAIN_IMAGE_WINDOW.begin);
    KEXPECT_TRUE(PIE_RESULT.load_base < layout::MAIN_IMAGE_WINDOW.end);
    KEXPECT_EQ(PIE_RESULT.load_base & (layout::IMAGE_ALIGNMENT - 1), 0U);
    KEXPECT_EQ(PIE_RESULT.image_start, PIE_RESULT.load_base);
    KEXPECT_EQ(PIE_RESULT.image_end, PIE_RESULT.load_base + ELF_FIXTURE_SIZE);

    auto static_executable = make_executable_fixture(ET_EXEC, 0x400000);
    elf::ElfLoadResult const STATIC_RESULT = load_fixture(static_executable, 0xA51002);
    KEXPECT_EQ(STATIC_RESULT.entry_point, 0x400200U);
    KEXPECT_EQ(STATIC_RESULT.load_base, 0U);
    KEXPECT_EQ(STATIC_RESULT.image_start, 0x400000U);
    KEXPECT_EQ(STATIC_RESULT.image_end, 0x401000U);
}

KTEST(AddressSpace, LoaderRejectsMalformedProtectionContracts) {
    auto writable_executable = make_executable_fixture(ET_DYN, 0);
    writable_executable.program_headers.at(0).p_flags = PF_R | PF_W | PF_X;
    KEXPECT_EQ(load_fixture(writable_executable, 0xA51010).entry_point, 0U);

    auto executable_stack = make_executable_fixture(ET_DYN, 0);
    executable_stack.program_headers.at(1).p_flags = PF_R | PF_W | PF_X;
    KEXPECT_EQ(load_fixture(executable_stack, 0xA51011).entry_point, 0U);

    auto contradictory_overlap = make_executable_fixture(ET_DYN, 0);
    contradictory_overlap.program_headers.at(1) = {
        .p_type = PT_LOAD,
        .p_flags = PF_R | PF_W,
        .p_offset = 0x800,
        .p_vaddr = 0x800,
        .p_paddr = 0x800,
        .p_filesz = 0x800,
        .p_memsz = 0x800,
        .p_align = paging::PAGE_SIZE,
    };
    KEXPECT_EQ(load_fixture(contradictory_overlap, 0xA51012).entry_point, 0U);

    auto same_permission_overlap = make_executable_fixture(ET_DYN, 0);
    same_permission_overlap.program_headers.at(1) = {
        .p_type = PT_LOAD,
        .p_flags = PF_R | PF_X,
        .p_offset = 0x400,
        .p_vaddr = 0x400,
        .p_paddr = 0x400,
        .p_filesz = 0x800,
        .p_memsz = 0x800,
        .p_align = paging::PAGE_SIZE,
    };
    KEXPECT_EQ(load_fixture(same_permission_overlap, 0xA51014).entry_point, 0U);

    auto overflowing_segment = make_executable_fixture(ET_DYN, 0);
    overflowing_segment.program_headers.at(0).p_offset = 0x800;
    overflowing_segment.program_headers.at(0).p_vaddr = UINT64_MAX - 0x7FF;
    overflowing_segment.program_headers.at(0).p_paddr = overflowing_segment.program_headers.at(0).p_vaddr;
    overflowing_segment.program_headers.at(0).p_filesz = 0;
    overflowing_segment.program_headers.at(0).p_memsz = paging::PAGE_SIZE;
    overflowing_segment.header.e_entry = overflowing_segment.program_headers.at(0).p_vaddr;
    KEXPECT_EQ(load_fixture(overflowing_segment, 0xA51013).entry_point, 0U);
}

KTEST(AddressSpace, LoaderAcceptsZeroFillTlsInLoadAlignmentTail) {
    auto fixture = make_executable_fixture(ET_DYN, 0);
    fixture.program_headers.at(0).p_filesz = 0x800;
    fixture.program_headers.at(0).p_memsz = 0x800;
    fixture.program_headers.at(2) = {
        .p_type = PT_TLS,
        .p_flags = PF_R,
        .p_offset = 0x808,
        .p_vaddr = 0x808,
        .p_paddr = 0x808,
        .p_filesz = 0,
        .p_memsz = 8,
        .p_align = 8,
    };
    fixture.program_header_count = 3;

    elf::ElfLoadResult const RESULT = load_fixture(fixture, 0xA51015);
    KREQUIRE_NE(RESULT.entry_point, 0U);
    elf::TlsModule tls{};
    KEXPECT_TRUE(elf::inspect_tls(fixture.bytes.data(), fixture.bytes.size(), tls));
    KEXPECT_EQ(tls.tls_size, 8U);
}

KTEST(AddressSpace, BoundedLoaderRejectsTruncatedMetadata) {
    auto fixture = make_executable_fixture(ET_DYN, 0);
    KEXPECT_EQ(load_contiguous_fixture(fixture, 0xA51020, sizeof(Elf64_Ehdr) - 1).entry_point, 0U);
    KEXPECT_EQ(load_contiguous_fixture(fixture, 0xA51021, sizeof(Elf64_Ehdr) + sizeof(Elf64_Phdr) - 1).entry_point, 0U);

    auto truncated_sections = make_executable_fixture(ET_DYN, 0);
    truncated_sections.header.e_shoff = ELF_FIXTURE_SIZE - sizeof(Elf64_Shdr) + 1;
    truncated_sections.header.e_shentsize = sizeof(Elf64_Shdr);
    truncated_sections.header.e_shnum = 1;
    truncated_sections.header.e_shstrndx = SHN_UNDEF;
    KEXPECT_EQ(load_contiguous_fixture(truncated_sections, 0xA51027).entry_point, 0U);
}

KTEST(AddressSpace, LoaderCapsInitialTlsBeforeAllocation) {
    auto fixture = make_executable_fixture(ET_DYN, 0);
    fixture.program_headers.at(2) = {
        .p_type = PT_LOAD,
        .p_flags = PF_R | PF_W,
        .p_offset = 0,
        .p_vaddr = paging::PAGE_SIZE,
        .p_paddr = paging::PAGE_SIZE,
        .p_filesz = 0,
        .p_memsz = elf::MAX_INITIAL_TLS_SIZE,
        .p_align = paging::PAGE_SIZE,
    };
    fixture.program_headers.at(3) = {
        .p_type = PT_TLS,
        .p_flags = PF_R,
        .p_offset = 0,
        .p_vaddr = paging::PAGE_SIZE,
        .p_paddr = paging::PAGE_SIZE,
        .p_filesz = 0,
        .p_memsz = elf::MAX_INITIAL_TLS_SIZE,
        .p_align = paging::PAGE_SIZE,
    };
    fixture.program_header_count = 4;
    sync_fixture(fixture);

    elf::TlsModule tls{};
    KEXPECT_TRUE(elf::inspect_tls(fixture.bytes.data(), fixture.bytes.size(), tls));
    KEXPECT_EQ(tls.tls_size, elf::MAX_INITIAL_TLS_SIZE);

    fixture.program_headers.at(2).p_memsz += paging::PAGE_SIZE;
    fixture.program_headers.at(3).p_memsz += paging::PAGE_SIZE;
    sync_fixture(fixture);
    KEXPECT_FALSE(elf::inspect_tls(fixture.bytes.data(), fixture.bytes.size(), tls));
    KEXPECT_EQ(load_contiguous_fixture(fixture, 0xA51022).entry_point, 0U);
}

KTEST(AddressSpace, StaticRelocationsCannotEscapeWritableLoadSegments) {
    constexpr uint32_t X86_64_RELOC_RELATIVE = 8;
    auto fixture = make_executable_fixture(ET_EXEC, 0x400000);
    fixture.program_headers.at(2) = {
        .p_type = PT_LOAD,
        .p_flags = PF_R | PF_W,
        .p_offset = 0,
        .p_vaddr = 0x401000,
        .p_paddr = 0x401000,
        .p_filesz = 0,
        .p_memsz = paging::PAGE_SIZE,
        .p_align = paging::PAGE_SIZE,
    };
    fixture.program_header_count = 3;

    constexpr uint64_t SHDR_OFFSET = 0x400;
    constexpr uint64_t SHSTR_OFFSET = 0x600;
    constexpr uint64_t SYMTAB_OFFSET = 0x620;
    constexpr uint64_t STRTAB_OFFSET = 0x640;
    constexpr uint64_t RELA_OFFSET = 0x648;
    constexpr std::array<char, 7> SECTION_NAMES{'\0', '.', 'r', 'e', 'l', 'a', '\0'};
    fixture.header.e_shoff = SHDR_OFFSET;
    fixture.header.e_shentsize = sizeof(Elf64_Shdr);
    fixture.header.e_shnum = 5;
    fixture.header.e_shstrndx = 1;

    std::array<Elf64_Shdr, 5> sections{};
    sections.at(1) = {.sh_type = SHT_STRTAB, .sh_offset = SHSTR_OFFSET, .sh_size = SECTION_NAMES.size(), .sh_addralign = 1};
    sections.at(2) = {.sh_type = SHT_SYMTAB,
                      .sh_offset = SYMTAB_OFFSET,
                      .sh_size = sizeof(Elf64_Sym),
                      .sh_link = 3,
                      .sh_addralign = alignof(Elf64_Sym),
                      .sh_entsize = sizeof(Elf64_Sym)};
    sections.at(3) = {.sh_type = SHT_STRTAB, .sh_offset = STRTAB_OFFSET, .sh_size = 1, .sh_addralign = 1};
    sections.at(4) = {.sh_name = 1,
                      .sh_type = SHT_RELA,
                      .sh_offset = RELA_OFFSET,
                      .sh_size = sizeof(Elf64_Rela),
                      .sh_link = 2,
                      .sh_addralign = alignof(Elf64_Rela),
                      .sh_entsize = sizeof(Elf64_Rela)};
    Elf64_Sym const NULL_SYMBOL{};
    Elf64_Rela const HOSTILE_RELOCATION{
        .r_offset = 0x700000000000ULL,
        .r_info = X86_64_RELOC_RELATIVE,
        .r_addend = 0,
    };
    std::memcpy(fixture.bytes.data() + SHDR_OFFSET, sections.data(), sizeof(sections));
    std::memcpy(fixture.bytes.data() + SHSTR_OFFSET, SECTION_NAMES.data(), SECTION_NAMES.size());
    std::memcpy(fixture.bytes.data() + SYMTAB_OFFSET, &NULL_SYMBOL, sizeof(NULL_SYMBOL));
    fixture.bytes.at(STRTAB_OFFSET) = 0;
    std::memcpy(fixture.bytes.data() + RELA_OFFSET, &HOSTILE_RELOCATION, sizeof(HOSTILE_RELOCATION));
    KEXPECT_EQ(load_contiguous_fixture(fixture, 0xA51023).entry_point, 0U);
}

KTEST(AddressSpace, LoaderRejectsMetadataOutsideLoadSegments) {
    auto fixture = make_executable_fixture(ET_DYN, 0);
    fixture.program_headers.at(2) = {
        .p_type = PT_NOTE,
        .p_flags = PF_R,
        .p_offset = 0x300,
        .p_vaddr = 0x600000,
        .p_paddr = 0x600000,
        .p_filesz = 8,
        .p_memsz = 8,
        .p_align = 4,
    };
    fixture.program_header_count = 3;
    KEXPECT_EQ(load_fixture(fixture, 0xA51024).entry_point, 0U);
}

KTEST(AddressSpace, FixedExecutableCannotOverlapLoaderOwnedArenas) {
    auto header_collision = make_executable_fixture(ET_EXEC, 0x1000);
    KEXPECT_EQ(load_fixture(header_collision, 0xA51025).entry_point, 0U);

    auto debug_collision = make_executable_fixture(ET_EXEC, 0x700000000000ULL);
    KEXPECT_EQ(load_fixture(debug_collision, 0xA51026).entry_point, 0U);
}
