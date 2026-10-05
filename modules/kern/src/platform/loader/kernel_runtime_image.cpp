#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/mm/kernel_image.hpp>
#include <span>
#include <string_view>

#include "runtime_images.hpp"

// These builders run before the IDT/shadow fault handler exists and during
// coredumps. Keep their translation unit in the early-runtime exclusion list;
// userspace runtime-image parsing remains instrumented in runtime_images.cpp.
namespace ker::loader::runtime {

#ifndef WOS_HOST_TEST
extern "C" void _start();  // NOLINT(readability-identifier-naming)
#endif

namespace {

auto valid_kernel_layout(const ker::mod::mm::kernel_image::RuntimeLayout& layout) -> bool {
    return layout.load_base != 0 && layout.image_end > layout.load_base && layout.text_end > layout.text_start &&
           layout.text_start >= layout.load_base && layout.text_end <= layout.image_end;
}

auto read_kernel_build_id(std::span<uint8_t> destination, uint8_t& size) -> bool {
    size = 0;
    auto const NOTE = ker::mod::mm::kernel_image::build_id_note();
    if (NOTE.size() < 12) {
        return false;
    }
    auto read_u32 = [&](size_t offset) -> uint32_t {
        auto const* ptr = reinterpret_cast<const uint8_t*>(NOTE.data()) + offset;
        return static_cast<uint32_t>(ptr[0]) | (static_cast<uint32_t>(ptr[1]) << 8U) | (static_cast<uint32_t>(ptr[2]) << 16U) |
               (static_cast<uint32_t>(ptr[3]) << 24U);
    };
    uint32_t const NAME_SIZE = read_u32(0);
    uint32_t const DESC_SIZE = read_u32(4);
    uint32_t const TYPE = read_u32(8);
    size_t const NAME_PADDED = (static_cast<size_t>(NAME_SIZE) + 3U) & ~size_t{3};
    size_t const DESC_OFFSET = 12U + NAME_PADDED;
    if (TYPE != 3U || NAME_SIZE != 4U || DESC_SIZE == 0U || DESC_SIZE > destination.size() || DESC_OFFSET > NOTE.size() ||
        DESC_SIZE > NOTE.size() - DESC_OFFSET) {
        return false;
    }
    auto const* desc = reinterpret_cast<const uint8_t*>(NOTE.data()) + DESC_OFFSET;
    std::copy_n(desc, DESC_SIZE, destination.begin());
    size = static_cast<uint8_t>(DESC_SIZE);
    return true;
}

auto make_kernel_image(RuntimeImage& image) -> bool {
#ifdef WOS_HOST_TEST
    (void)image;
    return false;
#else
    auto const LAYOUT = ker::mod::mm::kernel_image::layout();
    if (!valid_kernel_layout(LAYOUT)) {
        return false;
    }

    image = {};
    image.load_base = LAYOUT.load_base;
    image.image_start = LAYOUT.load_base;
    image.image_end = LAYOUT.image_end;
    image.text_start = LAYOUT.text_start;
    image.text_end = LAYOUT.text_end;
    image.entry = reinterpret_cast<uint64_t>(&_start);
    image.dynamic_addr = LAYOUT.dynamic_start;
    image.flags = IMAGE_KERNEL | IMAGE_EXACT_MAPPING;
    constexpr std::string_view PATH = "/boot/wos";
    std::copy(PATH.begin(), PATH.end(), image.path.begin());
    if (read_kernel_build_id(image.build_id, image.build_id_size)) {
        image.flags |= IMAGE_BUILD_ID_PRESENT;
    }
    return true;
#endif
}

}  // namespace

auto append_kernel_image(std::array<RuntimeImage, MAX_IMAGES>& images, size_t& count) -> bool {
    if (count >= images.size()) {
        return false;
    }
    RuntimeImage image{};
    if (!make_kernel_image(image)) {
        return false;
    }
    images.at(count++) = image;
    return true;
}

auto kernel_debug_metadata(wos::abi::KernelDebugMetadata& metadata) -> bool {
#ifdef WOS_HOST_TEST
    (void)metadata;
    return false;
#else
    auto const LAYOUT = ker::mod::mm::kernel_image::layout();
    if (!valid_kernel_layout(LAYOUT)) {
        return false;
    }
    metadata = {
        .magic = wos::abi::KERNEL_DEBUG_METADATA_MAGIC,
        .version = wos::abi::KERNEL_DEBUG_METADATA_VERSION,
        .size = static_cast<uint32_t>(sizeof(metadata)),
        .load_base = LAYOUT.load_base,
        .image_end = LAYOUT.image_end,
        .text_start = LAYOUT.text_start,
        .text_end = LAYOUT.text_end,
        .rodata_start = LAYOUT.rodata_start,
        .rodata_end = LAYOUT.rodata_end,
        .data_start = LAYOUT.data_start,
        .data_end = LAYOUT.data_end,
        .entry = reinterpret_cast<uint64_t>(&_start),
        .dynamic_addr = LAYOUT.dynamic_start,
        .flags = wos::abi::KERNEL_DEBUG_METADATA_EXACT_MAPPING,
        .build_id_size = 0,
    };
    std::span<uint8_t> const BUILD_ID{metadata.build_id, sizeof(metadata.build_id)};
    if (read_kernel_build_id(BUILD_ID, metadata.build_id_size)) {
        metadata.flags |= wos::abi::KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT;
    }
    return true;
#endif
}

}  // namespace ker::loader::runtime
