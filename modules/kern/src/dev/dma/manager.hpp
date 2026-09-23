#pragma once

#include <array>
#include <dev/dma/dma.hpp>
#include <optional>

namespace ker::dev::dma::detail {
constexpr size_t PAGE = 4096;
constexpr size_t MAX_BACKING = 64 * 1024 * 1024;
struct Layout {
    size_t alignment;
    size_t mapped;
    size_t backing;
};
[[nodiscard]] constexpr auto layout(size_t bytes, size_t alignment, uint64_t mask) -> std::optional<Layout> {
    if (bytes == 0 || bytes > MAX_BACKING || alignment == 0 || alignment > MAX_BACKING || (alignment & (alignment - 1)) != 0 ||
        mask < PAGE - 1 || (mask != UINT64_MAX && (mask & (mask + 1)) != 0))
        return std::nullopt;
    if (alignment < PAGE) alignment = PAGE;
    size_t const MAPPED = (bytes + PAGE - 1) & ~(PAGE - 1);
    size_t const NEEDED = MAPPED + alignment - PAGE;
    if (NEEDED > MAX_BACKING) return std::nullopt;
    size_t backing = PAGE;
    while (backing < NEEDED) backing *= 2;
    return Layout{alignment, MAPPED, backing};
}
[[nodiscard]] constexpr auto extent_fits(uint64_t address, size_t bytes, uint64_t mask) -> bool {
    return bytes != 0 && address <= mask && bytes - 1 <= mask - address;
}
struct Storage {
    std::byte* cpu = nullptr;
    uint64_t physical = 0;
};
// A failed map is releasable only if the backend proves no hardware exposure.
struct Mapping {
    Error error = Error::none;
    uint64_t address = 0;
    bool quarantine = false;
};
enum class State : uint8_t { free, reserved, cpu, preparing, device, completing, releasing, quarantined };

// Kernel-lifetime, nonmovable registry. Backend lock/unlock protect metadata
// only. All other backend calls occur without that lock. Backend allocation
// returns exclusive PAGE-aligned contiguous storage, zeroed over backing bytes.
// map publishes/invalidate-syncs translations before success; unmap succeeds
// only after invalidation/draining. Failure with uncertain exposure pins storage.
// Host tests instantiate THIS manager, not a second implementation of its rules.
template <typename Backend, size_t Capacity, uint64_t CounterLimit = UINT64_MAX>
class Manager {
    static_assert(Capacity != 0 && CounterLimit != 0);
    struct Record {
        const pci::PCIDevice* device = nullptr;
        uint64_t generation = 0;
        uint64_t sequence = 0;
        State state = State::free;
        bool streaming = false;
        Direction direction = Direction::bidirectional;
        Direction allowed_direction = Direction::bidirectional;
        Storage storage{};
        size_t backing = 0;
        size_t offset = 0;
        size_t mapped = 0;
        size_t size = 0;
        size_t capacity = 0;
        uint64_t address = 0;
        std::byte* borrowed = nullptr;
    };
    struct Guard {
        Backend& backend;
        uint64_t flags;
        explicit Guard(Backend& value) : backend(value), flags(backend.lock()) {}
        ~Guard() { backend.unlock(flags); }
        Guard(const Guard&) = delete;
        auto operator=(const Guard&) -> Guard& = delete;
    };
    Backend& backend_;
    std::array<Record, Capacity> records_{};
    uint64_t generation_ = 0;

    auto find(Handle h) -> Record* {
        if (h.registry != this || h.slot >= Capacity || h.generation == 0) return nullptr;
        auto& r = records_[h.slot];
        return r.state != State::free && r.generation == h.generation && r.device == h.device ? &r : nullptr;
    }
    auto allocate(const pci::PCIDevice* device, size_t bytes, size_t alignment, uint64_t mask, std::byte* borrowed, Direction direction,
                  bool streaming) -> AllocationResult {
        if (!backend_.allocation_context()) return {.error = Error::invalid_context};
        auto const PLAN = layout(bytes, alignment, mask);
        if (!PLAN) return {.error = Error::invalid_argument};
        if (!backend_.admit(device)) return {.error = Error::unavailable_device};
        Handle handle{};
        {
            Guard guard{backend_};
            if (generation_ == CounterLimit) return {.error = Error::exhausted};
            for (size_t i = 0; i < Capacity; ++i) {
                auto& r = records_[i];
                if (r.state != State::free) continue;
                r = {.device = device,
                     .generation = ++generation_,
                     .state = State::reserved,
                     .streaming = streaming,
                     .direction = direction,
                     .allowed_direction = direction,
                     .mapped = PLAN->mapped,
                     .size = bytes,
                     .capacity = bytes,
                     .borrowed = borrowed};
                handle = {this, device, r.generation, i};
                break;
            }
        }
        if (handle.registry == nullptr) return {.error = Error::exhausted};
        Storage const STORAGE = backend_.allocate(PLAN->backing);
        if (STORAGE.cpu == nullptr) {
            Guard guard{backend_};
            records_[handle.slot] = {};
            return {.error = Error::allocation_failed};
        }
        Error error = Error::none;
        size_t offset = 0;
        Mapping mapping{.error = Error::address_limit};
        if ((STORAGE.physical & (PAGE - 1)) != 0 || !extent_fits(STORAGE.physical, PLAN->backing, UINT64_MAX)) {
            error = Error::address_limit;
        } else {
            offset = static_cast<size_t>((PLAN->alignment - (STORAGE.physical & (PLAN->alignment - 1))) & (PLAN->alignment - 1));
            // Allocation includes worst-case alignment padding, all exclusively owned.
            if (offset > PLAN->backing || PLAN->mapped > PLAN->backing - offset) {
                error = Error::address_limit;
            } else {
                mapping = backend_.map(device, STORAGE.physical + offset, PLAN->mapped, PLAN->alignment, mask, direction);
                error = mapping.error;
                if (error == Error::none &&
                    ((mapping.address & (PLAN->alignment - 1)) != 0 || !extent_fits(mapping.address, PLAN->mapped, mask))) {
                    error = Error::address_limit;
                    mapping.quarantine = !backend_.unmap(device, mapping.address, STORAGE.physical + offset, PLAN->mapped, direction);
                }
            }
        }
        if (error != Error::none && !mapping.quarantine) {
            backend_.free(STORAGE, PLAN->backing);
            Guard guard{backend_};
            records_[handle.slot] = {};
            return {.error = error};
        }
        {
            Guard guard{backend_};
            auto& r = records_[handle.slot];
            r.storage = STORAGE;
            r.backing = PLAN->backing;
            r.offset = offset;
            r.address = mapping.address;
            r.state = mapping.quarantine ? State::quarantined : State::cpu;
        }
        return {.error = mapping.quarantine ? Error::quarantined : Error::none,
                .allocation = {handle, borrowed == nullptr && !mapping.quarantine ? STORAGE.cpu + offset : nullptr, bytes}};
    }

   public:
    explicit constexpr Manager(Backend& backend) : backend_(backend) {}
    Manager(const Manager&) = delete;
    auto operator=(const Manager&) -> Manager& = delete;
    // Deliberately no destructor free: registry and hardware-visible backing
    // must outlive all users. Kernel instance has static lifetime.
    auto allocate_coherent(const pci::PCIDevice* device, size_t bytes, size_t alignment, uint64_t mask) -> AllocationResult {
        return allocate(device, bytes, alignment, mask, nullptr, Direction::bidirectional, false);
    }
    auto map_streaming(const pci::PCIDevice* device, std::span<std::byte> buffer, Direction direction, uint64_t mask) -> AllocationResult {
        if (buffer.empty() || buffer.data() == nullptr ||
            (direction != Direction::to_device && direction != Direction::from_device && direction != Direction::bidirectional) ||
            !backend_.kernel_buffer(buffer))
            return {.error = Error::invalid_argument};
        return allocate(device, buffer.size(), PAGE, mask, buffer.data(), direction, true);
    }
    auto allocate_streaming(const pci::PCIDevice* device, size_t capacity, Direction direction, uint64_t mask) -> AllocationResult {
        if (direction != Direction::to_device && direction != Direction::from_device && direction != Direction::bidirectional)
            return {.error = Error::invalid_argument};
        return allocate(device, capacity, PAGE, mask, nullptr, direction, true);
    }
    auto bind_streaming(Handle handle, std::span<std::byte> buffer, Direction direction) -> Error {
        if (buffer.empty() || buffer.data() == nullptr || !backend_.kernel_buffer(buffer) ||
            (direction != Direction::to_device && direction != Direction::from_device && direction != Direction::bidirectional))
            return Error::invalid_argument;
        Guard guard{backend_};
        auto* r = find(handle);
        if (r == nullptr) return Error::invalid_handle;
        if (r->state == State::quarantined) return Error::quarantined;
        if (r->state != State::cpu) return Error::busy;
        if (!r->streaming || buffer.size() > r->capacity ||
            (r->allowed_direction != Direction::bidirectional && direction != r->allowed_direction))
            return Error::invalid_argument;
        auto const BEGIN = reinterpret_cast<uintptr_t>(buffer.data());
        auto const OWNED = reinterpret_cast<uintptr_t>(r->storage.cpu);
        if (!extent_fits(BEGIN, buffer.size(), UINTPTR_MAX)) return Error::invalid_argument;
        // The exact CPU view supports staging user copies without a second
        // memcpy. All other overlaps would violate memcpy's disjoint contract.
        if (BEGIN != OWNED + r->offset && BEGIN <= OWNED + r->backing - 1 && BEGIN + buffer.size() - 1 >= OWNED)
            return Error::invalid_argument;
        r->borrowed = buffer.data();
        r->size = buffer.size();
        r->direction = direction;
        return Error::none;
    }
    auto prepare(Handle handle) -> SubmissionResult {
        Record snapshot{};
        {
            Guard guard{backend_};
            auto* r = find(handle);
            if (r == nullptr) return {.error = Error::invalid_handle};
            if (r->state == State::quarantined) return {.error = Error::quarantined};
            if (r->state != State::cpu) return {.error = Error::busy};
            if (!backend_.admit(r->device)) return {.error = Error::unavailable_device};
            if (r->streaming && r->borrowed == nullptr) return {.error = Error::invalid_argument};
            if (r->sequence == CounterLimit) return {.error = Error::exhausted};
            ++r->sequence;
            r->state = State::preparing;
            snapshot = *r;
        }
        auto* cpu = snapshot.storage.cpu + snapshot.offset;
        if (snapshot.streaming && snapshot.direction != Direction::from_device && cpu != snapshot.borrowed)
            backend_.copy(cpu, snapshot.borrowed, snapshot.size);
        backend_.for_device(cpu, snapshot.mapped, snapshot.direction);
        {
            Guard guard{backend_};
            records_[handle.slot].state = State::device;
        }
        return {.submission = {handle, snapshot.sequence, snapshot.address, snapshot.size}};
    }
    auto complete(Submission submission, size_t completed_bytes, Retirement proof) -> Error {
        Record snapshot{};
        {
            Guard guard{backend_};
            auto* r = find(submission.handle);
            if (r == nullptr) return Error::invalid_handle;
            if (r->state == State::quarantined) return Error::quarantined;
            if (r->state != State::device) return Error::busy;
            if (submission.sequence == 0 || submission.sequence != r->sequence || completed_bytes > r->size ||
                (proof != Retirement::never_submitted && proof != Retirement::command_completed && proof != Retirement::device_quiesced) ||
                (!r->streaming && proof == Retirement::command_completed))
                return Error::invalid_completion;
            r->state = State::completing;
            snapshot = *r;
        }
        auto* cpu = snapshot.storage.cpu + snapshot.offset;
        backend_.for_cpu(cpu, snapshot.mapped, snapshot.direction);
        if (snapshot.streaming && snapshot.direction != Direction::to_device && completed_bytes != 0 && cpu != snapshot.borrowed)
            backend_.copy(snapshot.borrowed, cpu, completed_bytes);
        {
            Guard guard{backend_};
            records_[submission.handle.slot].state = State::cpu;
            if (snapshot.streaming) records_[submission.handle.slot].borrowed = nullptr;
        }
        return Error::none;
    }
    auto release(Handle handle) -> Error {
        if (!backend_.allocation_context()) return Error::invalid_context;
        Record snapshot{};
        {
            Guard guard{backend_};
            auto* r = find(handle);
            if (r == nullptr) return Error::invalid_handle;
            if (r->state == State::quarantined) return Error::quarantined;
            if (r->state != State::cpu) return Error::busy;
            r->state = State::releasing;
            snapshot = *r;
        }
        if (!backend_.unmap(snapshot.device, snapshot.address, snapshot.storage.physical + snapshot.offset, snapshot.mapped,
                            snapshot.allowed_direction)) {
            Guard guard{backend_};
            records_[handle.slot].state = State::quarantined;
            return Error::quarantined;
        }
        backend_.free(snapshot.storage, snapshot.backing);
        {
            Guard guard{backend_};
            records_[handle.slot] = {};
        }
        return Error::none;
    }
    auto quarantine(Handle handle) -> Error {
        Guard guard{backend_};
        auto* r = find(handle);
        if (r == nullptr) return Error::invalid_handle;
        if (r->state == State::quarantined) return Error::quarantined;
        if (r->state != State::cpu && r->state != State::device) return Error::busy;
        r->state = State::quarantined;
        r->borrowed = nullptr;
        return Error::quarantined;
    }
    auto mapping_info(const pci::PCIDevice* device, uint64_t address) -> MappingInfo {
        if (!backend_.allocation_context()) return {};
        Guard guard{backend_};
        for (size_t i = 0; i < Capacity; ++i) {
            auto const& r = records_[i];
            if (r.device != device || r.state == State::free || r.backing == 0 || address < r.address || address - r.address >= r.mapped)
                continue;
            return {{this, device, r.generation, i}, r.storage.physical + r.offset, r.address, r.mapped, r.allowed_direction,
                    r.state == State::quarantined,   r.state == State::releasing};
        }
        return {};
    }
    auto stats(const pci::PCIDevice* device = nullptr) -> Stats {
        Guard guard{backend_};
        Stats result{};
        for (auto const& r : records_) {
            if (r.state == State::free) continue;
            if (device != nullptr && r.device != device) continue;
            ++result.regions;
            result.backing_pages += r.backing / PAGE;
            result.streaming += r.streaming;
            result.device_owned += r.state == State::device;
            result.quarantined += r.state == State::quarantined;
        }
        return result;
    }
};
}  // namespace ker::dev::dma::detail
