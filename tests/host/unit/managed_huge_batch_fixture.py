"""Finite physical/root/ACK ports for the actual whole-batch owner and mapper.

The actual allocator wrapper, registry, owners, aligned CAS and full mapper are
compiled. Range allocation below is a Host free-extent ledger, never a PMM or
WOS/SMP/TLB timing claim. Existing54 bodies execute unchanged with ports disabled.
"""
from pathlib import Path


class Builder:
    def __init__(self, kernel_file, support, definition, bounded):
        self.source = kernel_file("platform/mm/virt.opt.cpp")
        self.case_path = support / "managed_huge_batch_cases.inc"
        self.inputs = [Path(__file__), self.source, self.case_path]
        source = self.source.read_text()
        self.mapper = bounded(source, "// BEGIN whole-batch managed mapper.", "// END whole-batch managed mapper.")
        self.adapter = definition(source, "auto publish_prepared_huge_batch(paging::PageTable*")
        self.test_count = self.case_path.read_text().count("TEST_F(ManagedBatch,")
        if not 1 <= self.test_count <= 18:
            raise RuntimeError("batch fixture exceeds accepted named-case cap")

    @staticmethod
    def replace(text, old, new):
        if text.count(old) != 1:
            raise RuntimeError(f"actual batch endpoint changed: {old}")
        return text.replace(old, new)

    def compose(self, text):
        text = self.replace(text, "return allocate(first, pages, alignment, initialization);", """
        if (host_batch::enabled) {
            if (host_batch::allocation_index++ == host_batch::fail_data_before) return nullptr;
            uint64_t chosen = first;
            for (;;) {
                bool occupied = false;
                for (auto const& block : blocks)
                    if (block.storage && chosen < block.physical + uint64_t{block.pages} * PAGE &&
                        block.physical < chosen + uint64_t{pages} * PAGE) {occupied=true;break;}
                if (!occupied) break;
                if (end - chosen < alignment) return nullptr;
                chosen += alignment;
            }
            if (chosen > end || uint64_t{pages} * PAGE > end - chosen) return nullptr;
            return allocate(chosen, pages, alignment, initialization);
        }
        return allocate(first, pages, alignment, initialization);""")
        text = self.replace(text, "void* operator new(std::size_t bytes, const std::nothrow_t&) noexcept {", """void* operator new(std::size_t bytes, const std::nothrow_t&) noexcept {
    if (host_batch::enabled && bytes == sizeof(huge::detail::HugeMapBatchStorage)) {
        huge::Snapshot snapshot{}; huge_core.snapshot(snapshot);
        if (snapshot.plans != 2 || snapshot.table_pages < 5) host_batch::heap_without_plans = true;
        if (host_batch::fail_heap) {host_batch::fail_heap=false;return nullptr;}
    }
""")
        text = self.replace(text, "void release_table(paging::PageTable* p) noexcept {", "void release_table(paging::PageTable* p) noexcept {\n    host_batch::release(p);")
        text = self.replace(text, "void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {", "void flush_pagemap_after_update(PageTable* root, uint64_t va, bool full) {\n    host_batch::ack();")
        prefix = """
#include <cstdint>
#include <cstddef>
namespace host_batch {
inline bool enabled=false,fail_heap=false,heap_without_plans=false;
inline std::size_t allocation_index=0,fail_data_before=SIZE_MAX;
void release(void*);void ack();
}
"""
        return (prefix + text + "\nnamespace ker::mod::mm::virt {\n" + self.mapper +
                "\n} namespace ker::mod::mm::huge::detail {\n" + self.adapter + "\n}\n" + self.case_path.read_text())

    def controls(self, text):
        replace = self.replace
        yield ("batch-incomplete-preflight", replace(text,
               "for (uint16_t leaf = 0; leaf < batch.count; ++leaf) {",
               "for (uint16_t leaf = 0; leaf + 1 < batch.count; ++leaf) {"),
               "ManagedBatch.LastDestinationConflictLeavesTheWholeBatchUnpublished")
        yield ("batch-two-parent-assumption", replace(text,
               "if (next_parent == parents.table_count) return huge::Error::Invalid;",
               "if (next_parent == 2) return huge::Error::Invalid;"),
               "ManagedBatch.FourParentsAcrossThe512GigabyteBoundary")
        yield ("batch-ignores-last-empty-word", replace(text,
               "word < paging::PAGE_TABLE_ENTRIES; ++word) {",
               "word + 1 < paging::PAGE_TABLE_ENTRIES; ++word) {"),
               "ManagedBatch.EveryNonzeroOldEmptyPositionRefusesBeforeTheFirstWrite")
        yield ("batch-unknown-ps-follow", replace(text,
               "} else if (raw != 0) {\n                if ((raw & paging::PAGE_PRESENT) == 0 || (raw & PS) != 0 || (raw & ~PARENT_ALLOWED) != 0)",
               "} else if (raw != 0) {\n                if ((raw & paging::PAGE_PRESENT) == 0)"),
               "ManagedBatch.UnknownPsOneGigabyteAndSoftwareParentNeverDescend")
        yield ("batch-old-table-before-ack", replace(text,
               "huge::detail::HugeMapBatchAccess::transfer_after_publish(owner);",
               "huge::detail::HugeMapBatchAccess::transfer_after_publish(owner); for(uint16_t i=0;i<batch->detached_count;++i) huge::detail::release_table(batch->detached[i]); batch->detached_count=0;"),
               "ManagedBatch.DetachedOldTablesSurviveAckAndReturnOnlyAfterAllOwners")
        yield ("batch-missing-transfer", replace(text,
               "huge::detail::HugeMapBatchAccess::transfer_after_publish(owner);", "/* missing actual owner transfer */"),
               "ManagedBatch.OneTwoAnd64LeavesTransferEveryIndependentReference")


class CurrentBuilder(Builder):
    """Real current admission with explicit finite entry/ACK observations only."""

    def __init__(self, kernel_file, support, definition, bounded):
        super().__init__(kernel_file, support, definition, bounded)
        self.current_case_path = support / "managed_current_huge_map_cases.inc"
        self.module = kernel_file("platform/mm/managed_huge.cpp")
        self.header = kernel_file("platform/mm/managed_huge.hpp")
        self.current_methods = bounded(self.module.read_text(), "// BEGIN current huge-map admission.",
                                       "// END current huge-map admission.")
        self.current_declaration = definition(self.header.read_text(), "class CurrentHugeMapAdmission {")
        count = self.current_case_path.read_text().count("TEST_F(ManagedCurrentMap,")
        if not 1 <= count <= 12:
            raise RuntimeError("current huge-map fixture exceeds accepted named-case cap")
        self.test_count += count
        self.inputs.extend([self.current_case_path, self.module, self.header])

    def compose(self, text):
        text = super().compose(text)
        # Observation seams only: the actual mapper and its old ACK assertions
        # remain intact; no authority is supplied by these counters.
        text = self.replace(text, "return virt::publish_managed_huge_batch_owned(root, publication, batch);",
                            "host_current_map::entered(); return virt::publish_managed_huge_batch_owned(root, publication, batch);")
        text = self.replace(text, "++acknowledgments;", "host_current_map::ack(); ++acknowledgments;")
        prefix = """
namespace ker::mod::sched::task { struct Task; }
namespace host_current_map {
inline bool enabled=false;
inline ker::mod::sched::task::Task* actor=nullptr;
inline unsigned entries=0,acks=0;
void entered();void ack();
}
"""
        return prefix + text + "\n" + self.current_case_path.read_text()

    def module_controls(self, text):
        # CppText supplied by the maintained driver tolerates canonical wrapping.
        yield ("current-map-context", self.replace(text,
               "range_current_context(*subject_, *preparation_, true)", "true"),
               "ManagedCurrentMap.AbsentForeignInactiveAndDaemonCurrentNeverAcquire")
        yield ("current-map-missing-release", self.replace(text,
               "CurrentHugeMapAdmission::~CurrentHugeMapAdmission() noexcept { detail::release_current_range_root(access_); }",
               "CurrentHugeMapAdmission::~CurrentHugeMapAdmission() noexcept { /* omitted local release */ }"),
               "ManagedCurrentMap.PartialKernelKeyAndNullRootFailuresReleaseExactlyOnce")
        yield ("current-map-stale-incarnation", self.replace(text,
               "if (!RegistryCore::valid_root(key_) || !RegistryCore::same_root(current, key_)) return Error::Stale;",
               "if (false) return Error::Stale;"),
               "ManagedCurrentMap.IncarnationChangeRefusesBeforeOriginalPublisher")
        yield ("current-map-normal-reader", self.replace(text,
               "access_.subject = &subject; access_.local = subject.try_acquire_usercopy_pagemap(access_.root);",
               "(void)detail::acquire_current_range_root(subject, access_);"),
               "ManagedCurrentMap.LocalOnlyOneAndTwoLeafPublishRetainsLeaseThroughAck")
