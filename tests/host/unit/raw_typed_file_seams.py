"""Unreachable typed-ownership declarations for the older RAW-only fixtures.

These are link/type boundaries, not typed MM or retirement models. Every method
aborts if a RAW vector enters it. Actual extracted production bodies stay whole.
The demand MM regression supplies separate real branch/byte/ref oracles.
"""

DECLARATIONS = r'''
namespace ker::vfs { struct File; namespace tmpfs {
class TmpfsPageLoan { public: void* frame() const { std::abort(); }
 void* release_to_pte() { std::abort(); } };
class TmpfsReadSharedBacking { public: uint64_t eof() const { std::abort(); }
 bool loan_page(uint64_t,TmpfsPageLoan&) const { std::abort(); } };
}}
namespace ker::mod::mm::file_mapping {
enum class FamilyOwner { SNAPSHOT };
enum class BindingOwner { PIN, SNAPSHOT, INSTALLER };
enum class FileMappingBackingMode { PRIVATE_COPY, TMPFS_READ_SHARED, TMPFS_DEMAND_READ_SHARED, TMPFS_DEMAND_WRITE_SHARED };
class FileMappingLifetime { public:
 bool try_acquire(FamilyOwner) { std::abort(); }
 void release(FamilyOwner) { std::abort(); }
 ker::vfs::File* file() const { std::abort(); }
 FileMappingBackingMode backing_mode() const { std::abort(); }
 bool is_demand_shared() const { std::abort(); }
 const ker::vfs::tmpfs::TmpfsReadSharedBacking* read_shared_backing() const { std::abort(); }
};
class FileMappingBinding { public:
 FileMappingLifetime* lifetime() const { std::abort(); }
 uint64_t start() const { std::abort(); } uint64_t end() const { std::abort(); }
 uint64_t file_offset() const { std::abort(); }
 bool reclassify(BindingOwner,BindingOwner) { std::abort(); }
 void release(BindingOwner) { std::abort(); }
};
enum class FileMappingPinResult { ACQUIRED, NOT_FILE_MAPPING, REFUSE };
struct FileMappingIndex {
 template<class Root> FileMappingPinResult retain_for_pin_locked(Root*,uint64_t,uint64_t,FileMappingBinding*&) { std::abort(); }
 template<class Root> bool matches_pin_locked(Root*,uint64_t,uint64_t,const FileMappingBinding*) { std::abort(); }
};
}
'''

RANGE_HELPERS = r'''
inline bool lazy_vmem_is_tmpfs_shared(const LazyVmemRange& r) {
 return r.kind==LazyVmemKind::TMPFS_READ_SHARED||r.kind==LazyVmemKind::TMPFS_WRITE_SHARED;
}
inline bool lazy_vmem_has_lifetime(const LazyVmemRange& r) {
 return r.kind==LazyVmemKind::NATIVE_FILE_BACKED||lazy_vmem_is_tmpfs_shared(r);
}
inline bool lazy_vmem_is_file(const LazyVmemRange& r) {
 return r.kind==LazyVmemKind::FILE_BACKED||lazy_vmem_has_lifetime(r);
}
inline ker::vfs::File* lazy_vmem_file(const LazyVmemRange& r) {
 return lazy_vmem_has_lifetime(r)?r.lifetime->file():r.file;
}
'''

MM_SEAMS = r'''
struct RawExcludedRootControl { uint64_t serial{}; };
template<class Root> RawExcludedRootControl* find_user_root_control_locked(Root*) { std::abort(); }
file_mapping::FileMappingIndex file_mapping_index;
template<class Binding,class Range> bool file_binding_matches_range(const Binding&,const Range&) { std::abort(); }
class BorrowedFileMappingRootAccess;
class FileMappingInstallerLease { public:
 template<class Task,class Range> bool acquire(Task&,const Range&,uint64_t) { std::abort(); }
 template<class Task,class Range> bool acquire_borrowed(Task&,const Range&,uint64_t,const BorrowedFileMappingRootAccess&) { std::abort(); }
 // RAW materialization legitimately queries its never-acquired empty lease.
 file_mapping::FileMappingBinding* binding() const { return nullptr; }
};
inline bool prepare_captured_tmpfs_read_shared_page(sched::task::Task&,const sched::task::LazyVmemRange&,uint64_t,uint64_t,
                                                    file_mapping::FileMappingBinding&) { std::abort(); }
'''


def extend_raw_fixture(code: str, lock: str) -> str:
    """Extend only the explicit Host type/primitive shell, never actual bodies."""
    import re
    match = re.search(r"namespace ker\s*\{", code)
    assert match is not None
    code = '#include <cstdlib>\n' + code[:match.start()] + DECLARATIONS + code[match.start():]
    old = 'enum class LazyVmemKind{ANONYMOUS,FILE_BACKED};'
    assert code.count(old) == 1
    code = code.replace(old, 'enum class LazyVmemKind{ANONYMOUS,FILE_BACKED,NATIVE_FILE_BACKED,TMPFS_READ_SHARED,TMPFS_WRITE_SHARED};')
    old = 'ker::vfs::File*file=nullptr;'
    assert code.count(old) == 1
    code = code.replace(old, old + 'ker::mod::mm::file_mapping::FileMappingLifetime*lifetime=nullptr;')
    code = code.replace('struct PageFault{unsigned writable=0;};', 'struct PageFault{unsigned writable=0,fetch=0;};')
    marker = 'struct Task{'
    assert code.count(marker) == 1
    code = code.replace(marker, RANGE_HELPERS + marker)
    marker = 'enum class LazyFilePageInstallResult'
    assert code.count(marker) == 1
    code = code.replace(marker, MM_SEAMS + lock + '\n' + marker)
    # Existing fixture primitives expose only PROT_WRITE/PAGE_SIZE; these
    # constants are compile-only for the excluded typed branches.
    marker = '#include <cstdint>'
    code = code.replace(marker, marker + r'''
namespace ker::abi::vmem { inline constexpr uint64_t PROT_READ=1, MAP_SHARED=1; }
namespace ker::mod::mm::paging {
inline constexpr uint64_t PAGE_PRESENT=1,PAGE_USER=4,PAGE_SHARED=1024,PAGE_WRITE_RESTRICTED=2048,PAGE_NX=1ULL<<63;
}
''', 1)
    return code
