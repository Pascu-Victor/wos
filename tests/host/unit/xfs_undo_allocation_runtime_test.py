#!/usr/bin/env python3
"""Run real XFS transaction bodies/types with instrumented allocation and IO seams.

The allocator, journal and log boundaries are controlled; capture, alias refresh,
restore, commit, cancel, pool release and destruction are production code. This
does not exercise the kernel buddy allocator or prove a performance benefit.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
KERNEL = ROOT / "modules/kern/src"
SOURCE = Path(os.environ.get("WOS_XFS_UNDO_SOURCE", KERNEL / "vfs/fs/xfs/xfs_trans.cpp"))
HEADER = Path(os.environ.get("WOS_XFS_UNDO_HEADER", KERNEL / "vfs/fs/xfs/xfs_trans.hpp"))

LOCKS = r'''
#pragma once
#include <cassert>
#include <cstdint>
namespace ker::mod::sys {
inline unsigned undo_test_lock_depth{};
class Spinlock {
    bool held{};
public:
    void lock(){assert(!held);held=true;++undo_test_lock_depth;}
    auto try_lock()->bool {if(held)return false;lock();return true;}
    void unlock(){assert(held&&undo_test_lock_depth);held=false;--undo_test_lock_depth;}
    auto lock_irqsave()->uint64_t {lock();return 0;}
    void unlock_irqrestore(uint64_t){unlock();}
};
class Mutex:public Spinlock {};
class MutexGuard {Mutex& m;public:explicit MutexGuard(Mutex& x):m(x){m.lock();}~MutexGuard(){m.unlock();}};
}
'''

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>
#include <vfs/fs/xfs/xfs_trans.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>
#include <platform/mm/page_alloc.hpp>
namespace ker::mod::mm::paging {ACTUAL_PAGE_SIZE}
namespace fixture {
using namespace ker::vfs;
using namespace ker::vfs::xfs;
enum class Kind {SCALAR,ARRAY,PAGE};
struct Allocation {void* p{};size_t size{};Kind kind{};bool initialized{};};
std::array<Allocation,1024> allocations{};
size_t array_calls{},page_calls{},array_frees{},page_frees{},copies{},journal_holds{},journal_releases{},log_calls{},dirty_calls{},checkpoint_calls{},retire_calls{},cases{};
size_t fail_array_size{},fail_scalar_size{};
bool fail_page{},reject_all_arrays{};
XfsTransBufUndo* last_undo{};
XfsTransaction* watched{};
XfsTransBufUndo* old_head{};
BufHead* watched_bp{};
int32_t old_refs{};
size_t early_uninitialized_frees{};
int inode_rc{},log_rc{},checkpoint_rc{};
bool log_owned{},inode_overflow{};
constexpr uint8_t POISON=0xd3;
auto find(const void* p)->Allocation* {for(auto& a:allocations)if(a.p==p)return &a;return nullptr;}
void register_allocation(void* p,size_t size,Kind kind){
    assert(p&&!find(p));for(auto& a:allocations)if(!a.p){a={p,size,kind,false};return;}assert(false);
}
auto allocate(size_t size,Kind kind,bool nothrow)->void* {
    if(kind==Kind::ARRAY){++array_calls;if(reject_all_arrays||(fail_array_size&&size==fail_array_size))return nullptr;}
    if(kind==Kind::SCALAR&&nothrow&&fail_scalar_size&&size==fail_scalar_size)return nullptr;
    void* p=std::malloc(size?size:1);if(!p){if(nothrow)return nullptr;std::abort();}
    std::memset(p,POISON,size);register_allocation(p,size,kind);
    if(kind==Kind::SCALAR&&watched&&size==sizeof(XfsTransBufUndo))last_undo=static_cast<XfsTransBufUndo*>(p);
    return p;
}
void release(void* p,Kind kind){
    if(!p)return;auto* a=find(p);
    if(!a||a->kind!=kind)std::fprintf(stderr,"WRONG_BACKING_FREE kind=%u actual=%u\n",unsigned(kind),a?unsigned(a->kind):99U);
    assert(a&&a->kind==kind);
    if(kind==Kind::PAGE)++page_frees;
    if(kind==Kind::ARRAY)++array_frees;
    if(kind!=Kind::SCALAR&&!a->initialized&&last_undo&&last_undo->before_image==p){
        assert(last_undo->size==0&&last_undo->bp==nullptr&&!last_undo->journal_held);
        ++early_uninitialized_frees;
    }
    if(p==last_undo)last_undo=nullptr;
    *a={};std::free(p);
}
auto copy(void* destination,const void* source,size_t size)->void* {
    ++copies;auto* a=find(destination);
    if(a&&!a->initialized&&(a->kind==Kind::PAGE||a->kind==Kind::ARRAY)){
        assert(size==a->size);
        for(size_t i=0;i<size;++i)assert(static_cast<uint8_t*>(destination)[i]==POISON);
        if(watched&&last_undo&&last_undo->before_image==destination){
            assert(watched->buf_undo==old_head&&watched_bp->refcount.load()==old_refs);
            assert(last_undo->bp==nullptr&&last_undo->size==0&&last_undo->journal_held);
        }
        a->initialized=true;
    }
    return std::memcpy(destination,source,size);
}
void clean(){
    for(auto const& a:allocations)assert(a.p==nullptr);
    assert(ker::mod::sys::undo_test_lock_depth==0&&journal_holds==journal_releases);
    assert(page_calls>=page_frees&&array_calls>=array_frees);
    assert(!watched);last_undo=nullptr;
}
void start(const char* name){
    clean();++cases;array_calls=page_calls=array_frees=page_frees=copies=journal_holds=journal_releases=log_calls=dirty_calls=checkpoint_calls=retire_calls=early_uninitialized_frees=0;
    fail_array_size=fail_scalar_size=0;fail_page=reject_all_arrays=false;
    inode_rc=log_rc=checkpoint_rc=0;log_owned=inode_overflow=false;
    std::printf("CASE %s\n",name);std::fflush(stdout);
}
auto expected_page(size_t size)->bool {
#if defined(WOS_ASYNC_BLOCK_IO) && !defined(XFS_UNDO_ORIGINAL_POLICY)
    return size>=4096&&size<=65536&&size%4096==0&&(size&(size-1))==0;
#else
    (void)size;return false;
#endif
}
void check_backing(XfsTransBufUndo* undo,size_t size,bool failed_page=false){
    assert(undo&&undo->size==size&&undo->before_image);
    auto* a=find(undo->before_image);assert(a&&a->size==size&&a->initialized);
    assert(a->kind==(expected_page(size)&&!failed_page?Kind::PAGE:Kind::ARRAY));
#if defined(WOS_ASYNC_BLOCK_IO) && defined(XFS_UNDO_HAS_PAGE_FLAG)
    assert(undo->before_image_page_allocated==(a->kind==Kind::PAGE));
#endif
}
auto capture(XfsTransaction* tp,BufHead* bp)->int {
    assert(!watched);watched=tp;old_head=tp->buf_undo;watched_bp=bp;old_refs=bp?bp->refcount.load():0;
    int const rc=xfs_trans_capture_buf(tp,bp);watched=nullptr;watched_bp=nullptr;old_head=nullptr;
    return rc;
}
}
void* operator new(size_t n){return fixture::allocate(n,fixture::Kind::SCALAR,false);}
void* operator new[](size_t n){return fixture::allocate(n,fixture::Kind::ARRAY,false);}
void* operator new(size_t n,const std::nothrow_t&) noexcept {return fixture::allocate(n,fixture::Kind::SCALAR,true);}
void* operator new[](size_t n,const std::nothrow_t&) noexcept {return fixture::allocate(n,fixture::Kind::ARRAY,true);}
void operator delete(void* p) noexcept {fixture::release(p,fixture::Kind::SCALAR);}
void operator delete[](void* p) noexcept {fixture::release(p,fixture::Kind::ARRAY);}
void operator delete(void* p,size_t) noexcept {::operator delete(p);}
void operator delete[](void* p,size_t) noexcept {::operator delete[](p);}
void operator delete(void* p,const std::nothrow_t&) noexcept {::operator delete(p);}
void operator delete[](void* p,const std::nothrow_t&) noexcept {::operator delete[](p);}
namespace ker::mod::mm::phys {
auto page_alloc_full_overwrite_may_fail(PhysicalPageOwner owner,uint64_t size,std::string_view name)->void* {
    using namespace fixture;++page_calls;
    assert(owner==PhysicalPageOwner::XFS_TRANSACTION_METADATA&&name=="xfs_undo");
    assert(watched&&watched->buf_undo==old_head&&watched_bp->refcount.load()==old_refs&&watched_bp->journal_pending.load()==0);
    if(fail_page)return nullptr;
    void* p=std::aligned_alloc(4096,((size+4095)/4096)*4096);assert(p);
    std::memset(p,POISON,size);register_allocation(p,size,Kind::PAGE);return p;
}
auto page_alloc_full_overwrite(PhysicalPageOwner,uint64_t,std::string_view)->void* {return nullptr;}
auto page_reassign_owner(void*,PhysicalPageOwner)->bool {return true;}
void page_free(void* p){fixture::release(p,fixture::Kind::PAGE);}
}
namespace ker::mod::dbg {
template<class... T>void log(const char*,T...){}
[[noreturn]]void panic_handler(const char*){std::abort();}
}
namespace ker::mod::sched {void kern_yield(){assert(false);}}
namespace ker::vfs {
void bjournal_hold(BufHead* bp){
    using namespace fixture;assert(watched&&bp==watched_bp&&watched->buf_undo==old_head);
    assert(bp->refcount.load()==old_refs&&bp->journal_pending.load()==0);
    auto* a=find(last_undo->before_image);assert(a&&!a->initialized&&a->size==bp->size);
    for(size_t i=0;i<a->size;++i)assert(static_cast<uint8_t*>(a->p)[i]==POISON);
    bp->journal_pending.fetch_add(1);++journal_holds;
}
void bjournal_release(BufHead* bp){assert(bp->journal_pending.fetch_sub(1)==1);++fixture::journal_releases;}
void brelse(BufHead* bp){assert(bp->refcount.fetch_sub(1)>1);}
void bdirty(BufHead* bp){assert(bp->journal_pending.load()==1);bp->flags|=BH_DIRTY;++fixture::dirty_calls;}
auto retire_bdev_range(dev::BlockDevice*,uint64_t,size_t)->bool {++fixture::retire_calls;return true;}
}
namespace ker::vfs::xfs {
auto xfs_inode_write(XfsInode*,XfsTransaction* tp)->int {if(fixture::inode_overflow)tp->overflowed=true;return fixture::inode_rc;}
auto xfs_log_prepare_transaction(XfsMountContext*)->int {return -ENODEV;}
auto xfs_log_write(XfsMountContext*,const XfsTransItem*,int,bool* owned)->int {
    ++fixture::log_calls;*owned=fixture::log_owned;return fixture::log_rc;
}
auto xfs_log_checkpoint_if_needed(XfsMountContext*)->int {++fixture::checkpoint_calls;return fixture::checkpoint_rc;}
void xfs_dentry_cache_invalidate_dir(XfsInode*){}
}
#define __builtin_memcpy fixture::copy
ACTUAL_TRANSACTION_BODIES
#undef __builtin_memcpy
'''

TESTS = r'''
namespace fixture {
struct Buffer {
    std::array<uint8_t,131072> bytes{};
    BufHead bp{};
    explicit Buffer(size_t size=4096,uint64_t block=10,ker::dev::BlockDevice* device=nullptr){
        assert(size<=bytes.size());bp.data=bytes.data();bp.size=size;bp.block_no=block;bp.bdev=device;bp.refcount.store(1);
        for(size_t i=0;i<size;++i)bytes[i]=uint8_t((i*31+17)%251);
    }
    void original() const {for(size_t i=0;i<bp.size;++i)assert(bytes[i]==uint8_t((i*31+17)%251));}
    void fill(uint8_t value){std::fill_n(bytes.begin(),bp.size,value);}
    void value(uint8_t expected)const {for(size_t i=0;i<bp.size;++i)assert(bytes[i]==expected);}
    ~Buffer(){assert(bp.refcount.load()==1&&bp.journal_pending.load()==0);}
};
auto transaction(XfsMountContext& mount)->XfsTransaction* {auto* tp=new XfsTransaction{};tp->mount=&mount;return tp;}
void tests(){
    static_assert(ker::mod::mm::paging::PAGE_SIZE==4096);
    static_assert(sizeof(XfsTransBufUndo)==40);
    for(size_t size:{size_t(1),size_t(512),size_t(2048),size_t(4095),size_t(4096),size_t(4097),size_t(6144),size_t(8192),size_t(12288),size_t(16384),size_t(32768),size_t(65535),size_t(65536),size_t(65537),size_t(131072)}){
        start("size-bound-full-restore");Buffer b(size);XfsMountContext m{};auto* tp=transaction(m);
        assert(capture(tp,&b.bp)==0);check_backing(tp->buf_undo,size);
        if(page_calls!=size_t(expected_page(size)))std::fprintf(stderr,"PAGE_POLICY size=%zu calls=%zu expected=%u\n",size,page_calls,unsigned(expected_page(size)));
        assert(page_calls==size_t(expected_page(size))&&array_calls==size_t(!expected_page(size)));
        auto* snapshot=tp->buf_undo;size_t const ac=array_calls,pc=page_calls;
        b.fill(0xa4);assert(capture(tp,&b.bp)==0&&tp->buf_undo==snapshot&&array_calls==ac&&page_calls==pc);
        xfs_trans_log_buf_full(tp,&b.bp);assert(b.bp.refcount.load()==3);
        xfs_trans_cancel(tp);b.original();assert(array_calls==ac&&page_calls==pc);
        assert(array_frees==size_t(!expected_page(size))&&page_frees==size_t(expected_page(size))&&log_calls==0);
    }
    for(size_t size:{size_t(4096),size_t(65536)}){
        start("physical-failure-exact-heap-fallback");Buffer b(size);XfsMountContext m{};auto* tp=transaction(m);fail_page=true;
        assert(capture(tp,&b.bp)==0);check_backing(tp->buf_undo,size,true);
        assert(page_calls==size_t(expected_page(size))&&array_calls==1);b.fill(0x76);xfs_trans_cancel(tp);b.original();
        assert(page_frees==0&&array_frees==1);
    }
    for(unsigned mode=0;mode<3;++mode){
        start("allocation-failure-no-publication");Buffer b(mode==1?6144:4096);XfsMountContext m{};auto* tp=transaction(m);
        if(mode==2)fail_scalar_size=sizeof(XfsTransBufUndo);else {fail_page=true;reject_all_arrays=true;}
        assert(capture(tp,&b.bp)==-ENOMEM&&tp->error==-ENOMEM&&tp->buf_undo==nullptr);
        assert(b.bp.refcount.load()==1&&b.bp.journal_pending.load()==0&&journal_holds==0);
        assert(page_calls==(mode==0?size_t(expected_page(4096)):0)&&array_calls==(mode==2?0:1));
        b.original();xfs_trans_cancel(tp);assert(log_calls==0);
    }
    {
        start("invalid-capture-no-allocation");Buffer b;XfsMountContext m{};XfsTransaction tp{};tp.mount=&m;
        assert(xfs_trans_capture_buf(nullptr,&b.bp)==-EINVAL&&xfs_trans_capture_buf(&tp,nullptr)==-EINVAL);
        auto* data=b.bp.data;b.bp.data=nullptr;assert(xfs_trans_capture_buf(&tp,&b.bp)==-EINVAL);b.bp.data=data;
        b.bp.size=0;assert(xfs_trans_capture_buf(&tp,&b.bp)==-EINVAL);b.bp.size=4096;
        tp.committed=true;assert(xfs_trans_capture_buf(&tp,&b.bp)==-EINVAL);tp.committed=false;tp.cancelled=true;
        assert(xfs_trans_capture_buf(&tp,&b.bp)==-EINVAL&&page_calls==0&&array_calls==0);
    }
    for(unsigned invalid=0;invalid<2;++invalid){
        start("invalid-alias-frees-before-size-publication");Buffer canonical,alias;XfsMountContext m{};auto* tp=transaction(m);
        assert(capture(tp,&canonical.bp)==0);auto* first=tp->buf_undo;auto* before=first->before_image;
        if(invalid==0)canonical.bp.data=nullptr;else first->before_image=nullptr;
        assert(capture(tp,&alias.bp)==-EIO&&tp->error==-EIO&&tp->buf_undo==first);
        assert(early_uninitialized_frees==1&&alias.bp.refcount.load()==1&&alias.bp.journal_pending.load()==0);
        assert(array_frees+page_frees==1&&find(before));canonical.bp.data=canonical.bytes.data();first->before_image=before;
        xfs_trans_cancel(tp);canonical.original();alias.original();assert(array_frees+page_frees==2);
    }
    {
        start("canonical-new-reused-alias-restore");Buffer canonical,alias;XfsMountContext m{};auto* tp=transaction(m);
        assert(capture(tp,&canonical.bp)==0);canonical.fill(0x73);xfs_trans_log_buf_full(tp,&canonical.bp);
        alias.fill(0x22);assert(capture(tp,&alias.bp)==0);check_backing(tp->buf_undo,4096);alias.value(0x73);
        for(size_t i=0;i<4096;++i)assert(tp->buf_undo->before_image[i]==uint8_t((i*31+17)%251));
        alias.bytes[22]=0x99;xfs_trans_log_buf(tp,&alias.bp,22,1);assert(canonical.bytes[22]==0x99&&tp->item_count==1);
        canonical.fill(0x45);auto* saved=tp->buf_undo;size_t const calls=array_calls+page_calls;
        assert(capture(tp,&alias.bp)==0&&tp->buf_undo==saved&&array_calls+page_calls==calls);alias.value(0x45);
        alias.bytes[300]=0xee;xfs_trans_log_buf(tp,&alias.bp,300,1);
        xfs_trans_cancel(tp);canonical.original();alias.original();assert(array_frees+page_frees==2);
    }
    {
        start("reused-alias-invalid-canonical-retains-images");Buffer canonical,alias;XfsMountContext m{};auto* tp=transaction(m);
        assert(capture(tp,&canonical.bp)==0);xfs_trans_log_buf_full(tp,&canonical.bp);assert(capture(tp,&alias.bp)==0);
        canonical.bp.data=nullptr;size_t const calls=array_calls+page_calls;
        assert(capture(tp,&alias.bp)==-EIO&&array_calls+page_calls==calls&&array_frees+page_frees==0);
        canonical.bp.data=canonical.bytes.data();xfs_trans_cancel(tp);canonical.original();alias.original();
    }
    for(unsigned difference=0;difference<3;++difference){
        start("distinct-span-independent-images");ker::dev::BlockDevice dev1{},dev2{};Buffer a(4096,10,&dev1),b(difference==2?8192:4096,10,&dev1);
        if(difference==0)b.bp.bdev=&dev2;if(difference==1)b.bp.block_no=11;
        XfsMountContext m{};auto* tp=transaction(m);assert(capture(tp,&a.bp)==0);a.fill(0x65);
        assert(capture(tp,&b.bp)==0);b.original();a.fill(0x77);b.fill(0x88);xfs_trans_cancel(tp);a.original();b.original();
    }
    for(unsigned result=0;result<9;++result){
        start("commit-success-error-ownership");Buffer b;XfsMountContext m{};m.mounted=true;auto* tp=transaction(m);XfsInode inode{};
        assert(capture(tp,&b.bp)==0);b.fill(0x66);xfs_trans_log_buf_full(tp,&b.bp);
        if(result==1){tp->error=-ENOMEM;}
        if(result==2){tp->overflowed=true;}
        if(result==3){xfs_trans_log_inode(tp,&inode);inode_rc=-EIO;}
        if(result==4){log_rc=-EIO;}
        if(result==5){log_rc=-EIO;log_owned=true;}
        if(result==6){log_owned=true;}
        if(result==7){checkpoint_rc=-EIO;}
        if(result==8){xfs_trans_log_inode(tp,&inode);inode_overflow=true;}
        int const expected=result==1?-ENOMEM:result==2||result==8?-EFBIG:result==3||result==4?-EIO:0;
        assert(xfs_trans_commit(tp)==expected);assert(array_frees+page_frees==1);
        if(expected!=0){b.original();assert(m.mutation_sequence.load()==0&&checkpoint_calls==0&&dirty_calls==0);}
        else {b.value(0x66);assert(m.mutation_sequence.load()==1&&m.lookup_sequence.load()==1&&checkpoint_calls==1&&dirty_calls==size_t(!log_owned));}
        assert(log_calls==size_t(result!=1&&result!=2&&result!=3&&result!=8));
    }
    {
        start("direct-restore-keeps-backing-discard-releases");Buffer a,b(6144,11);XfsMountContext m{};XfsTransaction tp{};tp.mount=&m;
        assert(capture(&tp,&a.bp)==0&&capture(&tp,&b.bp)==0);a.fill(0x12);b.fill(0x23);
        size_t const ac=array_calls,pc=page_calls;xfs_trans_restore_undo(&tp);a.original();b.original();
        assert(array_calls==ac&&page_calls==pc&&array_frees+page_frees==0);
        xfs_trans_discard_undo(&tp);assert(!tp.buf_undo&&array_frees+page_frees==2&&log_calls==0);
        xfs_trans_discard_undo(&tp);assert(array_frees+page_frees==2);
    }
    {
        start("destructor-discards-without-restore");Buffer a,b(6144,11);XfsMountContext m{};
        {XfsTransaction tp{};tp.mount=&m;assert(capture(&tp,&a.bp)==0&&capture(&tp,&b.bp)==0);a.fill(0x58);b.fill(0x59);}
        a.value(0x58);b.value(0x59);assert(array_frees+page_frees==2&&log_calls==0);
    }
    {
        start("pool-reset-retains-slot-clears-undo");Buffer b;XfsMountContext m{};XfsTransaction tp{};tp.mount=&m;
        XfsTransactionArena arena{};arena.total_slots=1;arena.permanent_reserve=true;tp.pool_arena=&arena;
        transaction_pool.arenas=&arena;transaction_pool.arena_count=1;
        assert(capture(&tp,&b.bp)==0);b.fill(0x71);xfs_trans_cancel(&tp);b.original();
        assert(transaction_pool.free_list==&tp&&arena.free_slots==1&&!tp.buf_undo&&!tp.mount&&!tp.cancelled);
        auto* reused=xfs_trans_pool_pop();assert(reused==&tp&&arena.free_slots==0);tp.mount=&m;
        assert(capture(&tp,&b.bp)==0);xfs_trans_reset_for_reuse(&tp);assert(!tp.buf_undo&&!tp.mount&&array_frees+page_frees==2);
        transaction_pool.arenas=nullptr;transaction_pool.arena_count=0;tp.pool_arena=nullptr;
    }
    {
        start("perag-inode-forks-full-cancel");Buffer b;XfsMountContext m{};XfsPerAG pag{};m.per_ag=&pag;m.ag_count=1;pag.agf_freeblks=123;
        XfsInode inode{};inode.mode=0100644;inode.nlink=2;inode.uid=1000;inode.size=44;
        inode.data_fork.format=XFS_DINODE_FMT_LOCAL;inode.data_fork.local.size=5;inode.data_fork.local.data=new uint8_t[5];
        std::memcpy(inode.data_fork.local.data,"abcde",5);inode.has_attr_fork=true;
        inode.attr_fork.format=XFS_DINODE_FMT_EXTENTS;inode.attr_fork.extents.count=1;inode.attr_fork.extents.capacity=4;
        inode.attr_fork.extents.list=xfs_ifork_extents_inline_data(inode.attr_fork.extents);inode.attr_fork.extents.list[0].br_startblock=71;
        auto* tp=transaction(m);assert(capture(tp,&b.bp)==0&&xfs_trans_capture_perag(tp,0)==0&&xfs_trans_capture_inode(tp,&inode)==0);
        assert(xfs_trans_capture_inode(tp,&inode)==0&&xfs_trans_capture_perag(tp,0)==0);
        b.fill(0x44);pag.agf_freeblks=0;inode.nlink=0;inode.uid=0;inode.size=77;inode.data_fork.local.data[0]='z';
        inode.attr_fork.extents.list[0].br_startblock=99;xfs_trans_cancel(tp);b.original();
        assert(pag.agf_freeblks==123&&inode.nlink==2&&inode.uid==1000&&inode.size==44);
        assert(std::memcmp(inode.data_fork.local.data,"abcde",5)==0&&inode.attr_fork.extents.list[0].br_startblock==71);
        assert(xfs_ifork_extents_uses_inline(inode.attr_fork.extents));xfs_trans_free_ifork(&inode.data_fork);xfs_trans_free_ifork(&inode.attr_fork);
    }
    {
        start("expanded-items-reset-destructor");XfsMountContext m{};XfsTransaction tp{};tp.mount=&m;
        std::array<XfsInode,33> inodes{};for(auto& ip:inodes)xfs_trans_log_inode(&tp,&ip);
        assert(tp.item_count==33&&tp.item_capacity==512&&tp.items!=tp.inline_items.data());
        xfs_trans_reset_for_reuse(&tp);assert(tp.items==tp.inline_items.data()&&tp.item_count==0&&array_frees==1);
    }
    clean();std::printf("PASS cases=%zu\n",cases);
}
}
int main(){fixture::tests();}
'''


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    source = SOURCE.read_text()
    start = source.index("namespace ker::vfs::xfs {")
    end = source.index("#ifdef WOS_SELFTEST", start)
    bodies = source[start:end] + "\n} // namespace ker::vfs::xfs\n"
    page_size = re.search(r"constexpr uint64_t PAGE_SIZE\s*=.*?;", (KERNEL / "platform/mm/paging.hpp").read_text()).group(0)
    harness = PRELUDE.replace("ACTUAL_PAGE_SIZE", page_size).replace("ACTUAL_TRANSACTION_BODIES", bodies) + TESTS
    modes = os.environ.get("WOS_XFS_UNDO_MODES", "on,off").split(",")
    retained = os.environ.get("WOS_XFS_UNDO_ARTIFACTS")
    with tempfile.TemporaryDirectory(prefix="xfs-undo-", dir=os.environ.get("TMPDIR")) as temporary:
        work = Path(temporary)
        for relative, content in {
            "platform/sys/locks.hpp": LOCKS,
            "platform/sys/spinlock.hpp": '#pragma once\n#include "locks.hpp"\n',
            "platform/sys/mutex.hpp": '#pragma once\n#include "locks.hpp"\n',
            "bits/off_t.h": "#pragma once\n#include <sys/types.h>\n",
            "vfs/fs/xfs/xfs_trans.hpp": HEADER.read_text(),
        }.items():
            target = work / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        cpp = work / "undo.cpp"
        cpp.write_text(harness)
        for mode in modes:
            command = [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(work), "-I", str(KERNEL)]
            if mode == "on":
                command += ["-DWOS_ASYNC_BLOCK_IO=1"]
            if "before_image_page_allocated" in HEADER.read_text():
                command += ["-DXFS_UNDO_HAS_PAGE_FLAG=1"]
            if os.environ.get("WOS_XFS_UNDO_ORIGINAL_POLICY") == "1":
                command += ["-DXFS_UNDO_ORIGINAL_POLICY=1"]
            binary = work / ("undo-" + mode)
            command += [str(cpp), "-o", str(binary)]
            artifact = Path(retained) / mode if retained else None
            if artifact:
                shutil.copytree(work, artifact, dirs_exist_ok=True)
                (artifact / "compiler-argv.json").write_text(json.dumps(command, indent=2) + "\n")
                (artifact / "binding.json").write_text(json.dumps({"source": str(SOURCE), "source_sha256": digest(SOURCE), "header": str(HEADER), "header_sha256": digest(HEADER), "fixture": str(Path(__file__).resolve()), "fixture_sha256": digest(__file__), "mode": mode, "original_policy": os.environ.get("WOS_XFS_UNDO_ORIGINAL_POLICY") == "1", "body_sha256": hashlib.sha256(bodies.encode()).hexdigest()}, indent=2) + "\n")
            result = subprocess.run(command, text=True, capture_output=True)
            if artifact:
                (artifact / "compile.log").write_text(result.stdout + result.stderr)
            if result.returncode:
                print(result.stdout + result.stderr, end="")
            result.check_returncode()
            if artifact:
                shutil.copyfile(binary, artifact / "undo.bin")
            environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1", LSAN_OPTIONS="detect_leaks=0")
            result = subprocess.run([str(binary)], text=True, capture_output=True, env=environment)
            print(result.stdout + result.stderr, end="")
            if artifact:
                (artifact / "runtime.log").write_text(result.stdout + result.stderr)
                (artifact / "result.json").write_text(json.dumps({"compile_exit": 0, "runtime_exit": result.returncode}, indent=2) + "\n")
            result.check_returncode()
            print(json.dumps({"mode": mode, "source_sha256": digest(SOURCE), "header_sha256": digest(HEADER), "pass": True}))


if __name__ == "__main__":
    main()
