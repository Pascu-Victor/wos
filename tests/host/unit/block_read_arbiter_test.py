#!/usr/bin/env python3
"""Compare the actual request arbiter with an independent ordering/depth oracle.

Both profiles include the selected full production header. Transparent private
instrumentation adds counters at existing loop/blocks entry boundaries; removing
those statements recovers the selected header byte-for-byte. Counts describe
algorithm visits, not measured latency. Saved headers use their actual original
runtime caller-side issued-depth loop, with independent safety expectations.
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
HEADER = Path(os.environ.get("WOS_BLOCK_ARBITER_HEADER", ROOT / "modules/kern/src/dev/block_request_model.hpp"))
OLD_RUNTIME = Path(os.environ.get("WOS_BLOCK_ARBITER_OLD_RUNTIME", ROOT / "build/goal30-evidence/read-arbiter-v20/source/modules/kern/src/dev/block_request.cpp"))


def extract(text, anchor):
    start = text.index(anchor)
    brace = text.index("{", start)
    end, depth = brace + 1, 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    return text[start:end]


VISIT = "\n        ++fixture::slot_visits; // transparent loop entry probe\n"
CONFLICT = "\n    ++fixture::conflict_calls; // transparent predicate entry probe\n"

PRELUDE = r'''
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <tuple>
namespace fixture {inline size_t slot_visits{},conflict_calls{};}
#include "selected-model.hpp"
namespace fixture {
using namespace ker::dev;
size_t cases{},checks{};
auto pending(const BlockRequestModel& r)->bool {
    return r.state==BlockRequestState::QUEUED||r.state==BlockRequestState::ISSUED||r.state==BlockRequestState::COMPLETING;
}
auto oracle(const BlockQueueModel& q,size_t depth)->size_t {
    if(q.resetting)return q.requests.size();
    size_t active=0;for(auto const& r:q.requests)if(r.state==BlockRequestState::ISSUED)++active;
    if(active>=depth)return q.requests.size();
    size_t selected=q.requests.size();
    for(size_t i=0;i<q.requests.size();++i){
        auto const& candidate=q.requests[i];if(candidate.state!=BlockRequestState::QUEUED)continue;
        bool eligible=true;
        for(auto const& earlier:q.requests){
            if(!pending(earlier)||earlier.sequence>=candidate.sequence)continue;
            if(earlier.op==BlockRequestOperation::FLUSH||candidate.op==BlockRequestOperation::FLUSH){eligible=false;break;}
            // Independent inclusive interval oracle; admission rejects wrapping.
            bool overlap=earlier.lba<=candidate.lba+candidate.count-1&&candidate.lba<=earlier.lba+earlier.count-1;
            if(overlap&&(earlier.op==BlockRequestOperation::WRITE||candidate.op==BlockRequestOperation::WRITE)){eligible=false;break;}
        }
        if(eligible&&(selected==q.requests.size()||candidate.sequence<q.requests[selected].sequence))selected=i;
    }
    return selected;
}
auto stats(const BlockQueueModelStats& s){
    return std::tie(s.submitted,s.admission_rejections,s.issued,s.completion_claims,s.completed,s.released,
        s.cancellation_requests,s.timeouts,s.resets,s.stale_completions,s.invalid_completions,s.generation_exhaustions,
        s.request_high_water,s.byte_high_water);
}
void same(const BlockQueueModel& actual,const BlockQueueModel& expected){
    assert(actual.generation==expected.generation&&actual.next_sequence==expected.next_sequence);
    assert(actual.admission_open==expected.admission_open&&actual.resetting==expected.resetting);
    assert(actual.owned_requests==expected.owned_requests&&actual.owned_bytes==expected.owned_bytes);
    assert(actual.limits.max_requests==expected.limits.max_requests&&actual.limits.max_bytes==expected.limits.max_bytes&&actual.limits.max_request_bytes==expected.limits.max_request_bytes);
    assert(stats(actual.stats)==stats(expected.stats));
    for(size_t i=0;i<actual.requests.size();++i){
        auto const& a=actual.requests[i];auto const& b=expected.requests[i];
        assert(a.state==b.state&&a.op==b.op&&a.lba==b.lba&&a.count==b.count&&a.bytes==b.bytes&&a.sequence==b.sequence);
        assert(a.token==b.token&&a.cancel_reason==b.cancel_reason&&a.status==b.status&&a.completed_bytes==b.completed_bytes);
    }
}
auto actual_issue(BlockQueueModel& queue,BlockRequestToken& token,size_t depth)->bool {
#ifdef ARBITER_HAS_DEPTH
    return block_model_next_issue(queue,token,depth);
#else
    ACTUAL_OLD_RUNTIME_DEPTH_LOOP
    return issued<depth&&block_model_next_issue(queue,token);
#endif
}
auto issue(BlockQueueModel& queue,size_t depth=BLOCK_MODEL_MAX_REQUESTS,bool default_call=false)->BlockRequestToken {
    ++checks;auto expected=queue;size_t const chosen=oracle(queue,default_call?BLOCK_MODEL_MAX_REQUESTS:depth);
    BlockRequestToken const sentinel{UINT64_MAX,UINT64_MAX,UINT16_MAX};BlockRequestToken token=sentinel;
    bool const result=default_call?block_model_next_issue(queue,token):actual_issue(queue,token,depth);
    if(result!=(chosen!=queue.requests.size()))std::fprintf(stderr,"ORACLE_ELIGIBILITY depth=%zu chosen=%zu result=%u\n",depth,chosen,unsigned(result));
    assert(result==(chosen!=queue.requests.size()));
    if(result){
        assert(token==expected.requests[chosen].token);expected.requests[chosen].state=BlockRequestState::ISSUED;
        if(expected.stats.issued!=UINT64_MAX)++expected.stats.issued;
    }else assert(token==sentinel);
    same(queue,expected);return token;
}
auto add(BlockQueueModel& q,BlockRequestOperation op=BlockRequestOperation::READ,uint64_t lba=0,size_t count=1)->BlockRequestToken {
    BlockRequestToken t{};BlockRequestDescription d{op,lba,count,count*512};
    if(op==BlockRequestOperation::FLUSH)d={op};assert(block_model_submit(q,d,t)==0);return t;
}
void finish(BlockQueueModel& q,BlockRequestToken t,bool release=false){
    assert(block_model_claim_completion(q,t,0,q.requests[t.slot].bytes));
    assert(block_model_finish_completion(q,t));if(release)assert(block_model_release(q,t));
}
void begin(const char* name){++cases;std::printf("CASE %s\n",name);std::fflush(stdout);slot_visits=conflict_calls=0;}
void synthetic(BlockQueueModel& q,size_t slot,BlockRequestState state,BlockRequestOperation op,uint64_t sequence,uint64_t lba=0){
    auto& r=q.requests[slot];r.state=state;r.op=op;r.sequence=sequence;r.token={q.generation,1,uint16_t(slot)};
    r.lba=op==BlockRequestOperation::FLUSH?0:lba;r.count=op==BlockRequestOperation::FLUSH?0:1;r.bytes=r.count*512;
    if(state==BlockRequestState::COMPLETING||state==BlockRequestState::TERMINAL)r.completed_bytes=r.bytes;
    ++q.owned_requests;q.owned_bytes+=r.bytes;++q.stats.submitted;q.next_sequence=std::max(q.next_sequence,sequence+1);
}
bool const original_cost=
#ifdef ARBITER_ORIGINAL_COST
    true;
#else
    false;
#endif
bool const fast_profile=
#ifdef WOS_ASYNC_BLOCK_IO
    !original_cost;
#else
    false;
#endif
void tests(){
    static_assert(BLOCK_MODEL_MAX_REQUESTS==64);
    for(size_t depth:{size_t(0),size_t(1),size_t(8),size_t(64),size_t(65),SIZE_MAX}){
        begin("explicit-depth-bound");BlockQueueModel q{};std::array<BlockRequestToken,64> submitted{};
        for(auto& t:submitted)t=add(q);size_t const wanted=std::min(depth,size_t(64));
        for(size_t i=0;i<wanted;++i)assert(issue(q,depth)==submitted[i]);
        assert(issue(q,depth).slot==UINT16_MAX);assert(q.stats.issued==wanted&&q.owned_requests==64);
    }
    {
        begin("default-call-all64");BlockQueueModel q{};std::array<BlockRequestToken,64> submitted{};
        for(auto& t:submitted)t=add(q);for(auto t:submitted)assert(issue(q,64,true)==t);
        assert(issue(q,64,true).slot==UINT16_MAX);assert(q.stats.issued==64);
    }
    {
        begin("pure-read-batch8-visits");BlockQueueModel q{};std::array<BlockRequestToken,8> submitted{};
        for(auto& t:submitted)t=add(q);for(auto t:submitted)assert(issue(q,8)==t);assert(issue(q,8).slot==UINT16_MAX);
        size_t const expected_visits=fast_profile?576:1600;size_t const expected_conflicts=fast_profile?0:512;
        std::printf("BATCH_VISITS actual=%zu expected=%zu blocks=%zu expected_blocks=%zu\n",slot_visits,expected_visits,conflict_calls,expected_conflicts);std::fflush(stdout);
        assert(slot_visits==expected_visits&&conflict_calls==expected_conflicts);
    }
    {
        begin("mixed-read-write-batch8-old-fallback");BlockQueueModel q{};std::array<BlockRequestToken,8> submitted{};
        for(auto& t:submitted)t=add(q);add(q,BlockRequestOperation::WRITE,30);
        for(auto t:submitted)assert(issue(q,8)==t);assert(issue(q,8).slot==UINT16_MAX);
        assert(slot_visits==1600&&conflict_calls==512);
    }
    for(auto op:{BlockRequestOperation::WRITE,BlockRequestOperation::FLUSH}){
        for(auto state:{BlockRequestState::QUEUED,BlockRequestState::ISSUED,BlockRequestState::COMPLETING,BlockRequestState::TERMINAL}){
            begin("nonread-lifetime-exclusion");BlockQueueModel q{};synthetic(q,63,state,op,1);synthetic(q,0,BlockRequestState::QUEUED,BlockRequestOperation::READ,2);
            auto const selected=issue(q,8);
            if(state==BlockRequestState::TERMINAL){assert(selected.slot==0);if(fast_profile)assert(slot_visits==64&&conflict_calls==0);}
            else if(state==BlockRequestState::QUEUED){assert(selected.slot==63);assert(conflict_calls>0);}
            else {assert(selected.slot==UINT16_MAX&&conflict_calls>0);}
        }
    }
    for(auto state:{BlockRequestState::ISSUED,BlockRequestState::COMPLETING,BlockRequestState::TERMINAL}){
        begin("read-lifetime-and-depth");BlockQueueModel q{};synthetic(q,63,state,BlockRequestOperation::READ,1);
        synthetic(q,0,BlockRequestState::QUEUED,BlockRequestOperation::READ,2);
        auto t=issue(q,1);assert(t.slot==(state==BlockRequestState::ISSUED?UINT16_MAX:0));
        if(fast_profile)assert(slot_visits==64&&conflict_calls==0);
    }
    {
        begin("nonread-first-late-issued-still-counted");BlockQueueModel q{};
        synthetic(q,0,BlockRequestState::QUEUED,BlockRequestOperation::WRITE,2,50);
        synthetic(q,1,BlockRequestState::QUEUED,BlockRequestOperation::READ,3,70);
        synthetic(q,63,BlockRequestState::ISSUED,BlockRequestOperation::READ,1,90);
        assert(issue(q,1).slot==UINT16_MAX&&slot_visits==64&&conflict_calls==0);
        assert(issue(q,8).slot==0);
    }
    {
        begin("min-sequence-not-slot-or-first");BlockQueueModel q{};
        for(size_t i=0;i<64;++i)synthetic(q,i,BlockRequestState::QUEUED,BlockRequestOperation::READ,64-i,0);
        for(size_t i=0;i<64;++i)assert(issue(q,64).slot==63-i);
    }
    {
        begin("strict-sequence-tie-preserves-first-slot");BlockQueueModel q{};
        synthetic(q,1,BlockRequestState::QUEUED,BlockRequestOperation::READ,1);
        synthetic(q,63,BlockRequestState::QUEUED,BlockRequestOperation::READ,1);
        assert(issue(q,8).slot==1);
    }
    {
        begin("overlap-and-independent-bypass");BlockQueueModel q{};
        auto const a=add(q,BlockRequestOperation::READ,0,2),w=add(q,BlockRequestOperation::WRITE,1),r=add(q,BlockRequestOperation::READ,1),ind=add(q,BlockRequestOperation::READ,20);
        assert(issue(q,8)==a&&issue(q,8)==ind&&issue(q,8).slot==UINT16_MAX);
        assert(block_model_claim_completion(q,a,0,1024));assert(issue(q,8).slot==UINT16_MAX);
        assert(block_model_finish_completion(q,a));assert(issue(q,8)==w&&issue(q,8).slot==UINT16_MAX);
        finish(q,w);assert(issue(q,8)==r);
    }
    {
        begin("flush-barrier-through-callback-retirement");BlockQueueModel q{};
        auto a=add(q),b=add(q,BlockRequestOperation::READ,10),f=add(q,BlockRequestOperation::FLUSH),later=add(q,BlockRequestOperation::READ,50);
        assert(issue(q,8)==a&&issue(q,8)==b&&issue(q,8).slot==UINT16_MAX);finish(q,b);
        assert(block_model_claim_completion(q,a,0,512));assert(issue(q,8).slot==UINT16_MAX);assert(block_model_finish_completion(q,a));
        assert(issue(q,8)==f&&issue(q,8).slot==UINT16_MAX);assert(block_model_claim_completion(q,f,0,0));
        assert(issue(q,8).slot==UINT16_MAX);assert(block_model_finish_completion(q,f));assert(issue(q,8)==later);
        assert(q.owned_requests==4&&q.owned_bytes==1536);
    }
    {
        begin("queued-canceled-write-completing-still-blocks");BlockQueueModel q{};
        auto w=add(q,BlockRequestOperation::WRITE),r=add(q);
        assert(block_model_cancel(q,w)==BlockCancelResult::CLAIMED&&issue(q,8).slot==UINT16_MAX);
        assert(!block_model_release(q,w));assert(block_model_finish_completion(q,w));assert(issue(q,8)==r);
    }
    {
        begin("issued-canceled-write-retirement-before-read");BlockQueueModel q{};
        auto w=add(q,BlockRequestOperation::WRITE),r=add(q);assert(issue(q,8)==w);
        assert(block_model_cancel(q,w,-ETIMEDOUT)==BlockCancelResult::NEEDS_RETIREMENT&&issue(q,8).slot==UINT16_MAX);
        assert(block_model_claim_completion(q,w,0,512));assert(q.requests[w.slot].status==-ETIMEDOUT&&q.requests[w.slot].completed_bytes==0);
        assert(issue(q,8).slot==UINT16_MAX&&block_model_finish_completion(q,w));assert(issue(q,8)==r);
    }
    {
        begin("canceled-read-does-not-conflict");BlockQueueModel q{};auto a=add(q),b=add(q);assert(issue(q,8)==a);
        assert(block_model_cancel(q,a)==BlockCancelResult::NEEDS_RETIREMENT&&issue(q,8)==b);
    }
    {
        begin("defer-preserves-order-and-raced-cancel");BlockQueueModel q{};auto a=add(q),b=add(q);
        assert(issue(q,1)==a&&block_model_defer_issue(q,a));assert(issue(q,1)==a);
        assert(block_model_cancel(q,a)==BlockCancelResult::NEEDS_RETIREMENT&&block_model_defer_issue(q,a));
        assert(q.requests[a.slot].state==BlockRequestState::COMPLETING&&issue(q,1)==b);
        assert(block_model_finish_completion(q,a)&&block_model_release(q,a));
    }
    {
        begin("closed-admission-existing-read-drains");BlockQueueModel q{};auto a=add(q);q.admission_open=false;
        assert(issue(q,1)==a);
    }
    {
        begin("reset-no-mutation-then-old-tokens-release");BlockQueueModel q{};auto a=add(q),b=add(q);assert(issue(q,8)==a);
        assert(block_model_begin_reset(q)==0);assert(issue(q,8).slot==UINT16_MAX);assert(issue(q,64,true).slot==UINT16_MAX);
        assert(block_model_claim_completion(q,a,0,512));assert(block_model_finish_completion(q,a)&&block_model_finish_completion(q,b));
        assert(block_model_finish_reset(q)==0&&block_model_release(q,a)&&block_model_release(q,b));auto successor=add(q);
        assert(successor.queue_generation==2&&issue(q,8)==successor);
    }
    {
        begin("slot-reuse-oldest-queue-survives-low-slot-newcomer");BlockQueueModel q{};std::array<BlockRequestToken,64> t{};
        for(auto& x:t)x=add(q);for(size_t i=0;i<63;++i){assert(issue(q,64)==t[i]);finish(q,t[i],true);}
        auto new_first=add(q);assert(new_first.slot==0&&new_first.request_generation==2);
        assert(issue(q,64)==t[63]&&issue(q,64)==new_first);assert(!block_model_claim_completion(q,t[0],0,512));
    }
    {
        begin("stat-saturation-and-sequence-boundary");BlockQueueModel q{};q.next_sequence=UINT64_MAX-1;auto a=add(q);q.stats.issued=UINT64_MAX;
        assert(issue(q,1)==a&&q.stats.issued==UINT64_MAX);BlockRequestToken out{};
        assert(block_model_submit(q,{BlockRequestOperation::READ,0,1,512},out)==-EOVERFLOW);
        assert(issue(q,1).slot==UINT16_MAX);
    }
    {
        begin("empty-and-retained-terminal-no-queued");BlockQueueModel q{};assert(issue(q,8).slot==UINT16_MAX);
        auto a=add(q);assert(issue(q,8)==a);finish(q,a);assert(issue(q,8).slot==UINT16_MAX&&q.owned_requests==1&&q.owned_bytes==512);
    }
    // Bounded deterministic differential states exercise valid admission and
    // lifecycle transitions. The oracle checks every issuance and every field.
    for(unsigned seed=1;seed<=24;++seed){
        begin("deterministic-mixed-lifecycle-oracle");BlockQueueModel q{};uint64_t random=seed;
        for(unsigned step=0;step<160;++step){
            random=random*6364136223846793005ULL+1442695040888963407ULL;
            size_t slot=(random>>16)%64;auto token=q.requests[slot].token;
            switch((random>>8)%8){
                case 0:{BlockRequestOperation op=BlockRequestOperation((random>>24)%3);BlockRequestDescription d{op,(random>>32)%16,1,512};if(op==BlockRequestOperation::FLUSH)d={op};BlockRequestToken added{};static_cast<void>(block_model_submit(q,d,added));break;}
                case 1:static_cast<void>(issue(q,std::array<size_t,5>{0,1,8,64,65}[(random>>28)%5]));break;
                case 2:static_cast<void>(block_model_cancel(q,token,(random&1)?-ECANCELED:-ETIMEDOUT));break;
                case 3:static_cast<void>(block_model_claim_completion(q,token,0,q.requests[slot].bytes));break;
                case 4:static_cast<void>(block_model_finish_completion(q,token));break;
                case 5:static_cast<void>(block_model_release(q,token));break;
                case 6:static_cast<void>(block_model_defer_issue(q,token));break;
                case 7:if(step%19==0)static_cast<void>(block_model_begin_reset(q));else if(q.resetting)static_cast<void>(block_model_finish_reset(q));break;
            }
        }
    }
    std::printf("PASS cases=%zu oracle_checks=%zu\n",cases,checks);
}
}
int main(){fixture::tests();}
'''


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    original = HEADER.read_text()
    selector = extract(original, "inline auto block_model_next_issue(")
    predicate = extract(original, "inline auto blocks(")
    instrumented_selector, loops = re.subn(r"(for\s*\([^\n]+:\s*queue\.requests\)\s*\{)", lambda match: match.group(0) + VISIT, selector)
    assert loops in (2, 3), loops
    instrumented_predicate = predicate[:predicate.index("{") + 1] + CONFLICT + predicate[predicate.index("{") + 1:]
    instrumented = original.replace(selector, instrumented_selector).replace(predicate, instrumented_predicate)
    assert instrumented.replace(VISIT, "").replace(CONFLICT, "") == original
    has_depth = "max_issued" in selector[:selector.index("{")]
    old_depth = ""
    if not has_depth:
        runtime = OLD_RUNTIME.read_text()
        start = runtime.index("        size_t issued = 0;", runtime.index("auto service_queue("))
        end = runtime.index("        BlockRequestToken token{};", start)
        old_depth = runtime[start:end]
        assert "if (state.state == BlockRequestState::ISSUED) ++issued;" in old_depth
        old_depth = old_depth.replace("queue.model.requests", "queue.requests")
        old_depth = re.sub(r"(for\s*\([^\n]+:\s*queue\.requests\)\s*\{)", lambda match: match.group(0) + VISIT, old_depth)
    harness = PRELUDE.replace("ACTUAL_OLD_RUNTIME_DEPTH_LOOP", old_depth)
    modes = os.environ.get("WOS_BLOCK_ARBITER_MODES", "on,off").split(",")
    retained = os.environ.get("WOS_BLOCK_ARBITER_ARTIFACTS")
    with tempfile.TemporaryDirectory(prefix="block-arbiter-", dir=os.environ.get("TMPDIR")) as temporary:
        work = Path(temporary)
        (work / "selected-model.hpp").write_text(instrumented)
        (work / "arbiter.cpp").write_text(harness)
        for mode in modes:
            command = [os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-I", str(work)]
            if mode == "on":
                command += ["-DWOS_ASYNC_BLOCK_IO=1"]
            if has_depth:
                command += ["-DARBITER_HAS_DEPTH=1"]
            if os.environ.get("WOS_BLOCK_ARBITER_ORIGINAL_COST") == "1":
                command += ["-DARBITER_ORIGINAL_COST=1"]
            binary = work / ("arbiter-" + mode)
            command += [str(work / "arbiter.cpp"), "-o", str(binary)]
            artifact = Path(retained) / mode if retained else None
            if artifact:
                shutil.copytree(work, artifact, dirs_exist_ok=True)
                (artifact / "original-model.hpp").write_text(original)
                (artifact / "compiler-argv.json").write_text(json.dumps(command, indent=2) + "\n")
                (artifact / "binding.json").write_text(json.dumps({"header": str(HEADER), "header_sha256": digest(HEADER), "fixture": str(Path(__file__).resolve()), "fixture_sha256": digest(__file__), "selector_sha256": hashlib.sha256(selector.encode()).hexdigest(), "mode": mode, "has_depth_parameter": has_depth, "original_cost_profile": os.environ.get("WOS_BLOCK_ARBITER_ORIGINAL_COST") == "1", "instrumented_loops": loops, "instrumentation_inverse_exact": True, "old_runtime": str(OLD_RUNTIME) if not has_depth else None, "old_runtime_sha256": digest(OLD_RUNTIME) if not has_depth else None}, indent=2) + "\n")
            result = subprocess.run(command, text=True, capture_output=True)
            if artifact:
                (artifact / "compile.log").write_text(result.stdout + result.stderr)
            if result.returncode:
                print(result.stdout + result.stderr, end="")
            result.check_returncode()
            if artifact:
                shutil.copyfile(binary, artifact / "arbiter.bin")
            environment = dict(os.environ, ASAN_OPTIONS="detect_leaks=0:halt_on_error=1", LSAN_OPTIONS="detect_leaks=0")
            result = subprocess.run([str(binary)], text=True, capture_output=True, env=environment)
            print(result.stdout + result.stderr, end="")
            if artifact:
                (artifact / "runtime.log").write_text(result.stdout + result.stderr)
                (artifact / "result.json").write_text(json.dumps({"compile_exit": 0, "runtime_exit": result.returncode}, indent=2) + "\n")
            result.check_returncode()
            print(json.dumps({"mode": mode, "header_sha256": digest(HEADER), "pass": True}))


if __name__ == "__main__":
    main()
