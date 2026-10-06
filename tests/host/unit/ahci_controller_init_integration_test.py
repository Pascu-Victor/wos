#!/usr/bin/env python3
"""Execute the complete production AHCI controller-init body at host seams."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE_PATH = Path(os.environ.get("WOS_AHCI_SOURCE", ROOT / "modules/kern/src/dev/ahci.cpp"))


def extract(source, prefix):
    start = source.index(prefix)
    brace = source.index("{", start)
    end, depth = brace + 1, 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    return source[start:end]


PRELUDE = r'''
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <vector>
#include <dev/pci.hpp>
#include <dev/ahci.hpp>
#include <platform/mm/paging.hpp>
int held=0,find_calls=0,init_calls=0,base_calls=0,bme_disables=0,memory_enables=0,hhdm_reads=0,error_logs=0;
bool discovered=true,stubborn_coherence=false,bme=true;
uint64_t hhdm=0xffff800000000000ULL,kernel_end=0;
uint8_t capability=0;
uint16_t config_control=0;
struct Mapping{uint64_t virtual_address,physical_address,flags;};
std::vector<Mapping> mappings;
enum Event{enable=1,map=2,publish=3,init=4,disable=5,coherence=6};
std::vector<Event> events;
ker::dev::pci::PCIDevice fixture_device{};
// Fixture-only linker-symbol input: old production body executes unchanged.
#define __kernel_end reinterpret_cast<char*>(kernel_end)
namespace ker::mod::sys {
struct Mutex{};
struct MutexGuard{explicit MutexGuard(Mutex&){assert(held==0);++held;}~MutexGuard(){assert(held==1);--held;}};
}
namespace ker::mod::mm::addr {uint64_t get_hhdm_offset(){assert(held==1);++hhdm_reads;return hhdm;}}
namespace ker::mod::mm::virt {
bool kernel_nx_supported=true;
void map_to_kernel_page_table(uint64_t v,uint64_t p,uint64_t flags){assert(held==1&&base_calls==0&&init_calls==0);mappings.push_back({v,p,flags});events.push_back(map);}
'''

MIDDLE = r'''
}
namespace ker::dev::pci {
PCIDevice* pci_find_ahci_controller(){assert(held==1);++find_calls;return discovered?&fixture_device:nullptr;}
void pci_enable_memory_space(PCIDevice* d){assert(held==1&&d==&fixture_device);++memory_enables;events.push_back(enable);}
bool pci_set_bus_master(PCIDevice* d,bool value){assert(held==1&&d==&fixture_device&&!value);bme=value;++bme_disables;events.push_back(disable);return true;}
uint8_t pci_find_capability(PCIDevice* d,uint8_t id){assert(held==1&&d==&fixture_device&&id==0x10);return capability;}
uint16_t pci_config_read16(uint8_t bus,uint8_t slot,uint8_t function,uint8_t offset){assert(held==1&&bus==1&&slot==2&&function==3&&offset==uint8_t(capability+8));return config_control;}
void pci_config_write16(uint8_t bus,uint8_t slot,uint8_t function,uint8_t offset,uint16_t value){assert(held==1&&bus==1&&slot==2&&function==3&&offset==uint8_t(capability+8)&&mappings.empty()&&base_calls==0&&init_calls==0);events.push_back(coherence);if(!stubborn_coherence)config_control=value;}
}
namespace ker::dev::ahci {
struct log{template<class...T>static void info(const char*,T...){}template<class...T>static void error(const char*,T...){++error_logs;}};
mod::sys::Mutex lifecycle_lock{};
pci::PCIDevice* controller=nullptr;
bool shutdown_attempted=false;
volatile HbaMem* hba_mem=nullptr;
void ahci_set_base(volatile uint32_t* base){assert(held==1&&mappings.size()==2&&init_calls==0);hba_mem=reinterpret_cast<volatile HbaMem*>(base);++base_calls;events.push_back(publish);}
int ahci_init(){assert(held==1&&controller==&fixture_device&&mappings.size()==2&&base_calls==1);++init_calls;events.push_back(init);return 0;}
'''

TESTS = r'''
void setup(uint32_t bar=0xfebf0000,uint64_t offset=0xffff800000000000ULL,uint64_t end=0xffffffff81004000ULL){
    assert(held==0);fixture_device={};fixture_device.bus=1;fixture_device.slot=2;fixture_device.function=3;fixture_device.bar[5]=bar;
    controller=nullptr;shutdown_attempted=false;hba_mem=nullptr;discovered=true;stubborn_coherence=false;bme=true;capability=0;config_control=0;
    hhdm=offset;kernel_end=end;mod::mm::virt::kernel_nx_supported=true;mappings.clear();events.clear();find_calls=init_calls=base_calls=bme_disables=memory_enables=hhdm_reads=error_logs=0;
}
void assert_mapping(uint64_t address,uint64_t physical){
    assert(held==0&&mappings.size()==2&&base_calls==1&&init_calls==1&&memory_enables==1&&bme_disables==0&&bme&&error_logs==0&&controller==&fixture_device);
    auto const flags=mod::mm::paging::page_types::MMIO|(mod::mm::virt::kernel_nx_supported?mod::mm::paging::PAGE_NX:0);
    for(size_t i=0;i<2;++i){assert(mappings[i].virtual_address==address+i*4096&&mappings[i].physical_address==physical+i*4096&&mappings[i].flags==flags);assert((mappings[i].flags&mod::mm::paging::PAGE_USER)==0);}
    assert(reinterpret_cast<uint64_t>(hba_mem)==address&&events==std::vector<Event>({enable,map,map,publish,init}));
}
void assert_rejected(){assert(held==0&&mappings.empty()&&base_calls==0&&init_calls==0&&memory_enables==0&&bme_disables==1&&!bme&&controller==nullptr&&hba_mem==nullptr&&error_logs==1&&events==std::vector<Event>{disable});}
void wrap_control(){
    setup(0xfebf0000,0xffff800000000000ULL,0xffffffffffe49000ULL);ahci_controller_init();
    std::fprintf(stderr,"AHCI captured-wrap mapped=0x%llx physical=0x%llx pages=%zu base=0x%llx init=%d\n",static_cast<unsigned long long>(mappings.empty()?0:mappings[0].virtual_address),static_cast<unsigned long long>(mappings.empty()?0:mappings[0].physical_address),mappings.size(),static_cast<unsigned long long>(reinterpret_cast<uint64_t>(hba_mem)),init_calls);
    assert(mappings.size()==2&&mappings[0].virtual_address>=0xffff800000000000ULL);
    assert_mapping(0xffff8000febf0000ULL,0xfebf0000);
}
void old_safe_high_control(){
    setup();ahci_controller_init();assert_mapping(0xffffffff81204000ULL,0xfebf0000);
    std::fprintf(stderr,"AHCI old-safe-high pages=2 mapped=0xffffffff81204000 init=1\n");
}
void full_matrix(){
    wrap_control();
    // Image-end input has no influence on either physical or virtual mapping.
    for(uint64_t end:{uint64_t(0),uint64_t(0x200000),uint64_t(0xffffffff81000000ULL),uint64_t(0xffffffffffe49000ULL),UINT64_MAX})for(uint32_t flags:{0U,2U,4U,8U,14U}){
        setup(0xfebf0000|flags,0xffff800000000000ULL,end);ahci_controller_init();assert_mapping(0xffff8000febf0000ULL,0xfebf0000);assert(hhdm_reads==1);
    }
    setup();mod::mm::virt::kernel_nx_supported=false;ahci_controller_init();assert_mapping(0xffff8000febf0000ULL,0xfebf0000);
    // Inclusive upper endpoint: the second page may end exactly at UINT64_MAX.
    setup(0x1000,0xffffffffffffd000ULL);ahci_controller_init();assert_mapping(0xffffffffffffe000ULL,0x1000);
    setup(0xfffff000);ahci_controller_init();assert_mapping(0xffff8000fffff000ULL,0xfffff000);
    for(uint32_t bar:{0U,1U,8U,15U,UINT32_MAX,0xfebf0001U,0xfebf0010U,0xfebf0100U,0x10U}){setup(bar);ahci_controller_init();assert_rejected();}
    for(uint64_t offset:{uint64_t(0),uint64_t(0x8000000000000000ULL),uint64_t(0x0000800000000000ULL),uint64_t(0xffff7fffffff0000ULL),uint64_t(0xffff800000000001ULL),uint64_t(0xfffffffffffff000ULL)}){setup(0x1000,offset);ahci_controller_init();assert_rejected();}
    // Addition fits but the fixed second-page span does not.
    setup(0x1000,0xffffffffffffe000ULL);ahci_controller_init();assert_rejected();
    // Coherence setup remains before mapping/publication, with exact flags.
    for(uint8_t cap:{uint8_t(0x50),uint8_t(0xf4)}){
        setup();capability=cap;config_control=0x830;ahci_controller_init();assert(config_control==0x20&&mappings.size()==2&&base_calls==1&&init_calls==1&&bme_disables==0&&events==std::vector<Event>({enable,coherence,map,map,publish,init}));
        assert(mappings[0].virtual_address==0xffff8000febf0000ULL&&mappings[0].flags==(mod::mm::paging::page_types::MMIO|mod::mm::paging::PAGE_NX));
    }
    for(uint8_t cap:{uint8_t(0xf5),uint8_t(0xff)}){setup();capability=cap;ahci_controller_init();assert(held==0&&mappings.empty()&&base_calls==0&&init_calls==0&&bme_disables==1&&!bme&&controller==&fixture_device&&events==std::vector<Event>({enable,disable}));}
    setup();capability=0x50;config_control=0x830;stubborn_coherence=true;ahci_controller_init();assert(held==0&&mappings.empty()&&base_calls==0&&init_calls==0&&bme_disables==1&&!bme&&error_logs==1&&events==std::vector<Event>({enable,coherence,disable}));
    setup();controller=&fixture_device;ahci_controller_init();assert(held==0&&find_calls==0&&hhdm_reads==0&&events.empty()&&mappings.empty());
    setup();shutdown_attempted=true;ahci_controller_init();assert(held==0&&find_calls==0&&hhdm_reads==0&&events.empty()&&mappings.empty());
    setup();discovered=false;ahci_controller_init();assert(held==0&&find_calls==1&&hhdm_reads==0&&events.empty()&&mappings.empty()&&controller==nullptr&&bme&&bme_disables==0);
    std::fprintf(stderr,"AHCI controller-init matrix PASS: exact2pages,maskedBAR,checkedhighHHDM,endbounds,noinvalidmap,coherence,lifecycle\n");
}
}
int main(){
#if defined(AHCI_INIT_WRAP_CONTROL)
    ker::dev::ahci::wrap_control();
#elif defined(AHCI_INIT_OLD_SAFE_CONTROL)
    ker::dev::ahci::old_safe_high_control();
#else
    ker::dev::ahci::full_matrix();
#endif
}
'''


def main():
    mode = os.environ.get("WOS_AHCI_INIT_MODE", "all")
    modes = {"all": "", "captured-wrap": "#define AHCI_INIT_WRAP_CONTROL 1\n",
             "old-safe-high": "#define AHCI_INIT_OLD_SAFE_CONTROL 1\n"}
    if mode not in modes:
        raise ValueError("unknown controller-init mode: " + mode)
    flags_source = (ROOT / "modules/kern/src/platform/mm/virt.opt.cpp").read_text()
    text = modes[mode] + PRELUDE + extract(flags_source, "auto kernel_mmio_page_flags(") + MIDDLE
    text += extract(SOURCE_PATH.read_text(), "auto ahci_controller_init(") + TESTS
    with tempfile.TemporaryDirectory(prefix="ahci-controller-init-") as directory:
        temp = Path(directory)
        shim = temp / "platform/dbg"
        shim.mkdir(parents=True)
        (shim / "dbg.hpp").write_text("#pragma once\n")
        source = temp / "controller.cpp"
        source.write_text(text)
        binary = temp / "controller"
        command = [os.environ.get("HOST_CXX", "/usr/bin/clang++"), "--target=x86_64-linux-gnu", "--gcc-toolchain=/usr",
                   "-std=c++23", "-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer",
                   "-I" + str(temp), "-I" + str(ROOT / "modules/kern/src"), "-I" + str(ROOT / "modules"),
                   str(source), "-o", str(binary)]
        artifacts = os.environ.get("WOS_AHCI_INIT_ARTIFACT_DIR")
        if artifacts:
            artifact_path = Path(artifacts)
            artifact_path.mkdir(parents=True, exist_ok=True)
            (artifact_path / "controller.cpp").write_text(text)
            (artifact_path / "compiler-argv.txt").write_text("\n".join(command) + "\n")
        subprocess.run(command, check=True)
        if artifacts:
            shutil.copyfile(binary, artifact_path / "controller.bin")
        subprocess.run([str(binary)], check=True)
    print("Actual-source AHCI controller-init integration: PASS (ASan/UBSan)")


if __name__ == "__main__":
    main()
