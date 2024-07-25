#include "gates.hpp"

namespace ker::mod::gates {
    interruptHandler_t interruptHandlers[256] = {nullptr};
    
    void exception_handler (interruptFrame *frame) {
        ker::mod::io::serial::write("PANIC!\n");
        //print frame info
        ker::mod::io::serial::write("Interrupt number: ");
        ker::mod::io::serial::write(frame->intNum);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Error code: ");
        ker::mod::io::serial::write(frame->errCode);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("RIP: ");
        ker::mod::io::serial::write(frame->rip);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("CS: ");
        ker::mod::io::serial::write(frame->cs);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("RFLAGS: ");
        ker::mod::io::serial::write(frame->flags);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("RSP: ");
        ker::mod::io::serial::write(frame->rsp);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("SS: ");
        ker::mod::io::serial::write(frame->ss);
        ker::mod::io::serial::write("\n");
        ker::mod::io::serial::write("Halting\n");
        ker::mod::apic::eoi();
    }

    extern "C" void iterrupt_handler (interruptFrame *frame) {
        if(frame->errCode != 0) {
            exception_handler(frame);
            return;
        }
        if(interruptHandlers[frame->intNum] != nullptr) {
            interruptHandlers[frame->intNum](frame);
        }
        else {
            if(!isIrq(frame->intNum)) {
                ker::mod::io::serial::write("No handler for interrupt");
                ker::mod::io::serial::write(frame->intNum);
                ker::mod::io::serial::write("\n");
            }
        }
        ker::mod::apic::eoi();
    }

    void setInterruptHandler(uint8_t intNum, interruptHandler_t handler) {
        if(interruptHandlers[intNum] != nullptr) {
            ker::mod::io::serial::write("Handler already set\n");
            return;
        }
        interruptHandlers[intNum] = handler;
    }

    void removeInterruptHandler(uint8_t intNum) {
        interruptHandlers[intNum] = nullptr;
    }

    bool isInterruptHandlerSet(uint8_t intNum) {
        return interruptHandlers[intNum] != nullptr;
    }

    extern "C" void task_switch_handler(interruptFrame *frame) {
        (void) frame;
        ker::mod::io::serial::write("Task switch occurred\n");
        ker::mod::apic::eoi();
    }

}