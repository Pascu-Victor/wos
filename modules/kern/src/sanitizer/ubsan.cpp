// WOS Kernel UBSan Runtime
//
// Provides the __ubsan_handle_* callbacks that Clang's -fsanitize=undefined
// emits calls to.  Each handler logs the source location and description,
// then panics (or continues, depending on WOS_UBSAN_CONTINUE).
//
// Gated behind cmake -DWOS_KUBSAN=ON.  When WOS_KUBSAN is not defined this
// file is still compiled but contributes nothing (all symbols are extern "C"
// weak, so the linker drops them if unreferenced).

#ifdef WOS_KUBSAN

#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>

extern "C" {

// --------------------------------------------------------------------------
// UBSan source location descriptor (matches Clang's layout)
// --------------------------------------------------------------------------

struct UbsanSourceLocation {
    const char* file;
    uint32_t line;
    uint32_t column;
};

// Clang's UBSan TypeDescriptor — emitted for each type referenced by checks.
// Layout: uint16_t TypeKind, uint16_t TypeInfo, char TypeName[].
// TypeKind: 0x0000 = integer, 0x0001 = float, 0xFFFF = unknown
// For integers: TypeInfo bit 0 = signed, bits 1..15 = log2(bit_width)
struct UbsanTypeDescriptor {
    uint16_t type_kind;
    uint16_t type_info;
    char type_name[];  // Flexible array — null-terminated
};

struct UbsanTypeMismatchData {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* type;
    uint8_t log_alignment;
    uint8_t type_check_kind;
};

struct UbsanTypeMismatchDataV1 {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* type;
    uint8_t log_alignment;
    uint8_t type_check_kind;
};

struct UbsanOverflowData {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* type;
};

struct UbsanShiftData {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* lhs_type;
    const UbsanTypeDescriptor* rhs_type;
};

struct UbsanOutOfBoundsData {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* array_type;
    const UbsanTypeDescriptor* index_type;
};

struct UbsanUnreachableData {
    UbsanSourceLocation loc;
};

struct UbsanInvalidValueData {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* type;
};

struct UbsanAlignmentAssumptionData {
    UbsanSourceLocation loc;
    UbsanSourceLocation assumption_loc;
    const UbsanTypeDescriptor* type;
};

struct UbsanNonnullArgData {
    UbsanSourceLocation loc;
    UbsanSourceLocation attr_loc;
    int arg_index;
};

struct UbsanPointerOverflowData {
    UbsanSourceLocation loc;
};

struct UbsanImplicitConversionData {
    UbsanSourceLocation loc;
    const UbsanTypeDescriptor* from_type;
    const UbsanTypeDescriptor* to_type;
    uint8_t kind;
};

// --------------------------------------------------------------------------
// Internal helpers — direct serial writes (lock-free during panic)
// --------------------------------------------------------------------------

namespace {

namespace ser = ker::mod::io::serial;

void ubsan_write_hex(uint64_t val) {
    char buf[19];  // "0x" + 16 hex digits + NUL
    buf[0] = '0';
    buf[1] = 'x';
    constexpr const char* hex = "0123456789abcdef";
    for (int i = 15; i >= 0; i--) {
        buf[2 + (15 - i)] = hex[(val >> (i * 4)) & 0xf];
    }
    buf[18] = '\0';
    ser::write_unlocked(buf);
}

void ubsan_write_dec(uint64_t val) {
    char buf[21];
    int pos = 20;
    buf[pos] = '\0';
    if (val == 0) {
        buf[--pos] = '0';
    } else {
        while (val > 0) {
            buf[--pos] = static_cast<char>('0' + (val % 10));
            val /= 10;
        }
    }
    ser::write_unlocked(buf + pos);
}

void ubsan_write_type(const UbsanTypeDescriptor* type) {
    if (type == nullptr) {
        ser::write_unlocked("<unknown>");
        return;
    }
    // Print the human-readable name from the flexible array member
    ser::write_unlocked("'");
    ser::write_unlocked(type->type_name);
    ser::write_unlocked("'");
    // Print kind/info details
    ser::write_unlocked(" (kind=");
    if (type->type_kind == 0x0000) {
        ser::write_unlocked("integer");
        bool is_signed = (type->type_info & 1) != 0;
        uint16_t bit_width = 1u << (type->type_info >> 1);
        ser::write_unlocked(is_signed ? ", signed" : ", unsigned");
        ser::write_unlocked(", ");
        ubsan_write_dec(bit_width);
        ser::write_unlocked("-bit");
    } else if (type->type_kind == 0x0001) {
        ser::write_unlocked("float");
    } else {
        ser::write_unlocked("unknown");
    }
    ser::write_unlocked(")");
}

// Hex dump of memory around an address (8 rows of 16 bytes = 128 bytes)
void ubsan_hexdump(uintptr_t addr, size_t size) {
    // Center the dump around the address
    uintptr_t start = (addr - 64) & ~0xFULL;  // align to 16 bytes
    uintptr_t end = start + 128;

    // Bounds check: only dump kernel-space memory
    if (start < 0xffff000000000000ULL) return;

    ser::write_unlocked("\n--- Memory around ");
    ubsan_write_hex(addr);
    ser::write_unlocked(" (");
    ubsan_write_dec(size);
    ser::write_unlocked(" bytes) ---\n");

    constexpr const char* hex = "0123456789abcdef";
    for (uintptr_t row = start; row < end; row += 16) {
        // Indicator arrow
        if (addr >= row && addr < row + 16) {
            ser::write_unlocked("=>");
        } else {
            ser::write_unlocked("  ");
        }
        // Address
        ubsan_write_hex(row);
        ser::write_unlocked(": ");

        // 16 hex bytes with markers
        auto* p = reinterpret_cast<const uint8_t*>(row);
        for (int i = 0; i < 16; i++) {
            // Mark the faulting byte(s)
            bool is_target = ((row + i) >= addr && (row + i) < addr + size);
            if (is_target) ser::write_unlocked("[");

            char h[3];
            h[0] = hex[p[i] >> 4];
            h[1] = hex[p[i] & 0xf];
            h[2] = '\0';
            ser::write_unlocked(h);

            if (is_target) {
                ser::write_unlocked("]");
            } else {
                ser::write_unlocked(" ");
            }
            if (i == 7) ser::write_unlocked(" ");
        }

        // ASCII
        ser::write_unlocked(" |");
        for (int i = 0; i < 16; i++) {
            char c = (p[i] >= 0x20 && p[i] < 0x7f) ? static_cast<char>(p[i]) : '.';
            ser::write_unlocked(c);
        }
        ser::write_unlocked("|\n");
    }
}

}  // namespace

static void ubsan_log(const char* kind, const UbsanSourceLocation* loc) {
    ker::mod::dbg::log("[UBSAN] %s at %s:%u:%u", kind, loc->file ? loc->file : "?", loc->line, loc->column);
}

// Enhanced abort: prints detailed info before calling panic_handler
[[noreturn]] static void ubsan_abort(const char* kind, const UbsanSourceLocation* loc) {
    ubsan_log(kind, loc);
    ker::mod::dbg::panic_handler("UBSan violation (see above)");
    __builtin_unreachable();
}

// Abort with value and type context (for handlers that have them)
[[noreturn]] static void ubsan_abort_with_val(const char* kind, const UbsanSourceLocation* loc, const UbsanTypeDescriptor* type,
                                              uintptr_t val) {
    // Use direct serial writes for panic-safe output
    ser::enter_panic_mode();
    if (!ser::is_panic_owner()) {
        hcf();
    }
    ser::write_unlocked("\n[UBSAN] ");
    ser::write_unlocked(kind);
    ser::write_unlocked(" at ");
    ser::write_unlocked(loc->file ? loc->file : "?");
    ser::write_unlocked(":");
    ubsan_write_dec(loc->line);
    ser::write_unlocked(":");
    ubsan_write_dec(loc->column);
    ser::write_unlocked("\n");

    ser::write_unlocked("  Value: ");
    ubsan_write_hex(val);
    ser::write_unlocked(" (");
    ubsan_write_dec(val);
    ser::write_unlocked(")\n");

    if (type != nullptr) {
        ser::write_unlocked("  Type:  ");
        ubsan_write_type(type);
        ser::write_unlocked("\n");
    }

    // If the value looks like a pointer, dump memory around it
    if (val >= 0xffff000000000000ULL) {
        ubsan_hexdump(val, 8);
    }

    // Caller RIP
    ser::write_unlocked("  Caller: ");
    ubsan_write_hex(reinterpret_cast<uintptr_t>(__builtin_return_address(1)));
    ser::write_unlocked("\n");

    ker::mod::dbg::panic_handler("UBSan violation (see above)");
    __builtin_unreachable();
}

// Abort with two values (for overflow/shift/conversion handlers)
[[noreturn]] static void ubsan_abort_with_vals(const char* kind, const UbsanSourceLocation* loc, const UbsanTypeDescriptor* type,
                                               uintptr_t lhs, uintptr_t rhs) {
    ser::enter_panic_mode();
    if (!ser::is_panic_owner()) {
        hcf();
    }
    ser::write_unlocked("\n[UBSAN] ");
    ser::write_unlocked(kind);
    ser::write_unlocked(" at ");
    ser::write_unlocked(loc->file ? loc->file : "?");
    ser::write_unlocked(":");
    ubsan_write_dec(loc->line);
    ser::write_unlocked(":");
    ubsan_write_dec(loc->column);
    ser::write_unlocked("\n");

    ser::write_unlocked("  LHS: ");
    ubsan_write_hex(lhs);
    ser::write_unlocked("  RHS: ");
    ubsan_write_hex(rhs);
    ser::write_unlocked("\n");

    if (type != nullptr) {
        ser::write_unlocked("  Type: ");
        ubsan_write_type(type);
        ser::write_unlocked("\n");
    }

    ker::mod::dbg::panic_handler("UBSan violation (see above)");
    __builtin_unreachable();
}

// --------------------------------------------------------------------------
// Handlers - called by compiler-generated instrumentation
// --------------------------------------------------------------------------

void __ubsan_handle_type_mismatch(UbsanTypeMismatchData* data, uintptr_t ptr) {
    ubsan_abort_with_val("type-mismatch", &data->loc, data->type, ptr);
}

void __ubsan_handle_type_mismatch_v1(UbsanTypeMismatchDataV1* data, uintptr_t ptr) {
    if (ptr == 0) {
        ubsan_abort_with_val("null-pointer-access", &data->loc, data->type, ptr);
    }
    if (data->log_alignment != 0 && (ptr & ((1UL << data->log_alignment) - 1)) != 0) {
        ser::enter_panic_mode();
        if (!ser::is_panic_owner()) {
            hcf();
        }
        ser::write_unlocked("\n[UBSAN] misaligned-access at ");
        ser::write_unlocked(data->loc.file ? data->loc.file : "?");
        ser::write_unlocked(":");
        ubsan_write_dec(data->loc.line);
        ser::write_unlocked(":");
        ubsan_write_dec(data->loc.column);
        ser::write_unlocked("\n  Pointer: ");
        ubsan_write_hex(ptr);
        ser::write_unlocked("\n  Required alignment: ");
        ubsan_write_dec(1UL << data->log_alignment);
        ser::write_unlocked("\n  Type: ");
        ubsan_write_type(data->type);
        ser::write_unlocked("\n");
        if (ptr >= 0xffff000000000000ULL) {
            ubsan_hexdump(ptr, 8);
        }
        ker::mod::dbg::panic_handler("UBSan violation (see above)");
        __builtin_unreachable();
    }
}

void __ubsan_handle_add_overflow(UbsanOverflowData* data, uintptr_t lhs, uintptr_t rhs) {
    ubsan_abort_with_vals("add-overflow", &data->loc, data->type, lhs, rhs);
}

void __ubsan_handle_sub_overflow(UbsanOverflowData* data, uintptr_t lhs, uintptr_t rhs) {
    ubsan_abort_with_vals("sub-overflow", &data->loc, data->type, lhs, rhs);
}

void __ubsan_handle_mul_overflow(UbsanOverflowData* data, uintptr_t lhs, uintptr_t rhs) {
    ubsan_abort_with_vals("mul-overflow", &data->loc, data->type, lhs, rhs);
}

void __ubsan_handle_negate_overflow(UbsanOverflowData* data, uintptr_t val) {
    ubsan_abort_with_val("negate-overflow", &data->loc, data->type, val);
}

void __ubsan_handle_divrem_overflow(UbsanOverflowData* data, uintptr_t lhs, uintptr_t rhs) {
    ubsan_abort_with_vals("divrem-overflow", &data->loc, data->type, lhs, rhs);
}

void __ubsan_handle_shift_out_of_bounds(UbsanShiftData* data, uintptr_t lhs, uintptr_t rhs) {
    ubsan_abort_with_vals("shift-out-of-bounds", &data->loc, data->lhs_type, lhs, rhs);
}

void __ubsan_handle_out_of_bounds(UbsanOutOfBoundsData* data, uintptr_t index) {
    ubsan_abort_with_val("out-of-bounds", &data->loc, data->index_type, index);
}

void __ubsan_handle_builtin_unreachable(UbsanUnreachableData* data) { ubsan_abort("builtin-unreachable", &data->loc); }

void __ubsan_handle_missing_return(UbsanUnreachableData* data) { ubsan_abort("missing-return", &data->loc); }

void __ubsan_handle_load_invalid_value(UbsanInvalidValueData* data, uintptr_t val) {
    ubsan_abort_with_val("load-invalid-value", &data->loc, data->type, val);
}

void __ubsan_handle_alignment_assumption(UbsanAlignmentAssumptionData* data, uintptr_t ptr, uintptr_t align, uintptr_t offset) {
    (void)align;
    (void)offset;
    ubsan_abort_with_val("alignment-assumption", &data->loc, data->type, ptr);
}

void __ubsan_handle_nonnull_arg(UbsanNonnullArgData* data) { ubsan_abort("nonnull-arg", &data->loc); }

void __ubsan_handle_pointer_overflow(UbsanPointerOverflowData* data, uintptr_t base, uintptr_t result) {
    ubsan_abort_with_vals("pointer-overflow", &data->loc, nullptr, base, result);
}

void __ubsan_handle_implicit_conversion(UbsanImplicitConversionData* data, uintptr_t src, uintptr_t dst) {
    ser::enter_panic_mode();
    if (!ser::is_panic_owner()) {
        hcf();
    }
    ser::write_unlocked("\n[UBSAN] implicit-conversion at ");
    ser::write_unlocked(data->loc.file ? data->loc.file : "?");
    ser::write_unlocked(":");
    ubsan_write_dec(data->loc.line);
    ser::write_unlocked(":");
    ubsan_write_dec(data->loc.column);
    ser::write_unlocked("\n  From: ");
    ubsan_write_type(data->from_type);
    ser::write_unlocked("  value=");
    ubsan_write_hex(src);
    ser::write_unlocked("\n  To:   ");
    ubsan_write_type(data->to_type);
    ser::write_unlocked("  value=");
    ubsan_write_hex(dst);
    ser::write_unlocked("\n");
    ker::mod::dbg::panic_handler("UBSan violation (see above)");
    __builtin_unreachable();
}

// ---- _abort variants -------------------------------------------------------
// Clang emits these when -fno-sanitize-recover is NOT set for a specific check
// but the handler is called from a context where it might not abort on its own.
// In the kernel we always panic, so these are identical to the base handlers.

void __ubsan_handle_type_mismatch_v1_abort(UbsanTypeMismatchDataV1* data, uintptr_t ptr) { __ubsan_handle_type_mismatch_v1(data, ptr); }
void __ubsan_handle_add_overflow_abort(UbsanOverflowData* data, uintptr_t a, uintptr_t b) { __ubsan_handle_add_overflow(data, a, b); }
void __ubsan_handle_sub_overflow_abort(UbsanOverflowData* data, uintptr_t a, uintptr_t b) { __ubsan_handle_sub_overflow(data, a, b); }
void __ubsan_handle_mul_overflow_abort(UbsanOverflowData* data, uintptr_t a, uintptr_t b) { __ubsan_handle_mul_overflow(data, a, b); }
void __ubsan_handle_negate_overflow_abort(UbsanOverflowData* data, uintptr_t a) { __ubsan_handle_negate_overflow(data, a); }
void __ubsan_handle_divrem_overflow_abort(UbsanOverflowData* data, uintptr_t a, uintptr_t b) { __ubsan_handle_divrem_overflow(data, a, b); }
void __ubsan_handle_shift_out_of_bounds_abort(UbsanShiftData* data, uintptr_t a, uintptr_t b) {
    __ubsan_handle_shift_out_of_bounds(data, a, b);
}
void __ubsan_handle_out_of_bounds_abort(UbsanOutOfBoundsData* data, uintptr_t a) { __ubsan_handle_out_of_bounds(data, a); }
void __ubsan_handle_load_invalid_value_abort(UbsanInvalidValueData* data, uintptr_t a) { __ubsan_handle_load_invalid_value(data, a); }
void __ubsan_handle_alignment_assumption_abort(UbsanAlignmentAssumptionData* data, uintptr_t a, uintptr_t b, uintptr_t c) {
    __ubsan_handle_alignment_assumption(data, a, b, c);
}
void __ubsan_handle_nonnull_arg_abort(UbsanNonnullArgData* data) { __ubsan_handle_nonnull_arg(data); }
void __ubsan_handle_pointer_overflow_abort(UbsanPointerOverflowData* data, uintptr_t a, uintptr_t b) {
    __ubsan_handle_pointer_overflow(data, a, b);
}
void __ubsan_handle_implicit_conversion_abort(UbsanImplicitConversionData* data, uintptr_t a, uintptr_t b) {
    __ubsan_handle_implicit_conversion(data, a, b);
}

struct UbsanInvalidBuiltinData {
    UbsanSourceLocation loc;
    uint8_t kind;
};

struct UbsanNonnullReturnData {
    UbsanSourceLocation loc;
};

void __ubsan_handle_invalid_builtin(UbsanInvalidBuiltinData* data) { ubsan_abort("invalid-builtin", &data->loc); }
void __ubsan_handle_invalid_builtin_abort(UbsanInvalidBuiltinData* data) { __ubsan_handle_invalid_builtin(data); }

void __ubsan_handle_nonnull_return_v1(UbsanNonnullReturnData* data) { ubsan_abort("nonnull-return", &data->loc); }
void __ubsan_handle_nonnull_return_v1_abort(UbsanNonnullReturnData* data) { __ubsan_handle_nonnull_return_v1(data); }

}  // extern "C"

#endif  // WOS_KUBSAN
