"""Actual empty Object owners for maintained pre-device host fixtures.

The old vectors never provide an Object. Any nonempty retain/release or device
export aborts explicitly; this boundary cannot qualify the new device path.
"""

import hashlib
from pathlib import Path
import runpy


def empty_object_owners(kernel_path):
    helper = Path(__file__).with_name("file_mmap_registry_runtime_test.py")
    capture = runpy.run_path(str(helper))["capture"]
    path = kernel_path("dev/device_mapping_object.cpp")
    source = path.read_text()
    rows = [{"name": "nondevice-object-source", "path": str(path),
             "bytes": len(source.encode()), "sha256": hashlib.sha256(source.encode()).hexdigest()}]
    bodies = []
    for anchor in (
        "ObjectRef::~ObjectRef()", "ObjectRef::ObjectRef(ObjectRef&&",
        "auto ObjectRef::operator=", "auto ObjectRef::clone_into(", "void ObjectRef::reset(",
        "ObjectExportRef::~ObjectExportRef()", "ObjectExportRef::ObjectExportRef(ObjectExportRef&&",
        "auto ObjectExportRef::operator=", "auto ObjectExportRef::clone_into(", "void ObjectExportRef::reset(",
    ):
        body, row = capture(source, anchor)
        rows.append(row)
        bodies.append(body)
    return ('#include <dev/device_mapping_object.hpp>\nnamespace ker::dev::mapping {\n'
            + "\n".join(bodies) + '''
// Explicitly unreachable device endpoints in old anonymous/regular vectors.
auto Object::retain(ObjectRole,bool) noexcept -> bool { std::abort(); }
void Object::release(ObjectRole,bool) noexcept { std::abort(); }
auto Object::export_page(uint64_t,uint64_t,ObjectExportRef&) noexcept -> bool { std::abort(); }
}
''', rows)


def nondevice_family():
    return '''namespace ker::mod::mm::file_mapping {
// Only old anonymous/regular backing exists in this fixture. A device Object
// cannot be supplied through this explicit boundary.
struct NonDeviceFamily {
 auto is_device_backed() const noexcept -> bool { return false; }
 auto device_object() const noexcept -> ker::dev::mapping::Object* { std::abort(); }
 auto device_generation() const noexcept -> uint64_t { std::abort(); }
 auto device_window_base() const noexcept -> uint64_t { std::abort(); }
 auto device_window_end() const noexcept -> uint64_t { std::abort(); }
};
inline NonDeviceFamily nondevice_family;
}
'''
