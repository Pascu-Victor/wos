#pragma once

namespace std {
class exception {
   public:
    exception() noexcept = default;
    exception(const exception&) noexcept = default;
    exception& operator=(const exception&) noexcept = default;
    virtual ~exception() noexcept = default;

    virtual const char* what() const noexcept { return "std::exception"; }
};
class bad_optional_access : public exception {
   public:
    const char* what() const noexcept override { return "bad optional access"; }
};

class bad_function_call : public exception {
   public:
    const char* what() const noexcept override { return "bad function call"; }
};

class bad_alloc : public exception {
   public:
    const char* what() const noexcept override { return "bad alloc"; }
};

class bad_array_new_length : public exception {
   public:
    const char* what() const noexcept override { return "bad array new length"; }
};

class logic_error : public exception {
   public:
    const char* what() const noexcept override { return "logic error"; }
};
}  // namespace std
