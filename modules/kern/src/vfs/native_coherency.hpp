#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <platform/sys/spinlock.hpp>
#include <vfs/coherency.hpp>

namespace ker::vfs::coherency {

class Mutation;

// Native data and namespace may have different sleeping serialization locks.
// This lock protects only publication state, never the backend operation.
// Existing inode references must keep the object alive through every guard.
class Object {
   public:
    explicit Object(Counter& counter = owner_counter) : counter_(counter) {
        uint64_t const ID = counter_.take();
        stamp_ = {ID, ID, ID, ID};
    }
    Object(const Object&) = delete;
    Object(Object&&) = delete;
    auto operator=(const Object&) -> Object& = delete;
    auto operator=(Object&&) -> Object& = delete;

    auto snapshot(Stamp& output) const -> bool {
        lock_.lock();
        bool const VALID = !closed_ && active_ == 0 && valid_stamp(stamp_);
        if (VALID) output = stamp_;
        lock_.unlock();
        return VALID;
    }

    auto close_admission() -> bool {
        lock_.lock();
        closed_ = true;
        bool const DRAINED = active_ == 0;
        lock_.unlock();
        return DRAINED;
    }

    auto quiescent() const -> bool {
        lock_.lock();
        bool const DRAINED = active_ == 0;
        lock_.unlock();
        return DRAINED;
    }

   private:
    friend class Mutation;
    static constexpr uint32_t MAX_ACTIVE = 1024;

    auto begin(Domain domains, Ticket& ticket) -> int {
        if (!valid_domain(domains)) return -EINVAL;
        // Reserve outside the short lock, before any backend effect.
        uint64_t const SERIAL = counter_.take();
        if (SERIAL == 0) return -EOVERFLOW;
        lock_.lock();
        int result = 0;
        if (closed_)
            result = -ESTALE;
        else if (!valid_stamp(stamp_))
            result = -EOVERFLOW;
        else if (active_ == MAX_ACTIVE)
            result = -EAGAIN;
        else {
            ++active_;
            ticket = {SERIAL, mutation_domains(domains)};
        }
        lock_.unlock();
        return result;
    }

    void finish(const Ticket& ticket, bool publish) {
        lock_.lock();
        if (publish) {
            if (includes(ticket.domains, Domain::CONTENT)) stamp_.content = std::max(stamp_.content, ticket.serial);
            if (includes(ticket.domains, Domain::METADATA)) stamp_.metadata = std::max(stamp_.metadata, ticket.serial);
            if (includes(ticket.domains, Domain::DIRECTORY)) stamp_.directory = std::max(stamp_.directory, ticket.serial);
        }
        --active_;
        lock_.unlock();
    }

    Counter& counter_;
    mutable ker::mod::sys::Spinlock lock_;
    Stamp stamp_{};
    uint32_t active_ = 0;
    bool closed_ = false;
};

// A guard publishes conservatively on every exit, including an error after
// partial effects. Only cancel_no_effect() preserves the committed stamp.
// Destroy/finish the guard before releasing backend serialization or inode refs.
class Mutation {
   public:
    Mutation(Object& object, Domain domains) : status_(object.begin(domains, ticket_)) {
        if (status_ == 0) object_ = &object;
    }
    Mutation(const Mutation&) = delete;
    Mutation(Mutation&&) = delete;
    auto operator=(const Mutation&) -> Mutation& = delete;
    auto operator=(Mutation&&) -> Mutation& = delete;
    ~Mutation() { publish(); }

    auto status() const -> int { return status_; }
    explicit operator bool() const { return status_ == 0; }

    void publish() { finish(true); }
    void cancel_no_effect() { finish(false); }

   private:
    void finish(bool publish) {
        if (object_ == nullptr) return;
        object_->finish(ticket_, publish);
        object_ = nullptr;
    }

    Object* object_ = nullptr;
    Ticket ticket_{};
    int status_;
};

// Native operations retain their existing availability when version authority
// is exhausted or unavailable. Disable validation permanently before effects
// in that case: callers must use fresh backend RPC and cannot serve cached data.
class Publication {
   public:
    Publication(Object& object, Domain domains) : mutation_(object, domains) {
        if (!mutation_) static_cast<void>(object.close_admission());
    }
    Publication(const Publication&) = delete;
    Publication(Publication&&) = delete;
    auto operator=(const Publication&) -> Publication& = delete;
    auto operator=(Publication&&) -> Publication& = delete;

    void publish() { mutation_.publish(); }
    void cancel_no_effect() { mutation_.cancel_no_effect(); }

   private:
    Mutation mutation_;
};

}  // namespace ker::vfs::coherency
