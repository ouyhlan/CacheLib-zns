// forked from https://github.com/jbaldwin/libcoro
// add support for JobScheduler distributed, so we just rewrite the code
#pragma once

#include <coroutine>

#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

class Event {
 public:
  struct awaiter {
    /**
     * @param e The event to wait for it to be set.
     */
    awaiter(const Event& e) noexcept : m_event(e) {}

    /**
     * @return True if the event is already set, otherwise false to suspend this
     * coroutine.
     */
    auto await_ready() const noexcept -> bool { return m_event.is_set(); }

    /**
     * Adds this coroutine to the list of awaiters in a thread safe fashion.  If
     * the event is set while attempting to add this coroutine to the awaiters
     * then this will return false to resume execution immediately.
     * @return False if the event is already set, otherwise true to suspend this
     * coroutine.
     */
    auto await_suspend(std::coroutine_handle<> awaiting_coroutine) noexcept
        -> bool {
      const void* const set_state = &m_event;

      m_awaiting_coroutine = awaiting_coroutine;

      // This value will update if other threads write to it via acquire.
      void* old_value = m_event.m_state.load(std::memory_order::acquire);
      do {
        // Resume immediately if already in the set state.
        if (old_value == set_state) {
          return false;
        }

        m_next = static_cast<awaiter*>(old_value);
      } while (!m_event.m_state.compare_exchange_weak(
          old_value, this, std::memory_order::release,
          std::memory_order::acquire));

      return true;
    }

    /**
     * Nothing to do on resume.
     */
    auto await_resume() noexcept {}

    /// Refernce to the event that this awaiter is waiting on.
    const Event& m_event;
    /// The awaiting continuation coroutine handle.
    std::coroutine_handle<> m_awaiting_coroutine;
    /// The next awaiter in line for this event, nullptr if this is the end.
    awaiter* m_next{nullptr};
  };

  explicit Event(bool initially_set = false) noexcept
      : m_state((initially_set) ? static_cast<void*>(this) : nullptr) {}
  ~Event() = default;

  Event(const Event&) = delete;
  Event(Event&&) = delete;
  auto operator=(const Event&) -> Event& = delete;
  auto operator=(Event&&) -> Event& = delete;

  auto is_set() const noexcept -> bool {
    return m_state.load(std::memory_order_acquire) == this;
  }

  auto set() -> void {
    // Exchange the state to this, if the state was previously not this, then
    // traverse the list of awaiters and resume their coroutines.
    void* old_value = m_state.exchange(this, std::memory_order::acq_rel);
    if (old_value != this) {
      // If FIFO has been requsted then reverse the order upon resuming.

      old_value = reverse(static_cast<awaiter*>(old_value));

      auto* waiters = static_cast<awaiter*>(old_value);
      while (waiters != nullptr) {
        auto* next = waiters->m_next;
        waiters->m_awaiting_coroutine.resume();
        waiters = next;
      }
    }
  }

  auto set(JobScheduler& scheduler, JobType jobtype) -> void {
    void* old_value = m_state.exchange(this, std::memory_order::acq_rel);
    if (old_value != this) {
      // fifo policy:
      old_value = reverse(static_cast<awaiter*>(old_value));

      auto* waiters = static_cast<awaiter*>(old_value);
      while (waiters != nullptr) {
        auto* next = waiters->m_next;

        scheduler.enqueue(
            [coro = waiters->m_awaiting_coroutine]() {
              coro.resume();
              return JobExitCode::Done;
            },
            "set resume", jobtype);

        waiters = next;
      }
    }
  }

  auto operator co_await() const noexcept -> awaiter { return awaiter(*this); }

  auto reset() noexcept -> void {
    void* old_value = this;
    m_state.compare_exchange_strong(old_value, nullptr,
                                    std::memory_order::acquire);
  }

 private:
  friend struct awaiter;
  mutable std::atomic<void*> m_state;

  auto reverse(awaiter* curr) -> awaiter* {
    if (curr == nullptr || curr->m_next == nullptr) {
      return curr;
    }

    awaiter* prev = nullptr;
    awaiter* next = nullptr;
    while (curr != nullptr) {
      next = curr->m_next;
      curr->m_next = prev;
      prev = curr;
      curr = next;
    }

    return prev;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook