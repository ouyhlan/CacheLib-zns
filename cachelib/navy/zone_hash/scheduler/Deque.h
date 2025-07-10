// forked from https://github.com/jbaldwin/libcoro
// change RingBuffer into Deque with FIFO policy, so we just rewrite the code
#pragma once

#include <atomic>
#include <coroutine>
#include <deque>
#include <mutex>
#include <vector>

namespace facebook {
namespace cachelib {
namespace navy {

template <typename element>
class Deque {
 public:
  Deque(std::vector<element>& items) {
    for (auto&& item : items) {
      m_deque.emplace_back(std::move(item));
    }
  }

  Deque() = default;
  ~Deque() = default;

  Deque(const Deque<element>&) = delete;
  Deque(Deque<element>&&) = delete;

  auto operator=(const Deque<element>&) noexcept -> Deque<element>& = delete;
  auto operator=(Deque<element>&&) noexcept -> Deque<element>& = delete;

  struct consume_operation {
    explicit consume_operation(Deque<element>& rb) : m_rb(rb) {}

    auto await_ready() noexcept -> bool {
      std::unique_lock lk{m_rb.m_mutex};
      return m_rb.try_consume_locked(this);
    }

    auto await_suspend(std::coroutine_handle<> awaiting_coroutine) noexcept
        -> bool {
      std::unique_lock lk{m_rb.m_mutex};
      // We have to check again as there is a race condition between
      // await_ready() and now on the mutex acquire. It is possible that a
      // producer added items between await_ready() and await_suspend().
      if (m_rb.try_consume_locked(this)) {
        return false;
      }

      m_awaiting_coroutine = awaiting_coroutine;
      m_next = m_rb.m_consume_waiters;
      m_rb.m_consume_waiters = this;
      return true;
    }

    /**
     * @return The consumed element or std::nullopt if the consume has failed.
     */
    auto await_resume() -> element { return std::move(m_e); }

   private:
    template <typename element_subtype>
    friend class Deque;

    /// The ring buffer to consume an element from.
    Deque<element>& m_rb;
    /// If the operation needs to suspend, the coroutine to resume when the
    /// element can be consumed.
    std::coroutine_handle<> m_awaiting_coroutine;
    /// Linked list of consume operations that are awaiting to consume an
    /// element.
    consume_operation* m_next{nullptr};
    /// The element this consume operation will consume.
    element m_e;
  };

  /**
   * Produces the given element into the ring buffer.  This operation will
   * suspend until a slot in the ring buffer becomes available.
   * @param e The element to produce.
   */
  void produce(element e) {
    std::unique_lock lk{m_mutex};
    try_produce_locked(lk, e);
  }

  /**
   * Consumes an element from the ring buffer.  This operation will suspend
   * until an element in the ring buffer becomes available.
   */
  [[nodiscard]] auto consume() -> consume_operation {
    return consume_operation{*this};
  }

  /**
   * @return The current number of elements contained in the ring buffer.
   */
  auto size() const -> size_t {
    std::atomic_thread_fence(std::memory_order::acquire);
    return m_used;
  }

  /**
   * @return True if the ring buffer contains zero elements.
   */
  auto empty() const -> bool { return size() == 0; }

 private:
  friend consume_operation;

  std::mutex m_mutex{};

  std::deque<element> m_deque;
  /// The current front pointer to an open slot if not full.
  size_t m_front{0};
  /// The current back pointer to the oldest item in the buffer if not empty.
  size_t m_back{0};
  /// The number of items in the ring buffer.
  size_t m_used{0};

  /// The LIFO list of consume watier.
  consume_operation* m_consume_waiters{nullptr};

  void try_produce_locked(std::unique_lock<std::mutex>& lk, element& e) {
    if (m_consume_waiters != nullptr) {
      // FIFO policy
      consume_operation* to_resume;
      if (m_consume_waiters->m_next == nullptr) {
        to_resume = m_consume_waiters;
        m_consume_waiters = nullptr;
      } else {
        consume_operation* prev = m_consume_waiters;
        consume_operation* curr = m_consume_waiters->m_next;

        while (curr->m_next != nullptr) {
          prev = curr;
          curr = curr->m_next;
        }

        to_resume = curr;
        prev->m_next = nullptr;
      }

      // Since the consume operation suspended it needs to be provided an
      // element to consume.
      to_resume->m_e = std::move(e);

      lk.unlock();
      to_resume->m_awaiting_coroutine.resume();
    } else {
      m_deque.emplace_back(std::move(e));
      ++m_used;
    }
  }

  auto try_consume_locked(consume_operation* op) -> bool {
    if (m_used == 0) {
      return false;
    }

    op->m_e = std::move(m_deque.front());
    m_deque.pop_front();
    --m_used;

    return true;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook