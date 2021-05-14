// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <condition_variable>
#include <mutex>
#include <utility>
#include <vector>

#include "caf/actor.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/flow/async/notifiable.hpp"
#include "caf/flow/async/publisher.hpp"
#include "caf/flow/publisher.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow::async {

/// A queue that feeds asynchronously into a publisher until it is closed.
template <class T>
class publishing_queue : public ref_counted {
public:
  struct queue {
    std::mutex mtx;
    size_t capacity;
    std::vector<T> buf;
    std::condition_variable cv;

    explicit queue(size_t capacity) : capacity(capacity) {
      buf.reserve(capacity);
    }

    template <class Value>
    std::pair<bool, bool> try_push(Value&& value) {
      std::unique_lock guard{mtx};
      if (buf.empty()) {
        buf.emplace_back(std::forward<Value>(value));
        return {true, true};
      } else if (buf.size() < capacity) {
        buf.emplace_back(std::forward<Value>(value));
        return {true, false};
      } else {
        return {false, false};
      }
    }

    template <class Value>
    bool push(Value&& value) {
      std::unique_lock guard{mtx};
      for (;;) {
        if (buf.empty()) {
          buf.emplace_back(std::forward<Value>(value));
          return true;
        } else if (buf.size() < capacity) {
          buf.emplace_back(std::forward<Value>(value));
          return false;
        }
        cv.wait(guard);
      }
    }
  };

  using queue_ptr = std::shared_ptr<queue>;

  publishing_queue(queue_ptr queue, caf::actor worker,
                   notifiable_ptr notify_hdl)
    : queue_(std::move(queue)),
      worker_(std::move(worker)),
      notify_hdl_(std::move(notify_hdl)) {
    // nop
  }

  ~publishing_queue() {
    if (worker_)
      anon_send(worker_, detail::unsafe_flow_msg{close_atom_v, notify_hdl_});
  }

  /// Tries to push `value` into the queue without blocking.
  /// @returns `true` on success, `false` if the queue is full.
  bool try_push(T value) {
    auto [added, do_notify] = queue_->try_push(std::move(value));
    if (do_notify)
      anon_send(worker_, detail::unsafe_flow_msg{notify_hdl_});
    return added;
  }

  /// Pushes `value` into the queue. Blocks the caller if the queue is full
  /// until a slot becomes available.
  void push(T value) {
    auto do_notify = queue_->push(std::move(value));
    if (do_notify)
      anon_send(worker_, detail::unsafe_flow_msg{notify_hdl_});
  }

private:
  queue_ptr queue_;
  caf::actor worker_;
  notifiable_ptr notify_hdl_;
};

/// @relates publishing_queue
template <class T>
using publishing_queue_ptr = intrusive_ptr<publishing_queue<T>>;

/// The publisher where the @ref publishing_queue feeds into.
/// @relates publishing_queue
template <class T>
class publishing_queue_backend : public flow::buffered_publisher<T>,
                                 public notifiable {
public:
  using super = flow::buffered_publisher<T>;

  using queue_ptr = typename publishing_queue<T>::queue_ptr;

  explicit publishing_queue_backend(coordinator* ctx, queue_ptr queue)
    : super(ctx, defaults::flow::batch_size), queue_(std::move(queue)) {
    // nop
  }

  void on_notify() override {
    this->try_push();
  }

  void on_close() override {
    this->try_push();
    this->shutdown();
  }

  void on_abort(const error& reason) override {
    this->abort(reason);
  }

  void on_request(subscriber_base* sink, size_t n) override {
    super::on_request(sink, n);
  }

  bool done() const noexcept override {
    if (super::done()) {
      std::unique_lock guard{queue_->mtx};
      return queue_->buf.empty();
    } else {
      return false;
    }
  }

protected:
  void pull(size_t n) override {
    CAF_ASSERT(n > 0);
    std::unique_lock guard{queue_->mtx};
    auto& src_buf = queue_->buf;
    if (auto m = std::min(n, src_buf.size()); m > 0) {
      this->append_to_buf(src_buf.begin(), src_buf.begin() + m);
      src_buf.erase(src_buf.begin(), src_buf.begin() + m);
      queue_->cv.notify_all();
    }
  }

private:
  queue_ptr queue_;
};

/// Creates a new @ref publishing_queue as well a @ref flow::publisher that
/// reads items from the queue and makes them available to subscribers. The
/// publisher runs transparently on a worker actor in the background. The
/// producer that pushes to the @ref publishing_queue as well as any number of
/// @ref flow::subscriber instances runs asynchronously to the worker actor.
/// @returns A pointer to the new @ref publishing_queue as well as a
///          @ref flow::async::publisher that wraps the created
///          @ref flow::publisher.
/// @relates publishing_queue
template <class T>
auto make_publishing_queue(actor_system& sys, size_t capacity) {
  using impl = publishing_queue<T>;
  using worker_impl = event_based_actor;
  auto [self, launch] = sys.template make_flow_coordinator<worker_impl>();
  notifiable_ptr notify_hdl;
  auto queue = std::make_shared<typename impl::queue>(capacity);
  auto init = [self{self}, &notify_hdl, queue]() mutable {
    using backend_impl = publishing_queue_backend<T>;
    auto backend = make_counted<backend_impl>(self, std::move(queue));
    notify_hdl = backend;
    return backend->as_publisher();
  };
  auto pub = self->lift(init());
  launch();
  return std::make_pair(make_counted<impl>(std::move(queue), actor{self},
                                           std::move(notify_hdl)),
                        std::move(pub));
}

} // namespace caf::flow::async
