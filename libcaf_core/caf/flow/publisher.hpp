// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <cstddef>
#include <numeric>
#include <type_traits>
#include <vector>

#include "caf/defaults.hpp"
#include "caf/detail/core_export.hpp"
#include "caf/flow/disposable.hpp"
#include "caf/flow/fwd.hpp"
#include "caf/flow/publisher_base.hpp"
#include "caf/flow/subscriber.hpp"
#include "caf/flow/subscription.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/make_counted.hpp"
#include "caf/ref_counted.hpp"
#include "caf/sec.hpp"
#include "caf/span.hpp"

namespace caf::flow {

/// An item source that is only visible in the scope of a @ref coordinator. May
/// the lifted to an @ref async::publisher by the coordinator in order to make
/// the items visible to other parts of the system.
///
/// Subscribers must belong to the same @ref coordinator as the publisher.
template <class T>
class publisher : public publisher_base, public disposable {
public:
  using super = publisher_base;

  using published_type = T;

  explicit publisher(coordinator* ctx) : super(ctx) {
    // nop
  }

  virtual void subscribe(subscriber_ptr<T> sink) = 0;

  template <class OnNext>
  std::enable_if_t<std::is_invocable_v<OnNext, T>> subscribe(OnNext fn) {
    subscribe(make_subscriber(std::move(fn)));
  }

  template <class OnNext, class OnError>
  std::enable_if_t<std::is_invocable_v<OnNext, T>>
  subscribe(OnNext fn, OnError fail) {
    subscribe(make_subscriber(std::move(fn), std::move(fail)));
  }

  template <class OnNext, class OnError, class OnComplete>
  std::enable_if_t<std::is_invocable_v<OnNext, T>>
  subscribe(OnNext fn, OnError fail, OnComplete fin) {
    subscribe(make_subscriber(std::move(fn), std::move(fail), std::move(fin)));
  }

  publisher_ptr<T> as_publisher() noexcept {
    return {this};
  }

  publisher_ptr<T> take(size_t n);

  /// Subscribes `stage` to this publisher and returns it as is.
  template <class Impl>
  auto subscribe_with(intrusive_ptr<Impl> what) {
    subscribe(what->as_subscriber());
    return what;
  }

  /// Convenience function for calling
  /// `subscribe_with(make_counted<Impl>(ctor_args...))`.
  template <class Impl, class... Ts>
  auto subscribe_with_new(Ts&&... ctor_args) {
    return subscribe_with(make_counted<Impl>(std::forward<Ts>(ctor_args)...));
  }
};

/// @relates publisher
template <class T>
using publisher_ptr = intrusive_ptr<publisher<T>>;

template <class In, class Out>
class processor : public subscriber<In>, public publisher<Out> {
public:
  using publisher_base = publisher<Out>;

  using publisher_base::publisher_base;
};

// -- broadcasting -------------------------------------------------------------

/// Base type for processors with a buffer that broadcasts output to all
/// subscribers.
template <class T>
class buffered_publisher : public publisher<T> {
public:
  using super = publisher<T>;

  struct output_t {
    size_t demand;
    subscriber_ptr<T> sink;
  };

  explicit buffered_publisher(coordinator* ctx)
    : super(ctx), desired_capacity_(defaults::flow::buffer_size) {
    buf_.reserve(desired_capacity_);
  }

  buffered_publisher(coordinator* ctx, size_t desired_capacity)
    : super(ctx), desired_capacity_(desired_capacity) {
    buf_.reserve(desired_capacity_);
  }

  void on_request(subscriber_base* sink, size_t n) override {
    if (auto i = find(sink); i != outputs_.end()) {
      i->demand += n;
      update_max_demand();
      try_push();
    }
  }

  void on_cancel(subscriber_base* sink) override {
    if (auto i = find(sink); i != outputs_.end()) {
      outputs_.erase(i);
      if (outputs_.empty()) {
        shutdown();
      } else {
        update_max_demand();
        try_push();
      }
    }
  }

  using super::subscribe;

  void subscribe(subscriber_ptr<T> sink) override {
    if (done()) {
      sink->on_complete();
    } else {
      max_demand_ = 0;
      outputs_.emplace_back(output_t{0u, sink});
      super::do_subscribe(sink.get());
    }
  }

  virtual bool done() const noexcept {
    return completed_ && buf_.empty();
  }

  void dispose() override {
    completed_ = true;
    buf_.clear();
    for (auto& out : outputs_)
      out.sink->on_complete();
    outputs_.clear();
  }

  bool disposed() const noexcept override {
    return done() && outputs_.empty();
  }

protected:
  template <class Iterator, class Sentinel>
  void append_to_buf(Iterator first, Sentinel last) {
    buf_.insert(buf_.end(), first, last);
  }

  template <class Val>
  void append_to_buf(Val&& val) {
    buf_.emplace_back(std::forward<Val>(val));
  }

  // Stops the source, but allows subscribers to still consume buffered data.
  virtual void shutdown() {
    completed_ = true;
    if (done()) {
      for (auto& out : outputs_)
        out.sink->on_complete();
      outputs_.clear();
    }
  }

  virtual void abort(const error& reason) {
    completed_ = true;
    for (auto& out : outputs_)
      out.sink->on_error(reason);
    outputs_.clear();
  }

  virtual void pull(size_t) {
    // Customization point.
  }

  /// Tries to push data from the buffer downstream.
  /// @returns The number of successfully pushed items or 0 if the publisher is
  ///          already shutting down.
  size_t try_push() {
    size_t result = 0;
    while (max_demand_ > 0) {
      if (desired_capacity_ > buf_.size())
        pull(desired_capacity_ - buf_.size());
      auto n = std::min(max_demand_, buf_.size());
      if (n == 0)
        return result;
      auto items = span<const T>{buf_.data(), n};
      for (auto& out : outputs_) {
        out.demand -= n;
        out.sink->on_next(items);
      }
      max_demand_ -= n;
      buf_.erase(buf_.begin(), buf_.begin() + n);
      if (done()) {
        for (auto& out : outputs_)
          out.sink->on_complete();
        outputs_.clear();
        return result;
      }
    }
    return result;
  }

  auto find(subscriber_base* sink) {
    auto pred = [sink](auto& out) { return out.sink.get() == sink; };
    return std::find_if(outputs_.begin(), outputs_.end(), pred);
  }

  void update_max_demand() {
    if (outputs_.empty()) {
      max_demand_ = 0;
    } else {
      auto i = outputs_.begin();
      auto e = outputs_.end();
      auto init = (*i++).demand;
      auto f = [](size_t x, auto& out) { return std::min(x, out.demand); };
      max_demand_ = std::accumulate(i, e, init, f);
    }
  }

  size_t desired_capacity_;
  std::vector<T> buf_;
  bool completed_ = false;
  size_t max_demand_ = 0;
  std::vector<output_t> outputs_;
};

/// Base type for processors with a buffer that broadcasts output to all
/// subscribers.
template <class In, class Out>
class buffered_processor : public buffered_publisher<Out>,
                           public subscriber<In> {
public:
  using super = buffered_publisher<Out>;

  explicit buffered_processor(coordinator* ctx)
    : super(ctx, defaults::flow::buffer_size) {
    // nop
  }

  void on_complete() override {
    sub_ = nullptr;
    shutdown();
  }

  void on_error(const error& what) override {
    sub_ = nullptr;
    this->abort(what);
  }

  void on_subscribe(subscription_ptr sub) override {
    if (!sub_) {
      sub_ = std::move(sub);
      in_flight_ = super::desired_capacity_;
      sub_->request(in_flight_);
    } else {
      sub->cancel();
    }
  }

  void dispose() override {
    if (sub_) {
      sub_->cancel();
      sub_ = nullptr;
    }
    super::dispose();
  }

protected:
  void pull(size_t n) override {
    CAF_ASSERT(n > 0);
    if (sub_ && super::desired_capacity_ > in_flight_) {
      auto m = std::min(n, super::desired_capacity_ - in_flight_);
      CAF_ASSERT(m > 0);
      in_flight_ += m;
      sub_->request(m);
    }
  }

  // Stops reading from the source, but allows subscribers to still consume
  // buffered data.
  void shutdown() override {
    super::shutdown();
    if (sub_) {
      sub_->cancel();
      sub_ = nullptr;
    }
  }

  size_t in_flight_ = 0;
  error abort_reason_;
  subscription_ptr sub_;
};

/// Broadcasts its input to all subscribers without modifying it.
template <class T>
class broadcaster : public buffered_processor<T, T> {
public:
  using super = buffered_processor<T, T>;

  using super::super;

  void on_next(span<const T> items) override {
    this->append_to_buf(items.begin(), items.end());
    this->try_push();
  }
};

// -- publisher::take ----------------------------------------------

template <class T>
class limiter : public buffered_processor<T, T> {
public:
  using super = buffered_processor<T, T>;

  limiter(coordinator* ctx, size_t remaining)
    : super(ctx), remaining_(remaining) {
    // nop
  }

  void on_next(span<const T> items) override {
    if (remaining_ > items.size()) {
      super::append_to_buf(items.begin(), items.end());
      remaining_ -= items.size();
      this->try_push();
    } else {
      super::append_to_buf(items.begin(), items.begin() + remaining_);
      remaining_ = 0;
      this->try_push();
      this->shutdown();
    }
  }

private:
  size_t remaining_;
};

template <class T>
publisher_ptr<T> publisher<T>::take(size_t n) {
  auto ptr = make_counted<limiter<T>>(ctx_, n);
  subscribe(subscriber_ptr<T>{ptr});
  return publisher_ptr<T>{std::move(ptr)};
}

} // namespace caf::flow
