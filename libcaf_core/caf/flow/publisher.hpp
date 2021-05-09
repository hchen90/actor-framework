// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <cstddef>
#include <numeric>
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

  using observed_type = T;

  explicit publisher(coordinator* ctx) : super(ctx) {
    // nop
  }

  virtual void subscribe(subscriber_ptr<T> sink) = 0;

  template <class OnNext>
  std::enable_if_t<std::is_invocable_v<OnNext, T>> subscribe(OnNext fn) {
    subscribe(make_subscriber(std::move(fn)));
  }

  template <class OnNext, class OnError>
  void subscribe(OnNext fn, OnError fail) {
    subscribe(make_subscriber(std::move(fn), std::move(fail)));
  }

  template <class OnNext, class OnError, class OnComplete>
  void subscribe(OnNext fn, OnError fail, OnComplete fin) {
    subscribe(make_subscriber(std::move(fn), std::move(fail), std::move(fin)));
  }

  publisher_ptr<T> take(size_t n);
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

/// Broadcasts its inputs to all subscribers without modifying them.
template <class T>
class broadcaster : public processor<T, T> {
public:
  using super = processor<T, T>;

  struct input_t {
    size_t offset;
    batch items;
  };

  struct output_t {
    size_t demand;
    subscriber_ptr<T> sink;
  };

  explicit broadcaster(coordinator* ctx) : super(ctx) {
    // nop
  }

  void on_complete() override {
    done_ = true;
    sub_ = nullptr;
    if (inputs_.empty()) {
      for (auto& out : outputs_)
        out.sink->on_complete();
      outputs_.clear();
    }
  }

  void on_error(const error& what) override {
    done_ = true;
    sub_ = nullptr;
    for (auto& out : outputs_)
      out.sink->on_error(what);
    inputs_.clear();
    outputs_.clear();
  }

  void on_next(span<const T> items) override {
    if (auto n = push(items); n != items.size())
      inputs_.emplace_back(input_t{0, make_batch(items.subspan(n))});
  }

  void on_batch(const batch& buf) override {
    auto items = buf.items<T>();
    if (auto n = push(items); n != items.size())
      inputs_.emplace_back(input_t{n, buf});
  }

  void on_subscribe(subscription_ptr sub) override {
    if (!sub_) {
      sub_ = std::move(sub);
      sub_->request(defaults::flow::buffer_size);
    } else {
      sub->cancel();
    }
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
        inputs_.clear();
        done_ = true;
        sub_->cancel();
      } else {
        update_max_demand();
        try_push();
      }
    }
  }

  void subscribe(subscriber_ptr<T> sink) override {
    if (done_) {
      sink->on_complete();
    } else {
      max_demand_ = 0;
      outputs_.emplace_back(output_t{0u, sink});
      super::do_subscribe(sink.get());
    }
  }

  void dispose() override {
    done_ = true;
    if (sub_) {
      sub_->cancel();
      sub_ = nullptr;
    }
    inputs_.clear();
    for (auto& out : outputs_)
      out.sink->on_complete();
    outputs_.clear();
  }

  bool disposed() const noexcept override {
    return done_ && inputs_.empty() && outputs_.empty();
  }

private:
  size_t push(span<const T> items) {
    auto n = std::min(items.size(), max_demand_);
    if (n > 0) {
      CAF_ASSERT(inputs_.empty());
      auto selection = items.subspan(0, n);
      for (auto& out : outputs_) {
        out.demand -= n;
        out.sink->on_next(selection);
      }
      if (sub_)
        sub_->request(n);
    }
    return n;
  }

  void try_push() {
    size_t total = 0;
    while (max_demand_ > 0 && !inputs_.empty()) {
      auto& [offset, buf] = inputs_[0];
      auto n = std::min(buf.size() - offset, max_demand_);
      auto items = buf.template items<T>().subspan(offset, n);
      for (auto& out : outputs_) {
        out.demand -= n;
        out.sink->on_next(items);
      }
      max_demand_ -= n;
      if (n + offset == buf.size()) {
        inputs_.erase(inputs_.begin());
      } else {
        CAF_ASSERT(max_demand_ == 0);
        offset += n;
      }
      total += n;
    }
    if (total > 0 && sub_)
      sub_->request(total);
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

  bool done_ = false;
  error abort_reason_;
  size_t max_demand_ = 0;
  std::vector<input_t> inputs_;
  std::vector<output_t> outputs_;
  subscription_ptr sub_;
};

/// Base type for processors with a buffer that broadcasts output to all
/// subscribers.
template <class In, class Out>
class buffered_processor : public processor<In, Out> {
public:
  using super = processor<In, Out>;

  struct output_t {
    size_t demand;
    subscriber_ptr<Out> sink;
  };

  explicit buffered_processor(coordinator* ctx) : super(ctx) {
    buf_.reserve(defaults::flow::buffer_size);
  }

  void on_complete() override {
    sub_ = nullptr;
    shutdown();
  }

  void on_error(const error& what) override {
    done_ = true;
    sub_ = nullptr;
    for (auto& out : outputs_)
      out.sink->on_error(what);
    buf_.clear();
    outputs_.clear();
  }

  void on_subscribe(subscription_ptr sub) override {
    if (!sub_) {
      sub_ = std::move(sub);
      sub_->request(defaults::flow::buffer_size);
    } else {
      sub->cancel();
    }
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

  void subscribe(subscriber_ptr<Out> sink) override {
    if (done_) {
      sink->on_complete();
    } else {
      max_demand_ = 0;
      outputs_.emplace_back(output_t{0u, sink});
      super::do_subscribe(sink.get());
    }
  }

  bool done() const noexcept {
    return done_;
  }

  void dispose() override {
    done_ = true;
    if (sub_) {
      sub_->cancel();
      sub_ = nullptr;
    }
    buf_.clear();
    for (auto& out : outputs_)
      out.sink->on_complete();
    outputs_.clear();
  }

  bool disposed() const noexcept override {
    return done_ && outputs_.empty();
  }

protected:
  template <class Iterator, class Sentinel>
  void append_to_buf(Iterator first, Sentinel last) {
    buf_.insert(buf_.end(), first, last);
  }

  // Stops reading from the source, but allows subscribers to still consume
  // buffered data.
  void shutdown() {
    done_ = true;
    if (sub_) {
      sub_->cancel();
      sub_ = nullptr;
    }
    if (buf_.empty()) {
      for (auto& out : outputs_)
        out.sink->on_complete();
      outputs_.clear();
    }
  }

  void try_push() {
    if (auto n = std::min(max_demand_, buf_.size()); n > 0) {
      auto items = span<const Out>{buf_.data(), n};
      for (auto& out : outputs_) {
        out.demand -= n;
        out.sink->on_next(items);
      }
      max_demand_ -= n;
      buf_.erase(buf_.begin(), buf_.begin() + n);
      if (sub_)
        sub_->request(n);
      if (done_ && buf_.empty()) {
        for (auto& out : outputs_)
          out.sink->on_complete();
        outputs_.clear();
      }
    }
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

  std::vector<Out> buf_;
  bool done_ = false;
  error abort_reason_;
  size_t max_demand_ = 0;
  std::vector<output_t> outputs_;
  subscription_ptr sub_;
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
      super::try_push();
    } else {
      super::append_to_buf(items.begin(), items.begin() + remaining_);
      remaining_ = 0;
      super::try_push();
      super::shutdown();
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
