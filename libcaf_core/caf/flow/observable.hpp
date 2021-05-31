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
#include "caf/disposable.hpp"
#include "caf/flow/fwd.hpp"
#include "caf/flow/observable_base.hpp"
#include "caf/flow/observer.hpp"
#include "caf/flow/step.hpp"
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
/// Observers must belong to the same @ref coordinator as the observable.
template <class T>
class observable {
public:
  using output_type = T;

  /// Internal impl of an `observable`.
  class impl : public observable_base, public disposable::impl {
  public:
    using super = observable_base;

    using output_type = T;

    explicit impl(coordinator* ctx) : super(ctx) {
      // nop
    }

    /// Attaches a new observer.
    virtual void attach(observer<T> what) = 0;

    disposable as_disposable() noexcept {
      return disposable{intrusive_ptr<disposable::impl>{this}};
    }

    observable as_observable() noexcept {
      return observable{intrusive_ptr<impl>(this)};
    }
  };

  explicit observable(intrusive_ptr<impl> pimpl) noexcept
    : pimpl_(std::move(pimpl)) {
    // nop
  }

  observable& operator=(std::nullptr_t) noexcept {
    pimpl_.reset();
    return *this;
  }

  observable() noexcept = default;
  observable(observable&&) noexcept = default;
  observable(const observable&) noexcept = default;
  observable& operator=(observable&&) noexcept = default;
  observable& operator=(const observable&) noexcept = default;

  disposable as_disposable() noexcept {
    return disposable{pimpl_};
  }

  /// @copydoc impl::attach
  void attach(observer<T> what) {
    if (pimpl_)
      pimpl_->attach(std::move(what));
    else
      what.on_error(make_error(sec::invalid_observable));
  }

  /// Transforms items by applying a step function to each input.
  template <class Step>
  transformation<Step> transform(Step step);

  transformation<limit_step<T>> take(size_t n);

  template <class Predicate>
  transformation<filter_step<Predicate>> filter(Predicate prediate);

  template <class Fn>
  transformation<map_step<Fn>> map(Fn fn);

  template <class OnNext>
  disposable for_each(OnNext on_next);

  template <class OnNext, class OnError>
  disposable for_each(OnNext on_next, OnError on_error);

  template <class OnNext, class OnError, class OnComplete>
  disposable for_each(OnNext on_next, OnError on_error, OnComplete on_complete);

  template <class Observer, class = std::enable_if_t<is_observer_v<Observer>>>
  auto observe_with(Observer hdl) {
    attach(hdl.as_subscriber());
    return hdl;
  }

  /// Convenience function for calling
  /// `observe_with(make_counted<Impl>(ctor_args...))`.
  template <class Impl, class... Ts>
  auto observe_with_new(Ts&&... ctor_args) {
    auto ptr = make_counted<Impl>(std::forward<Ts>(ctor_args)...);
    attach(ptr->as_observer());
    return typename Impl::handle_type{std::move(ptr)};
  }

  observable&& as_observable() && {
    return std::move(*this);
  }

  bool valid() const noexcept {
    return pimpl_ != nullptr;
  }

  explicit operator bool() const noexcept {
    return valid();
  }

  bool operator!() const noexcept {
    return !valid();
  }

  impl* ptr() {
    return pimpl_.get();
  }

  const impl* ptr() const {
    return pimpl_.get();
  }

  const intrusive_ptr<impl>& as_intrusive_ptr() const& noexcept {
    return pimpl_;
  }

  intrusive_ptr<impl>&& as_intrusive_ptr() && noexcept {
    return std::move(pimpl_);
  }

  void swap(observable& other) {
    pimpl_.swap(other.pimpl_);
  }

private:
  intrusive_ptr<impl> pimpl_;
};

template <class In, class Out>
class processor {
public:
  class impl : public virtual ref_counted {
  public:
    virtual observer<In> as_observer() noexcept = 0;

    virtual typename observer<In>::impl* as_observer_ptr() noexcept = 0;

    virtual observable<Out> as_observable() noexcept = 0;

    virtual typename observable<Out>::impl* as_observable_ptr() noexcept = 0;
  };

  explicit processor(intrusive_ptr<impl> pimpl) noexcept
    : pimpl_(std::move(pimpl)) {
    // nop
  }

  processor& operator=(std::nullptr_t) noexcept {
    pimpl_.reset();
    return *this;
  }

  using input_type = In;

  using output_type = Out;

  processor() noexcept = default;
  processor(processor&&) noexcept = default;
  processor(const processor&) noexcept = default;
  processor& operator=(processor&&) noexcept = default;
  processor& operator=(const processor&) noexcept = default;

  // -- conversion -------------------------------------------------------------

  disposable as_disposable() const& noexcept {
    return as_observable().as_disposable();
  }

  disposable as_disposable() && noexcept {
    return std::move(*this).as_observable().as_disposable();
  }

  observer<In> as_observer() const& noexcept {
    return pimpl_->as_observer();
  }

  observer<In> as_observer() && noexcept {
    auto raw = pimpl_.release()->as_observer_ptr();
    auto ptr = intrusive_ptr<typename observer<In>::impl>{raw, false};
    return observer<In>{std::move(ptr)};
  }

  observable<Out> as_observable() const& noexcept {
    return pimpl_->as_observable();
  }

  observable<Out> as_observable() && noexcept {
    auto raw = pimpl_.release()->as_observable_ptr();
    auto ptr = intrusive_ptr<typename observable<Out>::impl>{raw, false};
    return observable<Out>{std::move(ptr)};
  }

private:
  intrusive_ptr<impl> pimpl_;
};

// -- broadcasting -------------------------------------------------------------

/// Base type for processors with a buffer that broadcasts output to all
/// observers.
template <class T>
class buffered_observable_impl : public observable<T>::impl {
public:
  using super = typename observable<T>::impl;

  using handle_type = observable<T>;

  struct output_t {
    size_t demand;
    observer<T> sink;
  };

  explicit buffered_observable_impl(coordinator* ctx)
    : super(ctx), desired_capacity_(defaults::flow::buffer_size) {
    buf_.reserve(desired_capacity_);
  }

  buffered_observable_impl(coordinator* ctx, size_t desired_capacity)
    : super(ctx), desired_capacity_(desired_capacity) {
    buf_.reserve(desired_capacity_);
  }

  void on_request(observer_base* sink, size_t n) override {
    if (auto i = find(sink); i != outputs_.end()) {
      i->demand += n;
      update_max_demand();
      try_push();
    }
  }

  void on_cancel(observer_base* sink) override {
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

  void attach(observer<T> sink) override {
    if (done()) {
      sink.on_complete();
    } else {
      max_demand_ = 0;
      outputs_.emplace_back(output_t{0u, sink});
      super::do_attach(sink.ptr());
    }
  }

  size_t has_observers() const noexcept {
    return !outputs_.empty();
  }

  virtual bool done() const noexcept {
    return completed_ && buf_.empty();
  }

  void dispose() override {
    completed_ = true;
    buf_.clear();
    for (auto& out : outputs_)
      out.sink.on_complete();
    outputs_.clear();
  }

  bool disposed() const noexcept override {
    return done() && outputs_.empty();
  }

  template <class Iterator, class Sentinel>
  void append_to_buf(Iterator first, Sentinel last) {
    buf_.insert(buf_.end(), first, last);
  }

  template <class Val>
  void append_to_buf(Val&& val) {
    buf_.emplace_back(std::forward<Val>(val));
  }

  // Stops the source, but allows observers to still consume buffered data.
  virtual void shutdown() {
    completed_ = true;
    if (done()) {
      for (auto& out : outputs_)
        out.sink.on_complete();
      outputs_.clear();
    }
  }

  virtual void abort(const error& reason) {
    completed_ = true;
    for (auto& out : outputs_)
      out.sink.on_error(reason);
    outputs_.clear();
  }

  /// Tries to push data from the buffer downstream.
  void try_push() {
    size_t batch_size = std::min(desired_capacity_, defaults::flow::batch_size);
    while (max_demand_ > 0) {
      // Try to ship full batches.
      if (batch_size > buf_.size())
        pull(batch_size - buf_.size());
      auto n = std::min(max_demand_, buf_.size());
      if (n == 0)
        return;
      auto items = span<const T>{buf_.data(), n};
      for (auto& out : outputs_) {
        out.demand -= n;
        out.sink.on_next(items);
      }
      max_demand_ -= n;
      buf_.erase(buf_.begin(), buf_.begin() + n);
      if (done()) {
        for (auto& out : outputs_)
          out.sink.on_complete();
        outputs_.clear();
        return;
      }
    }
  }

  auto find(observer_base* sink) {
    auto pred = [sink](auto& out) { return out.sink.ptr() == sink; };
    return std::find_if(outputs_.begin(), outputs_.end(), pred);
  }

protected:
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

private:
  virtual void pull(size_t) {
    // Customization point for generators that can use this callback for
    // appending to the buffer.
  }
};

template <class T>
struct term_step {
  buffered_observable_impl<T>* pimpl;

  using output_type = T;

  bool on_next(const T& item) {
    pimpl->append_to_buf(item);
    return true;
  }

  void on_complete() {
    pimpl->shutdown();
  }

  void on_error(const error& what) {
    pimpl->abort(what);
  }
};

/// Base type for processors with a buffer that broadcasts output to all
/// observers.
template <class In, class Out>
class buffered_processor_impl : public observer<In>::impl,
                                public buffered_observable_impl<Out>,
                                public processor<In, Out>::impl {
public:
  using super = buffered_observable_impl<Out>;

  using handle_type = processor<In, Out>;

  explicit buffered_processor_impl(coordinator* ctx)
    : super(ctx, defaults::flow::buffer_size) {
    // nop
  }

  buffered_processor_impl(coordinator* ctx, size_t max_buffer_size)
    : super(ctx, max_buffer_size) {
    // nop
  }

  void on_complete() override {
    sub_ = nullptr;
    this->shutdown();
  }

  void on_error(const error& what) override {
    sub_ = nullptr;
    this->abort(what);
  }

  void on_attach(subscription sub) override {
    if (sub_) {
      sub.cancel();
    } else {
      sub_ = std::move(sub);
      in_flight_ = this->desired_capacity_;
      sub_.request(in_flight_);
    }
  }

  void dispose() override {
    if (sub_) {
      sub_.cancel();
      sub_ = nullptr;
    }
    super::dispose();
  }

  bool disposed() const noexcept override {
    // Must provide this override for observer<In>::impl base type.
    return super::disposed();
  }

  void on_request(observer_base* sink, size_t n) final {
    super::on_request(sink, n);
    try_fetch_more();
  }

  void on_cancel(observer_base* sink) final {
    super::on_cancel(sink);
    try_fetch_more();
  }

  void on_next(span<const In> items) final {
    CAF_ASSERT(in_flight_ >= items.size());
    in_flight_ -= items.size();
    do_on_next(items);
    this->try_push();
    try_fetch_more();
  }

  void on_batch(const async::batch& buf) final {
    on_next(buf.template items<In>());
  }

  void shutdown() override {
    super::shutdown();
    cancel_subscription();
  }

  void abort(const error& reason) override {
    super::abort(reason);
    cancel_subscription();
  }

  observer<In> as_observer() noexcept override {
    return observer<In>{intrusive_ptr<buffered_processor_impl>(this)};
  }

  typename observer<In>::impl* as_observer_ptr() noexcept override {
    return this;
  }

  observable<Out> as_observable() noexcept override {
    return observable<Out>{intrusive_ptr<buffered_processor_impl>(this)};
  }

  typename observable<In>::impl* as_observable_ptr() noexcept override {
    return this;
  }

protected:
  subscription sub_;
  size_t in_flight_ = 0;

private:
  void cancel_subscription() {
    if (sub_) {
      sub_.cancel();
      sub_ = nullptr;
    }
  }

  void try_fetch_more() {
    if (sub_) {
      auto abs = in_flight_ + this->buf_.size();
      if (this->desired_capacity_ > abs) {
        auto new_demand = this->desired_capacity_ - abs;
        in_flight_ += new_demand;
        sub_.request(new_demand);
      }
    }
  }

  virtual void do_on_next(span<const In> items) = 0;
};

/// Broadcasts its input to all observers without modifying it.
template <class T>
class broadcaster_impl : public buffered_processor_impl<T, T> {
public:
  using super = buffered_processor_impl<T, T>;

  using super::super;

private:
  void do_on_next(span<const T> items) override {
    this->append_to_buf(items.begin(), items.end());
  }
};

// -- transformation -----------------------------------------------------------

template <class... Steps>
struct transform_processor_oracle;

template <class Step>
struct transform_processor_oracle<Step> {
  using type = typename Step::output_type;
};

template <class Step1, class Step2, class... Steps>
struct transform_processor_oracle<Step1, Step2, Steps...>
  : transform_processor_oracle<Step2, Steps...> {};

template <class... Steps>
using transform_processor_output_type_t =
  typename transform_processor_oracle<Steps...>::type;

/// A special type of observer that applies a series of transformation steps to
/// its input before broadcasting the result as output.
template <class Step, class... Steps>
class transformation {
public:
  using input_type = typename Step::input_type;

  using output_type = transform_processor_output_type_t<Step, Steps...>;

  class impl : public buffered_processor_impl<input_type, output_type> {
  public:
    using super = buffered_processor_impl<input_type, output_type>;

    template <class... Ts>
    explicit impl(coordinator* ctx, Ts&&... steps)
      : super(ctx), steps(std::forward<Ts>(steps)...) {
      // nop
    }

    std::tuple<Step, Steps...> steps;

  private:
    void do_on_next(span<const input_type> items) override {
      auto fn = [this, items](auto& step, auto&... steps) {
        term_step<output_type> term{this};
        for (auto&& item : items)
          if (!step.on_next(item, steps..., term))
            return;
      };
      std::apply(fn, steps);
    }
  };

  template <class Tuple>
  transformation(observable<input_type> source, Tuple&& steps)
    : source_(std::move(source)), steps_(std::move(steps)) {
    // nop
  }

  transformation() = delete;
  transformation(const transformation&) = delete;
  transformation& operator=(const transformation&) = delete;

  transformation(transformation&&) = default;
  transformation& operator=(transformation&&) = default;

  /// @copydoc observable::transform
  template <class NewStep>
  transformation<Step, Steps..., NewStep> transform(NewStep step) && {
    return {std::move(source_),
            std::tuple_cat(std::move(steps_),
                           std::make_tuple(std::move(step)))};
  }

  auto take(size_t n) && {
    return std::move(*this).transform(limit_step<output_type>{n});
  }

  template <class Predicate>
  auto filter(Predicate predicate) && {
    return std::move(*this).transform(
      filter_step<Predicate>{std::move(predicate)});
  }

  template <class Fn>
  auto map(Fn fn) && {
    return std::move(*this).transform(map_step<Fn>{std::move(fn)});
  }

  template <class OnNext>
  disposable for_each(OnNext on_next) && {
    return std::move(*this).as_observable().for_each(std::move(on_next));
  }

  template <class OnNext, class OnError>
  disposable for_each(OnNext on_next, OnError on_error) && {
    return std::move(*this).as_observable().for_each(std::move(on_next),
                                                     std::move(on_error));
  }

  template <class OnNext, class OnError, class OnComplete>
  disposable
  for_each(OnNext on_next, OnError on_error, OnComplete on_complete) && {
    return std::move(*this).as_observable().for_each(std::move(on_next),
                                                     std::move(on_error),
                                                     std::move(on_complete));
  }

  observable<output_type> as_observable() && {
    auto pimpl = make_counted<impl>(source_.ptr()->ctx(), std::move(steps_));
    source_.attach(observer<input_type>{pimpl});
    return observable<output_type>{std::move(pimpl)};
  }

  void attach(observer<output_type> what) && {
    std::move(*this).as_observable().attach(std::move(what));
  }

private:
  observable<input_type> source_;
  std::tuple<Step, Steps...> steps_;
};

// -- observable::transform ----------------------------------------------------

template <class T>
template <class Step>
transformation<Step> observable<T>::transform(Step step) {
  static_assert(std::is_same_v<typename Step::input_type, T>,
                "step object does not match the output type");
  return {*this, std::forward_as_tuple(std::move(step))};
}

// -- observable::take ---------------------------------------------------------

template <class T>
transformation<limit_step<T>> observable<T>::take(size_t n) {
  return {*this, std::forward_as_tuple(limit_step<T>{n})};
}

// -- observable::filter -------------------------------------------------------

template <class T>
template <class Predicate>
transformation<filter_step<Predicate>>
observable<T>::filter(Predicate predicate) {
  using step_type = filter_step<Predicate>;
  static_assert(std::is_same_v<typename step_type::input_type, T>,
                "predicate does not match the output type");
  return {*this, std::forward_as_tuple(step_type{std::move(predicate)})};
}

// -- observable::map ----------------------------------------------------------

template <class T>
template <class Fn>
transformation<map_step<Fn>> observable<T>::map(Fn fn) {
  using step_type = map_step<Fn>;
  static_assert(std::is_same_v<typename step_type::input_type, T>,
                "map function does not match the output type");
  return {*this, std::forward_as_tuple(step_type{std::move(fn)})};
}

// -- observable::for_each -----------------------------------------------------

template <class T>
template <class OnNext>
disposable observable<T>::for_each(OnNext on_next) {
  auto obs = make_observer(std::move(on_next));
  attach(obs);
  return std::move(obs).as_disposable();
}

template <class T>
template <class OnNext, class OnError>
disposable observable<T>::for_each(OnNext on_next, OnError on_error) {
  auto obs = make_observer(std::move(on_next), std::move(on_error));
  attach(obs);
  return std::move(obs).as_disposable();
}

template <class T>
template <class OnNext, class OnError, class OnComplete>
disposable observable<T>::for_each(OnNext on_next, OnError on_error,
                                   OnComplete on_complete) {
  auto obs = make_observer(std::move(on_next), std::move(on_error),
                           std::move(on_complete));
  attach(obs);
  return std::move(obs).as_disposable();
}

} // namespace caf::flow
