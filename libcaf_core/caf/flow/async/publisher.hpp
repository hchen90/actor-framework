// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <type_traits>

#include "caf/flow/blocking_subscriber.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/fwd.hpp"
#include "caf/fwd.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow::async {

/// An item source behind an asynchronous barrier. Calls member functions on its
/// subscribers according to the Reactive Streams protocol
/// (https://www.reactive-streams.org). Subscribers attached to an asynchronous
/// publisher should not perform more work than necessary in their callbacks and
/// instead transfer tasks and data from the subscription to some other thread
/// or scheduler.
template <class T>
class publisher : public virtual ref_counted {
public:
  using published_type = T;

  /// Adds a new sink to the stream. The publisher calls member functions on the
  /// sink asynchronously.
  virtual void async_subscribe(subscriber_ptr<T> sink) = 0;

  /// Creates a new coordinator, then observes this asynchronous publisher
  /// locally and optionally returns a new (lifted) publisher created by the
  /// coordinator.
  template <class Impl = event_based_actor, class Context, class Fn,
            class... Ts>
  auto subscribe_with(Context& ctx, Fn init, Ts&&... ctor_args) {
    auto [ptr, launch] = ctx.template make_flow_coordinator<Impl>(
      std::forward<Ts>(ctor_args)...);
    auto pub = intrusive_ptr<publisher>{this};
    using init_res_t = decltype(init(ptr, ptr->observe(std::move(pub))));
    if constexpr (std::is_same_v<init_res_t, void>) {
      init(ptr, ptr->observe(std::move(pub)));
      launch();
    } else {
      auto init_res = init(ptr, ptr->observe(std::move(pub)));
      auto res = ptr->to_async_publisher(init_res->as_publisher());
      launch();
      return res;
    }
  }

  template <class OnNext, class OnError, class OnComplete>
  void for_each(OnNext fun, OnError err, OnComplete fin) {
    static_assert(std::is_invocable_v<OnNext, T>,
                  "OnNext handlers must have signature 'void(T)'");
    using res_t = decltype(fun(std::declval<T>()));
    static_assert(std::is_same_v<res_t, void>,
                  "OnNext handlers must have signature 'void(T)'");
    auto sub = make_counted<blocking_subscriber<T>>();
    async_subscribe(sub);
    sub->run(std::move(fun), std::move(err), std::move(fin));
  }

  template <class OnNext, class OnError>
  void for_each(OnNext fun, OnError err) {
    for_each(std::move(fun), std::move(err), [] {});
  }

  template <class OnNext>
  void for_each(OnNext fun) {
    for_each(
      std::move(fun), [](const error&) {}, [] {});
  }

  template <class OnNext, class OnError, class OnComplete>
  void for_each_while(OnNext fun, OnError err, OnComplete fin) {
    static_assert(std::is_invocable_v<OnNext, T>,
                  "OnNext handlers must have signature 'bool(T)'");
    using res_t = decltype(fun(std::declval<T>()));
    static_assert(std::is_same_v<res_t, bool>,
                  "OnNext handlers must have signature 'bool(T)'");
    auto sub = make_counted<blocking_subscriber<T>>();
    async_subscribe(sub);
    sub->run(std::move(fun), std::move(err), std::move(fin));
  }

  template <class OnNext, class OnError>
  void for_each_while(OnNext fun, OnError err) {
    for_each_while(std::move(fun), std::move(err), [] {});
  }

  template <class OnNext>
  void for_each_while(OnNext fun) {
    for_each_while(
      std::move(fun), [](const error&) {}, [] {});
  }
};

/// @relates publisher
template <class T>
using publisher_ptr = intrusive_ptr<publisher<T>>;

/// Creates a new @ref flow::async::publisher that dispatches to a
/// @ref flow::publisher managed by a new @ref coordinator.
/// @relates publisher
template <class Impl = event_based_actor, class Context, class Fn, class... Ts>
auto publisher_from(Context& ctx, Fn init, Ts&&... ctor_args) {
  auto [ptr, launch]
    = ctx.template make_flow_coordinator<Impl>(std::forward<Ts>(ctor_args)...);
  using init_res_t = decltype(init(ptr));
  static_assert(!std::is_same_v<init_res_t, void>);
  auto res = ptr->to_async_publisher(init(ptr));
  launch();
  return res;
}

} // namespace caf::flow::async
