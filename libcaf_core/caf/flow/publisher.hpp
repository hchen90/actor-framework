// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <type_traits>

#include "caf/flow/fwd.hpp"
#include "caf/fwd.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow {

/// An item source.
template <class T>
class publisher : public virtual ref_counted {
public:
  using observed_type = T;

  /// Adds a new sink to the stream.
  virtual void subscribe(subscriber_ptr<T> sink) = 0;

  template <class Impl = event_based_actor, class Context, class Fn,
            class... Ts>
  auto observe_with(Context& ctx, Fn init, Ts&&... ctor_args) {
    auto [ptr, launch] = ctx.template make_flow_coordinator<Impl>(
      std::forward<Ts>(ctor_args)...);
    auto pub = intrusive_ptr<publisher>{this};
    using init_res_t = decltype(init(ptr, ptr->observe(std::move(pub))));
    if constexpr (std::is_same_v<init_res_t, void>) {
      init(ptr, ptr->observe(std::move(pub)));
      launch();
    } else {
      auto res = ptr->lift(init(ptr, ptr->observe(std::move(pub))));
      launch();
      return res;
    }
  }
};

/// @relates publisher
template <class T>
using publisher_ptr = intrusive_ptr<publisher<T>>;

/// Creates a new @ref publisher by creating a new @ref coordinator with a
/// function that creates a @ref coordinated_publisher on that coordinator.
/// @relates publisher
template <class Impl = event_based_actor, class Context, class Fn, class... Ts>
auto from(Context& ctx, Fn init, Ts&&... ctor_args) {
  auto [ptr, launch]
    = ctx.template make_flow_coordinator<Impl>(std::forward<Ts>(ctor_args)...);
  using init_res_t = decltype(init(ptr));
  static_assert(!std::is_same_v<init_res_t, void>);
  auto res = ptr->lift(init(ptr));
  launch();
  return res;
}

} // namespace caf::flow
