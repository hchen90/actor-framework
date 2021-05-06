// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <tuple>

#include "caf/detail/core_export.hpp"
#include "caf/flow/fwd.hpp"

namespace caf::flow {

/// Coordinates any number of co-located publishers and subscribers. The
/// co-located objects never need to synchronize calls to other co-located
/// objects since the coordinator guarantees synchronous execution.
class CAF_CORE_EXPORT coordinator {
public:
  friend class coordinated_subscription;

  virtual ~coordinator();

private:
  /// Eventually executes `source->on_request(sink, n)`.
  /// @pre `source != nullptr`.
  /// @pre `sink != nullptr`.
  /// @pre `n > 0`.
  virtual void dispatch_request(coordinated_publisher_base* source,
                                subscriber_base* sink, size_t n)
    = 0;

  /// Eventually executes `source->on_cancel(sink)`.
  /// @pre `source != nullptr`.
  /// @pre `sink != nullptr`.
  virtual void
  dispatch_cancel(coordinated_publisher_base* source, subscriber_base* sink)
    = 0;

  /// Asks the coordinator to keep its event loop running until `obj` becomes
  /// disposed since it depends on external events.
  /// @pre `obj != nullptr`.
  virtual void watch(disposable* obj) = 0;
};

/// Creates a new @ref coordinator with a function to set up flow logic before
/// starting the coordinator.
/// @returns A handle to the new @ref coordinator if `init` returns void.
///          Otherwise, returns a tuple with the handle as first value and
///          lifted returns values of `init` for the other values.
/// @relates coordinator
template <class Impl, class Context, class Fn, class... Ts>
auto spawn(Context& ctx, Fn init, Ts&&... ctor_args) {
  auto [ptr, launch]
    = ctx.template make_flow_coordinator<Impl>(std::forward<Ts>(ctor_args)...);
  using init_res_t = decltype(init(ptr));
  if constexpr (std::is_same_v<init_res_t, void>) {
    init(ptr);
    auto hdl = typename Impl::handle_type{ptr};
    launch();
    return hdl;
  } else {
    auto res = ptr->lift(init(ptr));
    auto hdl = typename Impl::handle_type{ptr};
    launch();
    return std::make_tuple(hdl, res);
  }
}

} // namespace caf::flow
