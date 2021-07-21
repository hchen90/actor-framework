// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/flow/observable_base.hpp"

#include "caf/flow/coordinator.hpp"
#include "caf/flow/observer_base.hpp"
#include "caf/make_counted.hpp"

namespace caf::flow {

observable_base::~observable_base() {
  // nop
}

disposable observable_base::do_attach(observer_base* snk) {
  using impl = coordinator::subscription_impl;
  auto ptr = make_counted<impl>(ctx_, this, snk);
  snk->on_attach(subscription{ptr});
  return disposable{std::move(ptr)};
}

} // namespace caf::flow
