// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/flow/publisher_base.hpp"

#include "caf/flow/coordinator.hpp"
#include "caf/flow/subscriber_base.hpp"
#include "caf/make_counted.hpp"

namespace caf::flow {

publisher_base::~publisher_base() {
  // nop
}

void publisher_base::do_subscribe(subscriber_base* snk) {
  using impl = coordinator::subscription_impl;
  return snk->on_subscribe(make_counted<impl>(ctx_, this, snk));
}

} // namespace caf::flow
