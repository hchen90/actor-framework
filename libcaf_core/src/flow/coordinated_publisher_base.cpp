// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/flow/coordinated_publisher_base.hpp"

#include "caf/flow/coordinated.hpp"
#include "caf/flow/subscriber_base.hpp"

namespace caf::flow {

coordinated_publisher_base::~coordinated_publisher_base() {
  // nop
}

void coordinated_publisher_base::do_subscribe(subscriber_base* snk) {
  return snk->on_subscribe(
    make_counted<coordinated_subscription>(ctx_, this, snk));
}

} // namespace caf::flow
