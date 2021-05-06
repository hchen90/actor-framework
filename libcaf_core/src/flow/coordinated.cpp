// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/flow/coordinated.hpp"

#include "caf/flow/coordinator.hpp"

namespace caf::flow {

void coordinated_subscription::request(size_t n) {
  CAF_ASSERT(n != 0);
  ctx_->dispatch_request(src_.get(), snk_.get(), n);
}

void coordinated_subscription::cancel() {
  ctx_->dispatch_cancel(src_.get(), snk_.get());
  src_.reset();
  snk_.reset();
}

} // namespace caf::flow
