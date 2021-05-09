// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/flow/coordinator.hpp"

#include "caf/config.hpp"
#include "caf/flow/publisher_factory.hpp"

namespace caf::flow {

coordinator::~coordinator() {
  // nop
}

publisher_factory_ptr coordinator::make_publisher() {
  return publisher_factory_ptr{this};
}

void coordinator::subscription_impl::request(size_t n) {
  CAF_ASSERT(n != 0);
  ctx_->dispatch_request(src_.get(), snk_.get(), n);
}

void coordinator::subscription_impl::cancel() {
  ctx_->dispatch_cancel(src_.get(), snk_.get());
  src_.reset();
  snk_.reset();
}

} // namespace caf::flow
