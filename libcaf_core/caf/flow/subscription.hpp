// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <cstddef>

#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow {

/// Controls the flow of items from publishers to subscribers.
class subscription : public ref_counted {
public:
  /// Causes the publisher to stop producing items for the subscriber. Any
  /// in-flight items may still get dispatched.
  virtual void cancel() = 0;

  /// Signals demand for `n` more items.
  virtual void request(size_t n) = 0;
};

/// @relates subscription
using subscription_ptr = intrusive_ptr<subscription>;

} // namespace caf::flow
