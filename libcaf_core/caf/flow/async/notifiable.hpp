// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/detail/core_export.hpp"
#include "caf/fwd.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow::async {

/// Synchronization primitive for signaling events to a worker that acts as
/// publisher or processor on behalf of a producer that runs asynchronously.
class CAF_CORE_EXPORT notifiable : public virtual ref_counted {
public:
  ~notifiable() override;

  /// Signals to the worker that new data or events are available.
  virtual void on_notify() = 0;

  /// Signals to the worker that the producer is shutting down gracefully.
  virtual void on_close() = 0;

  /// Signals to the worker that the producer is shutting down after
  /// encountering an error.
  virtual void on_abort(const error& reason) = 0;
};

/// @relates notifiable
using notifiable_ptr = intrusive_ptr<notifiable>;

} // namespace caf::flow::async
