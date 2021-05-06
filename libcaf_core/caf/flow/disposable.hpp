// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/detail/core_export.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow {

/// Represents a disposable resource.
class CAF_CORE_EXPORT disposable : public virtual ref_counted {
public:
  virtual ~disposable();

  /// Disposes the resource.
  /// @note Calling `dispose()` on a disposed resource is a no-op.
  virtual void dispose() = 0;

  /// Checks whether the resource has been disposed.
  virtual bool disposed() const noexcept = 0;
};

/// @relates disposable
using disposable_ptr = intrusive_ptr<disposable>;

} // namespace caf::flow
