// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/async/fwd.hpp"
#include "caf/detail/core_export.hpp"
#include "caf/flow/fwd.hpp"
#include "caf/fwd.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow {

class CAF_CORE_EXPORT observer_base : public virtual ref_counted {
public:
  ~observer_base() override;
  virtual void on_complete() = 0;
  virtual void on_error(const error& what) = 0;
  virtual void on_attach(subscription sub) = 0;
  virtual void on_batch(const async::batch& buf) = 0;
};

using observer_base_ptr = intrusive_ptr<observer_base>;

} // namespace caf::flow
