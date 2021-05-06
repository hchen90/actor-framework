// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/detail/core_export.hpp"
#include "caf/flow/fwd.hpp"
#include "caf/fwd.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"

namespace caf::flow {

class CAF_CORE_EXPORT subscriber_base : public virtual ref_counted {
public:
  ~subscriber_base() override;
  virtual void on_complete() = 0;
  virtual void on_error(const error& what) = 0;
  virtual void on_subscribe(subscription_ptr sub) = 0;
  virtual void on_batch(const batch& buf) = 0;
};

using subscriber_base_ptr = intrusive_ptr<subscriber_base>;

} // namespace caf::flow
