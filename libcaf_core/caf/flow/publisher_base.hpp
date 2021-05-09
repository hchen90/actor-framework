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

class CAF_CORE_EXPORT publisher_base : public virtual ref_counted {
public:
  friend class coordinator;

  explicit publisher_base(coordinator* ctx) : ctx_(ctx) {
    // nop
  }

  ~publisher_base() override;

  virtual void on_request(subscriber_base* sink, size_t n) = 0;
  virtual void on_cancel(subscriber_base* sink) = 0;

  coordinator* ctx() {
    return ctx_;
  }

protected:
  coordinator* ctx_;
  void do_subscribe(subscriber_base* snk);
};

using publisher_base_ptr = intrusive_ptr<publisher_base>;

} // namespace caf::flow
