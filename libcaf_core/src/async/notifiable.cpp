// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/async/notifiable.hpp"

namespace caf::async {

notifiable::impl::~impl() {
  // nop
}

notifiable::listener::~listener() {
  // nop
}

} // namespace caf::async
