// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/fwd.hpp"

namespace caf::flow {

class batch;

class disposable;

using disposable_ptr = intrusive_ptr<disposable>;

class subscription;

using subscription_ptr = intrusive_ptr<subscription>;

class subscriber_base;

using subscriber_base_ptr = intrusive_ptr<subscriber_base>;

template <class T>
class subscriber;

template <class T>
using subscriber_ptr = intrusive_ptr<subscriber<T>>;

template <class T>
class publisher;

template <class T>
using publisher_ptr = intrusive_ptr<publisher<T>>;

class coordinator;

class coordinated_publisher_base;

using coordinated_publisher_base_ptr
  = intrusive_ptr<coordinated_publisher_base>;

template <class T>
class coordinated_publisher;

template <class T>
using coordinated_publisher_ptr = intrusive_ptr<coordinated_publisher<T>>;

} // namespace caf::flow
