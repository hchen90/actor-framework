// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/fwd.hpp"

namespace caf::flow {

class batch;

class publisher_factory;

class publisher_factory_ptr;

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

class coordinator;

class publisher_base;

using publisher_base_ptr = intrusive_ptr<publisher_base>;

template <class T>
class publisher;

template <class T>
using publisher_ptr = intrusive_ptr<publisher<T>>;

} // namespace caf::flow

namespace caf::flow::async {

class notifiable;

using notifiable_ptr = intrusive_ptr<notifiable>;

template <class T>
class publisher;

template <class T>
using publisher_ptr = intrusive_ptr<publisher<T>>;

template <class T>
class publishing_queue;

template <class T>
using publishing_queue_ptr = intrusive_ptr<publishing_queue<T>>;

} // namespace caf::flow::async

namespace caf::flow {

template <class T>
struct subscribed_type_oracle {
  using type = typename T::subscribed_type;
};

template <class T>
struct subscribed_type_oracle<intrusive_ptr<T>> : subscribed_type_oracle<T> {};

template <class T>
using subscribed_type_t = typename subscribed_type_oracle<T>::type;

template <class T>
struct published_type_oracle {
  using type = typename T::published_type;
};

template <class T>
struct published_type_oracle<intrusive_ptr<T>> : published_type_oracle<T> {};

template <class T>
using published_type_t = typename published_type_oracle<T>::type;

template <class>
struct has_impl_include {
  static constexpr bool value = false;
};

template <class T>
constexpr bool has_impl_include_v = has_impl_include<T>::value;

} // namespace caf::flow
