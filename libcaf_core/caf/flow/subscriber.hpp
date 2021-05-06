// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/defaults.hpp"
#include "caf/detail/type_traits.hpp"
#include "caf/error.hpp"
#include "caf/flow/batch.hpp"
#include "caf/flow/subscriber_base.hpp"
#include "caf/flow/subscription.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/make_counted.hpp"
#include "caf/ref_counted.hpp"
#include "caf/span.hpp"

namespace caf::flow {

/// A consumer of items. The item producer must call member functions on the
/// subscriber in strict sequential order.
template <class T>
class subscriber : public subscriber_base {
public:
  using value_type = T;
  virtual void on_next(span<const T> items) = 0;
  void on_batch(const batch& buf) override {
    on_next(buf.template items<T>());
  }
};

/// @relates subscriber
template <class T>
using subscriber_ptr = intrusive_ptr<subscriber<T>>;

template <class OnNextSignature>
struct on_next_trait;

template <class T>
struct on_next_trait<void(T)> {
  using value_type = T;

  template <class F>
  static void apply(F& f, span<const T> items) {
    for (auto&& item : items)
      f(item);
  }
};

template <class T>
struct on_next_trait<void(const T&)> {
  using value_type = T;

  template <class F>
  static void apply(F& f, span<const T> items) {
    for (auto&& item : items)
      f(item);
  }
};

template <class T>
struct on_next_trait<void(span<const T>)> {
  using value_type = T;

  template <class F>
  static void apply(F& f, span<const T> items) {
    f(items);
  }
};

template <class OnNext, class OnError, class OnComplete>
auto make_subscriber(OnNext fun, OnError err, OnComplete fin) {
  using detail::get_callable_trait_t;
  using fun_trait = get_callable_trait_t<OnNext>;
  using err_trait = get_callable_trait_t<OnError>;
  using fin_trait = get_callable_trait_t<OnComplete>;
  static_assert(std::is_same_v<typename err_trait::fun_sig, void(const error&)>,
                "OnError handlers must have signature 'void(const error&)'");
  static_assert(std::is_same_v<typename fin_trait::fun_sig, void()>,
                "OnComplete handlers must have signature 'void()'");
  static_assert(std::is_same_v<typename fun_trait::result_type, void>,
                "OnNext handlers must return 'void'");
  using trait = on_next_trait<typename fun_trait::fun_sig>;
  static_assert(detail::is_complete<trait>,
                "OnNext handler must take one value or a const span");
  using value_type = typename trait::value_type;
  struct impl : subscriber<value_type> {
    impl(OnNext&& on_next_fn, OnError&& on_error_fn,
         OnComplete&& on_complete_fn)
      : on_next_(std::move(on_next_fn)),
        on_error_(std::move(on_error_fn)),
        on_complete_(std::move(on_complete_fn)) {
      // nop
    }
    void on_next(span<const value_type> items) {
      trait::apply(on_next_, items);
      sub_->request(items.size());
    }
    void on_error(const error& what) {
      on_error_(what);
      sub_ = nullptr;
    }
    void on_complete() {
      on_complete_();
      sub_ = nullptr;
    }
    void on_subscribe(subscription_ptr sub) {
      sub_ = std::move(sub);
      sub_->request(defaults::flow::buffer_size);
    }
    OnNext on_next_;
    OnError on_error_;
    OnComplete on_complete_;
    subscription_ptr sub_;
  };
  auto ptr = make_counted<impl>(std::move(fun), std::move(err), std::move(fin));
  return subscriber_ptr<value_type>{std::move(ptr)};
}

template <class OnNext, class OnError>
auto make_subscriber(OnNext fun, OnError err) {
  return make_subscriber(std::move(fun), std::move(err), [] {});
}

template <class OnNext>
auto make_subscriber(OnNext fun) {
  return make_subscriber(
    std::move(fun), [](const error&) {}, [] {});
}

} // namespace caf::flow
