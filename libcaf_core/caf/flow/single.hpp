// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <algorithm>
#include <utility>
#include <variant>

#include "caf/detail/overload.hpp"
#include "caf/disposable.hpp"
#include "caf/error.hpp"
#include "caf/flow/observable.hpp"
#include "caf/none.hpp"

namespace caf::flow {

/// Similar to an `observable`, but always emits either a single value or an
/// error.
template <class T>
class single {
public:
  using output_type = T;

  /// Internal interface of a `single`.
  class impl : public observable<T>::impl {
  public:
    using super = typename observable<T>::impl;

    explicit impl(coordinator* ctx) : super(ctx) {
      // nop
    }

    void attach(observer<T> what) override {
      if (!std::holds_alternative<error>(value_)) {
        super::do_attach(what.ptr());
        observers_.emplace_back(std::move(what), 0u);
      } else {
        what.on_error(std::get<error>(value_));
      }
    }

    void on_request(observer_base* sink, size_t n) override {
      auto pred = [sink](auto& entry) { return entry.first.ptr() == sink; };
      if (auto i = std::find_if(observers_.begin(), observers_.end(), pred);
          i != observers_.end()) {
        auto f = detail::make_overload( //
          [i, n](none_t) { i->second += n; },
          [this, i](const T& val) {
            i->first.on_next(make_span(&val, 1));
            i->first.on_complete();
            observers_.erase(i);
          },
          [this, i](const error& err) {
            i->first.on_error(err);
            observers_.erase(i);
          });
        std::visit(f, value_);
      }
    }

    void on_cancel(observer_base* sink) override {
      auto pred = [sink](auto& entry) { return entry.first.ptr() == sink; };
      if (auto i = std::find_if(observers_.begin(), observers_.end(), pred);
          i != observers_.end())
        observers_.erase(i);
    }

    void dispose() override {
      if (!std::holds_alternative<error>(value_))
        set_error(make_error(sec::discarded));
    }

    bool disposed() const noexcept override {
      return observers_.empty() && !std::holds_alternative<none_t>(value_);
    }

    void set_value(T val) {
      if (std::holds_alternative<none_t>(value_)) {
        value_ = std::move(val);
        auto& ref = std::get<T>(value_);
        auto pred = [](auto& entry) { return entry.second == 0; };
        if (auto first = std::partition(observers_.begin(), observers_.end(),
                                        pred);
            first != observers_.end()) {
          for (auto i = first; i != observers_.end(); ++i) {
            i->first.on_next(make_span(&ref, 1));
            i->first.on_complete();
          }
          observers_.erase(first, observers_.end());
        }
      }
    }

    void set_error(error err) {
      value_ = std::move(err);
      auto& ref = std::get<error>(value_);
      for (auto& entry : observers_)
        entry.first.on_error(ref);
      observers_.clear();
    }

  private:
    std::variant<none_t, T, error> value_;
    std::vector<std::pair<observer<T>, size_t>> observers_;
  };

  explicit single(intrusive_ptr<impl> pimpl) noexcept
    : pimpl_(std::move(pimpl)) {
    // nop
  }

  single& operator=(std::nullptr_t) noexcept {
    pimpl_.reset();
    return *this;
  }

  single() noexcept = default;
  single(single&&) noexcept = default;
  single(const single&) noexcept = default;
  single& operator=(single&&) noexcept = default;
  single& operator=(const single&) noexcept = default;

  disposable as_disposable() && {
    return disposable{std::move(pimpl_)};
  }
  disposable as_disposable() const& {
    return disposable{pimpl_};
  }

  observable<T> as_observable() && {
    return observable<T>{std::move(pimpl_)};
  }
  observable<T> as_observable() const& {
    return observable<T>{pimpl_};
  }

  void attach(observer<T> what) {
    if (pimpl_)
      pimpl_->attach(std::move(what));
    else
      what.on_error(make_error(sec::invalid_observable));
  }

  template <class OnSuccess, class OnError>
  void attach(OnSuccess on_success, OnError on_error) {
    static_assert(std::is_invocable_v<OnSuccess, const T&>);
    as_observable().for_each(
      [f{std::move(on_success)}](span<const T> items) mutable {
        CAF_ASSERT(items.size() == 1);
        f(items[0]);
      },
      std::move(on_error));
  }

  bool valid() const noexcept {
    return pimpl_ != nullptr;
  }

  explicit operator bool() const noexcept {
    return valid();
  }

  bool operator!() const noexcept {
    return !valid();
  }

  impl* ptr() {
    return pimpl_.get();
  }

  const impl* ptr() const {
    return pimpl_.get();
  }

  const intrusive_ptr<impl>& as_intrusive_ptr() const& noexcept {
    return pimpl_;
  }

  intrusive_ptr<impl>&& as_intrusive_ptr() && noexcept {
    return std::move(pimpl_);
  }

  void swap(single& other) {
    pimpl_.swap(other.pimpl_);
  }

private:
  intrusive_ptr<impl> pimpl_;
};

} // namespace caf::flow
