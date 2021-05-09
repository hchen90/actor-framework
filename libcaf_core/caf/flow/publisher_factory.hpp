// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/defaults.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/publisher.hpp"

namespace caf::flow {

class publisher_factory {
public:
  friend class publisher_factory_ptr;

  publisher_factory(const publisher_factory&) noexcept = default;
  publisher_factory& operator=(const publisher_factory&) noexcept = default;

  template <class T>
  [[nodiscard]] publisher_ptr<T> repeat(T value) const;

private:
  explicit publisher_factory(coordinator* ctx) : ctx_(ctx) {
    // nop
  }

  coordinator* ctx_;
};

/// @note Currently there is no need for allocating publisher factories on the
///       heap. However, we still want to keep the API flexible for allowing
///       such changes in the future without braking existing code.
class publisher_factory_ptr {
public:
  friend class coordinator;

  const publisher_factory* operator->() const {
    return &fac_;
  }

private:
  explicit publisher_factory_ptr(coordinator* ctx) : fac_(ctx) {
    // nop
  }

  publisher_factory fac_;
};

// -- publisher_factory::repeat ------------------------------------------------

template <class T>
class repeater : public publisher<T> {
public:
  using super = publisher<T>;

  repeater(coordinator* ctx, T value)
    : super(ctx), buf_(defaults::flow::batch_size, std::move(value)) {
    // nop
  }

  void on_request(subscriber_base* ptr, size_t n) override {
    if (sink_.get() == ptr) {
      while (n > buf_.size()) {
        sink_->on_next(buf_);
        n -= buf_.size();
      }
      if (n > 0)
        sink_->on_next(span<const T>{buf_.data(), n});
    }
  }

  void on_cancel(subscriber_base*) override {
    sink_ = nullptr;
  }

  void subscribe(subscriber_ptr<T> sink) override {
    if (!sink_) {
      super::do_subscribe(sink.get());
      sink_ = std::move(sink);
    } else {
      auto err = make_error(sec::cannot_add_upstream,
                            "repeater already connected to another subscriber");
      sink->on_error(err);
    }
  }

  void dispose() override {
    if (sink_)
      sink_->on_complete();
  }

  bool disposed() const noexcept override {
    return sink_ == nullptr;
  }

private:
  subscriber_ptr<T> sink_;
  std::vector<T> buf_;
};

template <class T>
publisher_ptr<T> publisher_factory::repeat(T value) const {
  return make_counted<repeater<T>>(ctx_, std::move(value));
}

} // namespace caf::flow
