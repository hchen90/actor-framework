// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/flow/observable.hpp"
#include "caf/flow/observer.hpp"

namespace caf::flow {

/// Combines items from any number of observables.
template <class T>
class merger_impl : public buffered_observable_impl<T> {
public:
  using super = buffered_observable_impl<T>;

  using super::super;

  class forwarder;

  friend class forwarder;

  class forwarder : public observer<T>::impl {
  public:
    explicit forwarder(intrusive_ptr<merger_impl> parent)
      : parent(std::move(parent)) {
      // nop
    }

    void on_complete() override {
      if (sub) {
        sub = nullptr;
        parent->forwarder_completed(this);
        parent = nullptr;
      }
    }

    void on_error(const error& what) override {
      if (sub) {
        sub = nullptr;
        parent->forwarder_failed(this, what);
        parent = nullptr;
      }
    }

    void on_attach(subscription new_sub) override {
      if (!sub) {
        sub = std::move(new_sub);
        sub.request(defaults::flow::buffer_size);
      } else {
        new_sub.cancel();
      }
    }

    void on_batch(const async::batch& buf) override {
      if (parent)
        parent->on_batch(buf, this);
    }

    void on_next(span<const T> items) override {
      if (parent)
        parent->on_batch(async::make_batch(items), this);
    }

    void dispose() override {
      on_complete();
    }

    bool disposed() const noexcept override {
      return !parent;
    }

    intrusive_ptr<merger_impl> parent;
    subscription sub;
  };

  explicit merger_impl(coordinator* ctx)
    : super(ctx, defaults::flow::batch_size) {
    // nop
  }

  void add(observable<T> source, intrusive_ptr<forwarder> fwd) {
    forwarders_.emplace_back(fwd);
    source.attach(observer<T>{std::move(fwd)});
  }

  void add(observable<T> source) {
    add(std::move(source), make_counted<forwarder>(this));
  }

  bool done() const noexcept override {
    return super::done() && inputs_.empty() && forwarders_.empty();
  }

  void dispose() override {
    inputs_.clear();
    std::vector<fwd_ptr> fwds;
    fwds.swap(forwarders_);
    for (auto& fwd : fwds)
      fwd->dispose();
    super::dispose();
  }

  bool disposed() const noexcept override {
    return forwarders_.empty() && super::disposed();
  }

  void delay_error(bool value) {
    flags.delay_error = value;
  }

  void shutdown_on_last_complete(bool value) {
    flags.shutdown_on_last_complete = value;
  }

protected:
  void abort(const error& reason) override {
    super::abort(reason);
    inputs_.clear();
    forwarders_.clear();
  }

private:
  using fwd_ptr = intrusive_ptr<forwarder>;

  void pull(size_t n) override {
    while (n > 0 && !inputs_.empty()) {
      auto& input = inputs_[0];
      auto m = std::min(input.buf.size() - input.offset, n);
      CAF_ASSERT(m > 0);
      auto items = input.buf.template items<T>().subspan(input.offset, m);
      this->append_to_buf(items.begin(), items.end());
      if (m + input.offset == input.buf.size()) {
        if (auto& sub = input.src->sub)
          sub.request(input.buf.size());
        inputs_.erase(inputs_.begin());
      } else {
        input.offset += m;
      }
      n -= m;
    }
  }

  void on_batch(async::batch buf, fwd_ptr src) {
    inputs_.emplace_back(buf, src);
    this->try_push();
  }

  void forwarder_failed(forwarder* ptr, const error& reason) {
    if (!flags.delay_error) {
      abort(reason);
      return;
    }
    if (!delayed_error)
      delayed_error = reason;
    forwarder_completed(ptr);
  }

  void forwarder_completed(forwarder* ptr) {
    auto is_ptr = [ptr](auto& x) { return x == ptr; };
    auto i = std::find_if(forwarders_.begin(), forwarders_.end(), is_ptr);
    if (i != forwarders_.end()) {
      forwarders_.erase(i);
      if (forwarders_.empty() && flags.shutdown_on_last_complete) {
        if (delayed_error)
          this->abort(delayed_error);
        else
          this->shutdown();
      }
    }
  }

  struct input_t {
    size_t offset = 0;

    async::batch buf;

    fwd_ptr src;

    input_t(async::batch content, fwd_ptr source)
      : buf(std::move(content)), src(std::move(source)) {
      // nop
    }
  };

  struct flags_t {
    bool delay_error : 1;
    bool shutdown_on_last_complete : 1;

    flags_t() : delay_error(false), shutdown_on_last_complete(true) {
      // nop
    }
  };

  std::vector<input_t> inputs_;
  std::vector<fwd_ptr> forwarders_;
  flags_t flags;
  error delayed_error;
};

/// Combines the items emitted from `pub` and `pubs...` to appear as a single
/// stream of items.
template <class Observable, class... Observables>
observable<typename Observable::output_type>
merge(Observable x, Observables... xs) {
  using output_type = output_type_t<Observable>;
  static_assert(
    (std::is_same_v<output_type, output_type_t<Observables>> && ...));
  auto hdl = std::move(x).as_observable();
  auto ptr = make_counted<merger_impl<output_type>>(hdl.ptr()->ctx());
  ptr->add(std::move(hdl));
  (ptr->add(std::move(xs).as_observable()), ...);
  return observable<output_type>{std::move(ptr)};
}

} // namespace caf::flow
