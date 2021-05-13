// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/flow/publisher.hpp"

namespace caf::flow {

/// Combines items from any number of publishers.
template <class T>
class merger : public buffered_publisher<T> {
public:
  using super = buffered_publisher<T>;

  using super::super;

  class fwd;

  friend class fwd;

  class fwd : public subscriber<T> {
  public:
    explicit fwd(intrusive_ptr<merger> parent) : parent(std::move(parent)) {
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
        parent->abort(what);
        parent = nullptr;
      }
    }

    void on_subscribe(subscription_ptr new_sub) override {
      if (!sub) {
        sub.swap(new_sub);
        sub->request(defaults::flow::buffer_size);
      } else {
        new_sub->cancel();
      }
    }

    void on_batch(const batch& buf) override {
      if (parent)
        parent->on_batch(buf, this);
    }

    void on_next(span<const T> items) override {
      if (parent)
        parent->on_batch(make_batch(items), this);
    }

    intrusive_ptr<merger> parent;
    subscription_ptr sub;
  };

  explicit merger(coordinator* ctx) : super(ctx, defaults::flow::batch_size) {
    // nop
  }

  void add(publisher_ptr<T> source) {
    auto forwarder = make_counted<fwd>(this);
    forwarders_.emplace_back(forwarder);
    source->subscribe(std::move(forwarder));
  }

  bool done() const noexcept override {
    return super::done() && inputs_.empty() && forwarders_.empty();
  }

protected:
  void abort(const error& reason) override {
    super::abort(reason);
    inputs_.clear();
    forwarders_.clear();
  }

private:
  using fwd_ptr = intrusive_ptr<fwd>;

  void pull(size_t n) override {
    while (n > 0 && !inputs_.empty()) {
      auto& input = inputs_[0];
      auto m = std::min(input.buf.size() - input.offset, n);
      CAF_ASSERT(m > 0);
      auto items = input.buf.template items<T>().subspan(input.offset, m);
      this->append_to_buf(items.begin(), items.end());
      if (auto& sub = input.src->sub)
        sub->request(m);
      if (m + input.offset == input.buf.size()) {
        inputs_.erase(inputs_.begin());
      } else {
        input.offset += m;
      }
      n -= m;
    }
  }

  void on_batch(batch buf, fwd_ptr src) {
    inputs_.emplace_back(buf, src);
  }

  void forwarder_completed(fwd* ptr) {
    auto is_ptr = [ptr](auto& x) { return x == ptr; };
    auto i = std::find_if(forwarders_.begin(), forwarders_.end(), is_ptr);
    if (i != forwarders_.end()) {
      forwarders_.erase(i);
      if (forwarders_.empty())
        this->shutdown();
    }
  }

  struct input_t {
    size_t offset = 0;

    batch buf;

    fwd_ptr src;

    input_t(batch content, fwd_ptr source)
      : buf(std::move(content)), src(std::move(source)) {
      // nop
    }
  };

  std::vector<input_t> inputs_;
  std::vector<fwd_ptr> forwarders_;
};

/// Combines the items emitted from `pub` and `pubs...` to appear as a single
/// stream of items.
template <class T, class... Publishers>
publisher_ptr<T> merge(publisher_ptr<T> pub, Publishers... pubs) {
  static_assert((std::is_same_v<T, published_type_t<Publishers>> && ...));
  auto ptr = make_counted<merger<T>>(pub->ctx());
  ptr->add(std::move(pub));
  ptr->add(std::move(pubs)...);
  return ptr;
}

} // namespace caf::flow
