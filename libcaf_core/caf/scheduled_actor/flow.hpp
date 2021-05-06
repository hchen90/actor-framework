// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/actor.hpp"
#include "caf/detail/unsafe_flow_msg.hpp"
#include "caf/flow/batch.hpp"
#include "caf/flow/coordinated.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/disposable.hpp"
#include "caf/flow/publisher.hpp"
#include "caf/flow/subscriber.hpp"
#include "caf/scheduled_actor.hpp"

namespace caf::detail {

template <class T>
class publisher_proxy : public flow::publisher<T> {
public:
  class subscription_impl : public flow::subscription {
  public:
    subscription_impl(actor hdl, flow::subscription_ptr decorated)
      : hdl_(std::move(hdl)), decorated_(std::move(decorated)) {
      // nop
    }

    ~subscription_impl() {
      reset();
    }

    void cancel() override {
      reset();
    }

    void request(size_t n) override {
      anon_send(hdl_, unsafe_flow_msg{decorated_, n});
    }

  private:
    void reset() {
      if (hdl_) {
        anon_send(hdl_, unsafe_flow_msg{decorated_});
        hdl_ = nullptr;
        decorated_ = nullptr;
      }
    }

    actor hdl_;
    flow::subscription_ptr decorated_;
  };

  class forwarder_impl : public flow::subscriber<T> {
  public:
    forwarder_impl(actor hdl, flow::subscriber_ptr<T> sink)
      : hdl_(std::move(hdl)), sink_(std::move(sink)) {
      // nop
    }

    ~forwarder_impl() {
      if (sink_) {
        if (!sub_) {
          // Actor dropped the subscription message or terminated.
          sink_->on_error(make_error(sec::cannot_add_upstream));
        } else {
          // Actor failed to call on_subscribe() on the subscription.
          sink_->on_error(make_error(sec::invalid_stream_state));
        }
      }
    }

    void on_complete() {
      sink_->on_complete();
      reset();
    }

    void on_error(const error& what) {
      sink_->on_error(what);
      reset();
    }

    void on_subscribe(flow::subscription_ptr sub) {
      if (auto dptr = static_cast<flow::coordinated_subscription*>(sub.get())) {
        sub_.reset(dptr);
        sink_->on_subscribe(make_counted<subscription_impl>(hdl_, sub_));
      } else {
        sub->cancel();
        sink_->on_error(make_error(sec::invalid_stream_state));
        reset();
      }
    }

    void on_next(span<const T> items) {
      sink_->on_next(items);
    }

  private:
    void reset() {
      hdl_ = nullptr;
      sink_ = nullptr;
      sub_ = nullptr;
    }

    actor hdl_;
    flow::subscriber_ptr<T> sink_;
    flow::coordinated_subscription_ptr sub_;
  };

  publisher_proxy(actor hdl, flow::coordinated_publisher_ptr<T> src)
    : hdl_(std::move(hdl)), src_(std::move(src)) {
    // nop
  }

  void subscribe(flow::subscriber_ptr<T> sink) override {
    auto fwd = make_counted<forwarder_impl>(hdl_, std::move(sink));
    anon_send(hdl_, unsafe_flow_msg{src_, flow::subscriber_ptr<T>{fwd}});
  }

private:
  actor hdl_;
  flow::coordinated_publisher_ptr<T> src_;
};

/// Fetches items from a foreign publisher and forwards then to a coordinated
/// publisher of an actor.
template <class T>
class item_forwarder : public flow::subscriber<T> {
public:
  explicit item_forwarder(actor hdl, flow::subscriber_ptr<T> target)
    : hdl_(std::move(hdl)), target_(std::move(target)) {
    // nop
  }

  // -- called from outside the actor ------------------------------------------

  void on_complete() override {
    anon_send(hdl_, unsafe_flow_msg{target_});
  }

  void on_error(const error& what) override {
    anon_send(hdl_, unsafe_flow_msg{target_, what});
  }

  void on_next(span<const T> items) override {
    anon_send(hdl_, unsafe_flow_msg{target_, flow::make_batch(items)});
  }

  void on_subscribe(flow::subscription_ptr sub) override {
    anon_send(hdl_, unsafe_flow_msg{target_, std::move(sub)});
  }

private:
  actor hdl_;
  flow::subscriber_ptr<T> target_;
};

} // namespace caf::detail

namespace caf {

template <class T>
flow::coordinated_publisher_ptr<T>
scheduled_actor::observe(flow::publisher_ptr<T> source) {
  // Internally, we have a broadcaster called `local` that makes its inputs
  // available to coordinated subscribers. The forwarder simply converts all
  // function calls from the source to messages. After receiving the messages,
  // the actor then calls the appropriate member functions on `local`, including
  // on_subscribe().
  auto local = make_counted<flow::coordinated_broadcaster<T>>(this);
  watch(local.get());
  auto hdl = actor_cast<actor>(this);
  auto fwd = make_counted<detail::item_forwarder<T>>(std::move(hdl), local);
  source->subscribe(flow::subscriber_ptr<T>{std::move(fwd)});
  return local;
}

template <class T>
flow::publisher_ptr<T>
scheduled_actor::lift(flow::coordinated_publisher_ptr<T> source) {
  watch(source.get());
  return make_counted<detail::publisher_proxy<T>>(actor_cast<actor>(this),
                                                  std::move(source));
}

} // namespace caf
