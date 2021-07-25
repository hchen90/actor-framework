// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow.observe_on

#include "caf/flow/observable.hpp"

#include "core-test.hpp"

#include "caf/flow/coordinator.hpp"
#include "caf/flow/merge.hpp"
#include "caf/flow/observable_builder.hpp"
#include "caf/flow/observer.hpp"
#include "caf/scheduled_actor/flow.hpp"

using namespace caf;

namespace {

template <class T>
class bridge : public flow::observable<T>::impl,
               public flow::observer<T>::impl {
public:
  template <class ProducerPtr, class CoordinatorPtr>
  bridge(ProducerPtr&& producer_ctx, CoordinatorPtr&& consumer_ctx)
    : producer_ctx_(std::forward<ProducerPtr>(producer_ctx)),
      consumer_ctx_(std::forward<CoordinatorPtr>(consumer_ctx)) {
    memset(&flags_, 0, sizeof(flags_));
  }

  // -- called by producer and consumer ----------------------------------------

  void dispose() {
  }

  bool disposed() const noexcept {
  }

  // -- called by the producer -------------------------------------------------

  void on_next(span<const T> items) override {
    auto wakeup_consumer = buf_.empty();
    buf_.insert(buf_.end(), items.begin(), items.end());
    if (wakeup_consumer)
      at_consumer([thisptr{strong_this()}] { thisptr->pull(); });
  }

  void on_complete() override {
    at_consumer([thisptr{strong_this()}] {
      if (auto& dst = thisptr->dst_) {
        dst.on_complete();
        dst = nullptr;
      }
    });
  }

  void on_error(const error& what) override {
    at_consumer([thisptr{strong_this()}, what] {
      if (auto& dst = thisptr->dst_) {
        dst.on_error(what);
        dst = nullptr;
      }
    });
  }

  void on_attach(flow::subscription sub) override {
    if (!sub_) {
      sub_ = std::move(sub);
      sub_.request(defaults::flow::buffer_size);
    } else {
      sub.cancel();
    }
  }

  void drop() {
    sub_ = nullptr;
  }

  // -- called by the consumer -------------------------------------------------

  void on_request(flow::observer_base*, size_t n) override {
    demand_ += n;
    pull();
  }

  void on_cancel(flow::observer_base*) override {
    if (producer_ctx_) {
      producer_ctx_->schedule_fn([thisptr{strong_this()}] { thisptr->drop(); });
      producer_ctx_.reset();
    }
  }

  void attach(flow::observer<T> what) override {
    if (!dst_) {
      dst_ = std::move(what);
    } else {
      auto err = make_error(sec::cannot_add_downstream,
                            "bridges allow only a single observer");
      what.on_error(err);
    }
  }

  void pull() {
    if (pulling_ || !dst_)
      return;
    if (auto n = std::min(buf_.size(), demand_); n > 0) {
      pulling_ = true;
      auto items = make_span(buf_.data(), n);
      dst_.on_next(items);
      buf_.erase(buf_.begin(), buf_.begin() + n);
      pulling_ = false;
    }
  }

private:
  // -- utility functions ------------------------------------------------------

  intrusive_ptr<bridge> strong_this() const noexcept {
    return intrusive_ptr<bridge>{this};
  }

  // -- shared state -----------------------------------------------------------

  // should be thread-safe
  std::vector<T> buf_;

  // -- state for the producer -------------------------------------------------

  flow::coordinator_ptr consumer_ctx_;

  template <class F>
  void at_consumer(F fn) {
    if (consumer_ctx_)
      consumer_ctx_->schedule_fn(std::move(fn));
  }

  // -- state for the consumer -------------------------------------------------

  flow::coordinator_ptr producer_ctx_;

  /// Stores a pointer to the target observer running on `remote_ctx_`.
  flow::observer<T> dst_;

  flow::subscription sub_;
  struct {
    bool completed : 1;
    bool pulling : 1;
  } flags_;
  size_t demand_ = 0;

  template <class F>
  void at_producer(F fn) {
    if (producer_ctx_)
      producer_ctx_->schedule_fn(std::move(fn));
  }
};

} // namespace

BEGIN_FIXTURE_SCOPE(test_coordinator_fixture<>)

SCENARIO("observe_on moves data between actors") {
  GIVEN("a generation") {
    WHEN("calling observe_on") {
      THEN("the target actor observes all values") {
        /* subtest */ {
          using actor_t = event_based_actor;
          auto inputs = std::vector<int>{1, 2, 4, 8, 16, 32, 64, 128};
          auto outputs = std::vector<int>{};
          auto [actor_a, launch_a] = sys.spawn_inactive<actor_t>();
          auto [actor_b, launch_b] = sys.spawn_inactive<actor_t>();
          auto bptr = make_counted<bridge<int>>(actor_a, actor_b);
          actor_a->make_observable()
            .from_container(inputs)
            .filter([](int) {
                    MESSAGE("FILTER");
                    return true; })
            .as_observable()
            // .subscribe_on(actor_b)
            .for_each([&outputs](int x) { outputs.emplace_back(x); });
          launch_a();
          launch_b();
          run();
          CHECK_EQ(inputs, outputs);
        }
      }
    }
  }
}

END_FIXTURE_SCOPE()
