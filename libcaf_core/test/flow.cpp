// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow

#include "caf/test/bdd_dsl.hpp"

#include "caf/flow/async.hpp"
#include "caf/flow/batch.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/poll_subscriber.hpp"
#include "caf/flow/publisher.hpp"
#include "caf/flow/subscriber.hpp"
#include "caf/make_counted.hpp"
#include "caf/scheduled_actor/flow.hpp"

#include <condition_variable>
#include <list>
#include <mutex>

using namespace caf;

namespace {

class take128 : public flow::subscriber<int> {
public:
  std::vector<int> buf;

  void on_complete() {
    completed_ = true;
  }

  void on_error(const error&) {
    // nop
  }

  void on_next(span<const int> items) {
    buf.insert(buf.end(), items.begin(), items.end());
    if (buf.size() == 128)
      sub_->cancel();
    else if (buf.size() % 32 == 0)
      sub_->request(32);
  }

  void on_subscribe(flow::subscription_ptr source) {
    sub_ = std::move(source);
    sub_->request(32);
  }

  bool completed() const noexcept {
    return completed_;
  }

private:
  bool completed_ = false;
  flow::subscription_ptr sub_;
};

struct fixture : test_coordinator_fixture<> {
  fixture() {
    // nop
  }
};

struct nondeterministic_fixture {
  actor_system_config cfg;
  actor_system sys;

  nondeterministic_fixture() : sys(cfg.set("caf.scheduler.max-threads", 2)) {
    // nop
  }
};

} // namespace

BEGIN_FIXTURE_SCOPE(fixture)

SCENARIO("repeaters generate a sequence of identical values") {
  GIVEN("a repeater") {
    WHEN("subscribing to its output") {
      THEN("the subscriber receives the same value ad infinitum") {
        auto consumer = make_counted<take128>();
        flow::async::from(sys, [](auto* self) {
          return self->make_publisher()->repeat(42);
        })->subscribe(consumer);
        run();
        CHECK_EQ(consumer->completed(), false);
        CHECK_EQ(consumer->buf.size(), 128u);
        CHECK(std::all_of(consumer->buf.begin(), consumer->buf.end(),
                          [](int x) { return x == 42; }));
      }
    }
  }
}

SCENARIO("take operators cut off streams") {
  GIVEN("a take operator applied to a repeater") {
    WHEN("subscribing to the output") {
      THEN("the subscriber receives a fixed number of values") {
        auto consumer = make_counted<take128>();
        flow::async::from(sys, [](auto* self) {
          return self->make_publisher()->repeat(42)->take(17);
        })->subscribe(consumer);
        run();
        CHECK_EQ(consumer->completed(), true);
        CHECK_EQ(consumer->buf.size(), 17u);
        CHECK(std::all_of(consumer->buf.begin(), consumer->buf.end(),
                          [](int x) { return x == 42; }));
      }
    }
  }
}

END_FIXTURE_SCOPE()

BEGIN_FIXTURE_SCOPE(nondeterministic_fixture)

SCENARIO("async::publisher::for_each receives values in the current thread") {
  GIVEN("a repeater with no limit running on an actor") {
    auto source = flow::async::from(sys, [](auto* self) {
      return self->make_publisher()->repeat(42);
    });
    WHEN("subscribing to its output via for_each_while") {
      THEN("the caller gets blocked until the cancel predicate returns true") {
        int n = 0;
        int sum = 0;
        source->for_each_while(
          // OnNext.
          [&](int x) {
            sum += x;
            return ++n < 1024;
          },
          // OnError.
          [](const error&) { FAIL("OnError called"); },
          // OnComplete
          [] { MESSAGE("OnComplete called"); });
        CHECK_EQ(n, 1024);
        CHECK_EQ(n * 42, sum);
      }
    }
  }
  GIVEN("a repeater generating 512 values running on an actor") {
    auto source = flow::async::from(sys, [](auto* self) {
      return self->make_publisher()->repeat(42)->take(512);
    });
    WHEN("subscribing to its output via for_each") {
      THEN("the caller gets blocked until all values arrived") {
        int n = 0;
        int sum = 0;
        source->for_each(
          // OnNext.
          [&](int x) {
            sum += x;
            ++n;
          },
          // OnError.
          [](const error&) { FAIL("OnError called"); },
          // OnComplete
          [] { MESSAGE("OnComplete called"); });
        CHECK_EQ(n, 512);
        CHECK_EQ(n * 42, sum);
      }
    }
  }
}

END_FIXTURE_SCOPE()
