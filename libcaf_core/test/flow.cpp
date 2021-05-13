// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow

#include "caf/test/bdd_dsl.hpp"

#include "caf/flow/async.hpp"
#include "caf/flow/batch.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/merge.hpp"
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

class buffered_subscriber : public flow::subscriber<int> {
public:
  std::vector<int> buf;

  buffered_subscriber() : limit_(std::numeric_limits<size_t>::max()) {
    // nop
  }

  explicit buffered_subscriber(size_t limit) : limit_(limit) {
    // nop
  }

  void on_complete() {
    completed_ = true;
  }

  void on_error(const error&) {
    // nop
  }

  void on_next(span<const int> items) {
    buf.insert(buf.end(), items.begin(), items.end());
    if (buf.size() == limit_)
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
  size_t limit_;
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

SCENARIO("users can feed ranges into streams") {
  GIVEN("a range adapter") {
    WHEN("subscribing to its output asynchronously") {
      THEN("the subscriber receives all values from the range") {
        auto values = std::vector<int>{1, 2, 4, 8, 16, 32, 64, 128};
        auto consumer = make_counted<buffered_subscriber>();
        flow::async::from(sys, [vals{values}](auto* self) mutable {
          return self->make_publisher()->iterate(std::move(vals));
        })->async_subscribe(consumer);
        run();
        CHECK_EQ(consumer->completed(), true);
        CHECK_EQ(consumer->buf, values);
      }
    }
    WHEN("subscribing to its output on another actor") {
      THEN("the subscriber receives all values from the range") {
        auto values = std::vector<int>{1, 2, 4, 8, 16, 32, 64, 128};
        auto consumer = make_counted<buffered_subscriber>();
        flow::async::from(sys, [vals{values}](auto* self) mutable {
          return self->make_publisher()->iterate(std::move(vals));
        })->subscribe_with(sys, [consumer](auto*, auto&& in) {
          in->subscribe(consumer);
        });
        run();
        CHECK_EQ(consumer->completed(), true);
        CHECK_EQ(consumer->buf, values);
      }
    }
  }
}

SCENARIO("repeaters generate a sequence of identical values") {
  GIVEN("a repeater") {
    WHEN("subscribing to its output") {
      THEN("the subscriber receives the same value ad infinitum") {
        auto consumer = make_counted<buffered_subscriber>(128);
        flow::async::from(sys, [](auto* self) {
          return self->make_publisher()->repeat(42);
        })->async_subscribe(consumer);
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
        auto consumer = make_counted<buffered_subscriber>();
        flow::async::from(sys, [](auto* self) {
          return self->make_publisher()->repeat(42)->take(17);
        })->async_subscribe(consumer);
        run();
        CHECK_EQ(consumer->completed(), true);
        CHECK_EQ(consumer->buf.size(), 17u);
        CHECK(std::all_of(consumer->buf.begin(), consumer->buf.end(),
                          [](int x) { return x == 42; }));
      }
    }
  }
}

SCENARIO("merge operators combine inputs") {
  GIVEN("two publishers") {
    WHEN("merging them to a single publisher") {
      THEN("the subscriber receives the output of both sources") {
        auto consumer = make_counted<buffered_subscriber>();
        flow::async::from(sys, [](auto* self) {
          auto r1 = self->make_publisher()->repeat(11)->take(113);
          auto r2 = self->make_publisher()->repeat(22)->take(223);
          return flow::merge(r1, r2);
        })->async_subscribe(consumer);
        run();
        CHECK_EQ(consumer->completed(), true);
        auto& buf = consumer->buf;
        if (CHECK_EQ(buf.size(), 336u)) {
          std::sort(buf.begin(), buf.end());
          CHECK(std::all_of(buf.begin(), buf.begin() + 113,
                            [](int x) { return x == 11; }));
          CHECK(std::all_of(buf.begin() + 113, buf.end(),
                            [](int x) { return x == 22; }));
        }
      }
    }
  }
}

SCENARIO("broadcasters simply copy their inputs to all subscribers") {
  GIVEN("a repeater and a broadcaster") {
    WHEN("subscribing to the broadcaster asynchronously") {
      THEN("the subscriber receives the output of the repeater") {
        auto consumer = make_counted<buffered_subscriber>();
        flow::async::from(sys, [](auto* self) {
          return self->make_publisher()
            ->repeat(42)
            ->take(128)
            ->template subscribe_with_new<flow::broadcaster<int>>(self);
        })->async_subscribe(consumer);
        run();
        CHECK_EQ(consumer->completed(), true);
        auto& buf = consumer->buf;
        auto is_42 = [](int x) { return x == 42; };
        CHECK_EQ(buf.size(), 128u);
        CHECK(std::all_of(buf.begin(), buf.end(), is_42));
      }
    }
    WHEN("subscribing to the broadcaster on another actor") {
      THEN("the subscriber receives the output of the repeater") {
        auto consumer = make_counted<buffered_subscriber>();
        flow::async::from(sys, [](auto* self) {
          return self->make_publisher()
            ->repeat(42)
            ->take(128)
            ->template subscribe_with_new<flow::broadcaster<int>>(self);
        })->subscribe_with(sys, [consumer](auto*, auto&& in) {
          in->subscribe(consumer);
        });
        run();
        CHECK_EQ(consumer->completed(), true);
        auto& buf = consumer->buf;
        auto is_42 = [](int x) { return x == 42; };
        CHECK_EQ(buf.size(), 128u);
        CHECK(std::all_of(buf.begin(), buf.end(), is_42));
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
