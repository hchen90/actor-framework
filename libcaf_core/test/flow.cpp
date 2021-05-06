// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow

#include "caf/scheduled_actor/flow.hpp"
#include "caf/flow/batch.hpp"
#include "caf/flow/coordinated.hpp"
#include "caf/flow/coordinator.hpp"
#include "caf/flow/publisher.hpp"
#include "caf/flow/subscriber.hpp"

#include "caf/test/bdd_dsl.hpp"

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

} // namespace

BEGIN_FIXTURE_SCOPE(fixture)

SCENARIO("repeaters generate a sequence of identical values") {
  GIVEN("a repeater") {
    WHEN("subscribing to its output") {
      THEN("the subscriber receives the same value ad infinitum") {
        auto consumer = make_counted<take128>();
        flow::from(sys, [](auto* self) {
          return flow::repeat(self, 42);
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
        flow::from(sys, [](auto* self) {
          return flow::repeat(self, 42)->take(17);
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
