// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow.for_each

#include "caf/flow/observable.hpp"

#include "core-test.hpp"

#include "caf/flow/coordinator.hpp"
#include "caf/flow/merge.hpp"
#include "caf/flow/observable_builder.hpp"
#include "caf/flow/observer.hpp"
#include "caf/scheduled_actor/flow.hpp"

using namespace caf;

namespace {

struct fixture : test_coordinator_fixture<> {
  template <class F>
  void with_actor(F body) {
    auto f = [body](event_based_actor* self) {
      body(self);
      self->handle_flow_events();
    };
    sys.spawn(f);
    sched.run();
  }
};

} // namespace

BEGIN_FIXTURE_SCOPE(fixture)

SCENARIO("for_each iterates all values in a stream") {
  GIVEN("a generation") {
    WHEN("subscribing to its output via for_each") {
      THEN("the observer receives all values") {
        /* subtest */ {
          auto inputs = std::vector<int>{1, 2, 4, 8, 16, 32, 64, 128};
          auto outputs = std::vector<int>{};
          with_actor([&outputs, &inputs](auto* self) {
            self->make_observable()
              .from_container(inputs) //
              .filter([](int) { return true; })
              .for_each([&outputs](int x) { outputs.emplace_back(x); });
          });
          CHECK_EQ(inputs, outputs);
        }
        /* subtest */ {
          auto inputs = std::vector<int>{21, 21, 21, 21, 21, 21, 21};
          auto outputs = std::vector<int>{};
          with_actor([&outputs](auto* self) {
            self->make_observable()
              .repeat(7) //
              .take(7)
              .map([](int x) { return x * 3; })
              .for_each([&outputs](int x) { outputs.emplace_back(x); });
          });
          CHECK_EQ(inputs, outputs);
        }
      }
    }
  }
  GIVEN("a transformation") {
    WHEN("subscribing to its output via for_each") {
      THEN("the observer receives all values") {
        /* subtest */ {
          auto inputs = std::vector<int>{1, 2, 4, 8, 16, 32, 64, 128};
          auto outputs = std::vector<int>{};
          with_actor([&outputs, &inputs](auto* self) {
            self->make_observable()
              .from_container(inputs) //
              .as_observable()
              .filter([](int) { return true; })
              .for_each([&outputs](int x) { outputs.emplace_back(x); });
          });
          CHECK_EQ(inputs, outputs);
        }
        /* subtest */ {
          auto inputs = std::vector<int>{21, 21, 21, 21, 21, 21, 21};
          auto outputs = std::vector<int>{};
          with_actor([&outputs](auto* self) {
            self->make_observable()
              .repeat(7) //
              .as_observable()
              .take(7)
              .map([](int x) { return x * 3; })
              .for_each([&outputs](int x) { outputs.emplace_back(x); });
          });
          CHECK_EQ(inputs, outputs);
        }
      }
    }
  }
  GIVEN("an observable") {
    WHEN("subscribing to its output via for_each") {
      THEN("the observer receives all values") {
        /* subtest */ {
          auto inputs = std::vector<int>{1, 2, 4, 8, 16, 32, 64, 128};
          auto outputs = std::vector<int>{};
          with_actor([&outputs, &inputs](auto* self) {
            self->make_observable()
              .from_container(inputs) //
              .filter([](int) { return true; })
              .as_observable()
              .for_each([&outputs](int x) { outputs.emplace_back(x); });
          });
          CHECK_EQ(inputs, outputs);
        }
        /* subtest */ {
          auto inputs = std::vector<int>{21, 21, 21, 21, 21, 21, 21};
          auto outputs = std::vector<int>{};
          with_actor([&outputs](auto* self) {
            self->make_observable()
              .repeat(7) //
              .take(7)
              .map([](int x) { return x * 3; })
              .as_observable()
              .for_each([&outputs](int x) { outputs.emplace_back(x); });
          });
          CHECK_EQ(inputs, outputs);
        }
      }
    }
  }
}

END_FIXTURE_SCOPE()
