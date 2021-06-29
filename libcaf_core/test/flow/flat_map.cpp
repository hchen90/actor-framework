// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow.flat_map

#include "caf/flow/observable.hpp"

#include "core-test.hpp"

#include "caf/flow/coordinator.hpp"
#include "caf/flow/merge.hpp"
#include "caf/flow/observable_builder.hpp"
#include "caf/flow/observer.hpp"
#include "caf/scheduled_actor/flow.hpp"

using namespace caf;

namespace {

struct adder_state {
  static inline const char* name = "adder";

  explicit adder_state(int32_t x) : x(x) {
    // nop
  }

  caf::behavior make_behavior() {
    return {
      [this](int32_t y) { return x + y; },
    };
  }

  int32_t x;
};

using adder_actor = stateful_actor<adder_state>;

struct fixture : test_coordinator_fixture<> {
  actor adder;

  fixture() {
    adder = sys.spawn<adder_actor>(1);
  }

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

SCENARIO("flat_map merges multiple observables") {
  using i32_list = std::vector<int32_t>;
  GIVEN("a generation that emits lists") {
    WHEN("lifting each list to an observable with flat_map") {
      THEN("the observer receives values from all observables") {
        auto outputs = i32_list{};
        with_actor([&outputs](auto* self) {
          auto inputs = std::vector<i32_list>{
            i32_list{1},
            i32_list{2, 2},
            i32_list{3, 3, 3},
          };
          self->make_observable()
            .from_container(inputs)
            .flat_map([self](const i32_list& x) {
              return self->make_observable().from_container(x);
            })
            .for_each([&outputs](int32_t x) { outputs.emplace_back(x); });
        });
        std::sort(outputs.begin(), outputs.end());
        auto expected_outputs = i32_list{1, 2, 2, 3, 3, 3};
        CHECK_EQ(outputs, expected_outputs);
      }
    }
  }
  GIVEN("a generation that emits 10 integers") {
    WHEN("sending a request for each each integer") {
      THEN("flat_map merges the responses") {
        auto outputs = i32_list{};
        with_actor([this, &outputs](event_based_actor* self) {
          auto inputs = i32_list(10);
          std::iota(inputs.begin(), inputs.end(), 0);
          self->make_observable()
            .from_container(inputs)
            .flat_map([self, add1{adder}](int32_t x) {
              return self->request(add1, infinite, x).as_observable<int32_t>();
            })
            .for_each([&outputs](int32_t x) { outputs.emplace_back(x); });
        });
        std::sort(outputs.begin(), outputs.end());
        auto expected_outputs = i32_list{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        CHECK_EQ(outputs, expected_outputs);
      }
    }
  }
}

END_FIXTURE_SCOPE()
