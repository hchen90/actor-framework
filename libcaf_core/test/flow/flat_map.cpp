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

SCENARIO("flat_map merges multiple observables") {
  GIVEN("a generation that emits lists") {
    WHEN("lifting each list to an observable with flat_map") {
      THEN("the observer receives values from all observables") {
        auto outputs = std::vector<int>{};
        with_actor([&outputs](auto* self) {
          auto inputs = std::vector<std::vector<int>>{
            std::vector<int>{1},
            std::vector<int>{2, 2},
            std::vector<int>{3, 3, 3},
          };
          self->make_observable()
            .from_container(inputs) //
            .flat_map([self](const std::vector<int>& x) {
              return self->make_observable().from_container(x);
            })
            .for_each([&outputs](int x) { outputs.emplace_back(x); });
        });
        std::sort(outputs.begin(), outputs.end());
        auto expected_outputs = std::vector<int>{1, 2, 2, 3, 3, 3};
        CHECK_EQ(outputs, expected_outputs);
      }
    }
  }
}

END_FIXTURE_SCOPE()
