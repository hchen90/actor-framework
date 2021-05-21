// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE flow.merge

#include "caf/flow/merge.hpp"

#include "core-test.hpp"

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

SCENARIO("merge operators combine inputs") {
  GIVEN("two observables") {
    WHEN("merging them to a single publisher") {
      THEN("the observer receives the output of both sources") {
        auto outputs = std::vector<int>{};
        with_actor([&outputs](auto* self) {
          auto r1 = self->make_observable().repeat(11).take(113);
          auto r2 = self->make_observable().repeat(22).take(223);
          flow::merge(std::move(r1), std::move(r2)).for_each([&outputs](int x) {
            outputs.emplace_back(x);
          });
        });
        if (CHECK_EQ(outputs.size(), 336u)) {
          std::sort(outputs.begin(), outputs.end());
          CHECK(std::all_of(outputs.begin(), outputs.begin() + 113,
                            [](int x) { return x == 11; }));
          CHECK(std::all_of(outputs.begin() + 113, outputs.end(),
                            [](int x) { return x == 22; }));
        }
      }
    }
  }
}

END_FIXTURE_SCOPE()
