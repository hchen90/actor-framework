// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE response_handle

#include "caf/response_handle.hpp"

#include "core-test.hpp"

#include "caf/scheduled_actor/flow.hpp"

using namespace caf;

namespace {

struct dummy_state {
  static inline const char* name = "dummy";

  caf::behavior make_behavior() {
    return {
      [](int32_t x) -> result<int32_t> {
        if (x % 2 == 0)
          return x + x;
        else
          return error{sec::invalid_argument};
      },
    };
  }
};

using dummy_actor = stateful_actor<dummy_state>;

struct fixture : test_coordinator_fixture<> {
  actor dummy;
  actor aut;

  fixture() {
    dummy = sys.spawn<dummy_actor>();
    sched.run();
  }

  template <class F>
  void with_actor(F body) {
    auto f = [body](event_based_actor* self) {
      body(self);
      self->handle_flow_events();
    };
    aut = sys.spawn(f);
    sched.run_once();
  }
};

} // namespace

BEGIN_FIXTURE_SCOPE(fixture)

SCENARIO("response handles are convertible to singles") {
  GIVEN("a response handle") {
    WHEN("calling as_single") {
      THEN("observers can subscribe to the result") {
        using result_t = std::variant<none_t, int32_t, caf::error>;
        result_t result;
        with_actor([this, &result](event_based_actor* self) {
          self->request(dummy, infinite, int32_t{42})
            .as_single<int32_t>()
            .attach([&result](int32_t val) { result = val; },
                    [&result](const error& what) { result = what; });
        });
        expect((int32_t), from(aut).to(dummy).with(42));
        expect((int32_t), from(dummy).to(aut).with(84));
        CHECK_EQ(result, result_t{84});
      }
    }
  }
}

END_FIXTURE_SCOPE()
