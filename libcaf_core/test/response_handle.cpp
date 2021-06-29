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
          return make_error(sec::invalid_argument);
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

SCENARIO("response handles are convertible to observables and singles") {
  GIVEN("a response handle that produces a valid result") {
    WHEN("calling as_single") {
      THEN("observers see the result") {
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
        CHECK(!sched.has_job());
        CHECK_EQ(result, result_t{84});
      }
    }
    WHEN("calling as_observable") {
      THEN("observers see the result") {
        using result_t = std::variant<none_t, int32_t, caf::error>;
        size_t on_next_calls = 0;
        bool completed = false;
        result_t result;
        with_actor([&, this](event_based_actor* self) {
          self->request(dummy, infinite, int32_t{42})
            .as_observable<int32_t>()
            .for_each(
              [&](int32_t val) {
                result = val;
                ++on_next_calls;
              },
              [&](const error& what) { result = what; },
              [&] { completed = true; });
        });
        expect((int32_t), from(aut).to(dummy).with(42));
        expect((int32_t), from(dummy).to(aut).with(84));
        CHECK(!sched.has_job());
        CHECK_EQ(result, result_t{84});
        CHECK_EQ(on_next_calls, 1u);
        CHECK(completed);
      }
    }
  }
  GIVEN("a response handle that produces an error") {
    WHEN("calling as_single") {
      THEN("observers see an error") {
        using result_t = std::variant<none_t, int32_t, caf::error>;
        result_t result;
        with_actor([this, &result](event_based_actor* self) {
          self->request(dummy, infinite, int32_t{13})
            .as_single<int32_t>()
            .attach([&result](int32_t val) { result = val; },
                    [&result](const error& what) { result = what; });
        });
        expect((int32_t), from(aut).to(dummy).with(13));
        expect((error), from(dummy).to(aut));
        CHECK(!sched.has_job());
        CHECK_EQ(result, result_t{make_error(sec::invalid_argument)});
      }
    }
    WHEN("calling as_observable") {
      THEN("observers see an error") {
        using result_t = std::variant<none_t, int32_t, caf::error>;
        size_t on_next_calls = 0;
        bool completed = false;
        result_t result;
        with_actor([&, this](event_based_actor* self) {
          self->request(dummy, infinite, int32_t{13})
            .as_observable<int32_t>()
            .for_each(
              [&](int32_t val) {
                result = val;
                ++on_next_calls;
              },
              [&](const error& what) { result = what; },
              [&] { completed = true; });
        });
        expect((int32_t), from(aut).to(dummy).with(13));
        expect((error), from(dummy).to(aut));
        CHECK(!sched.has_job());
        CHECK_EQ(result, result_t{make_error(sec::invalid_argument)});
        CHECK_EQ(on_next_calls, 0u);
        CHECK(!completed);
      }
    }
  }
}

END_FIXTURE_SCOPE()
