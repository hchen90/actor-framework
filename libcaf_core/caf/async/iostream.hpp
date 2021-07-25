/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright 2011-2021 Dominik Charousset                                     *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#pragma once

#include "caf/async/blocking_observer.hpp"
#include "caf/async/observer_buffer.hpp"
#include "caf/error.hpp"
#include "caf/flow/observable.hpp"
#include "caf/flow/observer.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/ref_counted.hpp"
#include "caf/scheduled_actor.hpp"
#include "caf/sec.hpp"

#include <iostream>

namespace caf::async {

template <class ReadFn>
using cin_line_reader_res_t
  = decltype(std::declval<ReadFn&>()(std::declval<const std::string&>()));

template <class ReadFn>
class cin_line_reader_impl
  : public buffered_observable_impl<cin_line_reader_res_t<ReadFn>> {
public:
  using super = buffered_observable_impl<cin_line_reader_res_t<ReadFn>>;

  cin_line_reader_impl(flow::coordinator* ctx, ReaderFn fn)
    : super(ctx), fn_(std::move(fn)) {
    // nop
  }

  void pull(size_t n) override {
    while (n > 0) {
      if (std::getline(std::cin, buf_)) {
        this->append_to_buf(fn_(buf_));
        --n;
      } else {
        this->completed_ = true;
        return;
      }
    }
  }

private:
  std::string buf_;
  ReadFn fn_;
};

template <class ReadFn>
auto cin_line_reader(actor_system& sys, ReadFn read_fn) {
  using res_t = cin_line_reader_res_t<ReadFn>;
  using actor_t = event_based_actor;
  using impl = cin_line_reader_impl<ReadFn>;
  auto [ptr, launch] = sys.make_flow_coordinator<actor_t, detached>();
  auto obs = ptr->to_async_publisher(
    make_counted<impl>(ptr, std::move(read_fn))->as_observable());
  launch();
  return obs;
}

} // namespace caf::async
