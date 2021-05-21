// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <caf/fwd.hpp>

namespace caf::async {

class batch;

template <class T>
batch make_batch(span<const T> items);

class notifiable;

template <class T>
class publisher;

template <class T>
class blocking_publisher;

} // namespace caf::async
