// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

namespace caf::flow {

/// A stage in the streaming pipeline that acts both as a subscriber and a
/// publisher.
template <class In, class Out>
class processor : public subscriber<In>, public publisher<Out> {
public:
};

/// @relates processor
template <class In, class Out>
using processor_ptr = intrusive_ptr<processor_ptr<In, Out>>;

} // namespace caf::flow
