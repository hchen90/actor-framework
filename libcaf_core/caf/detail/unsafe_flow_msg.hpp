// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <variant>

#include "caf/allowed_unsafe_message_type.hpp"
#include "caf/async/batch.hpp"
#include "caf/async/notifiable.hpp"
#include "caf/detail/core_export.hpp"
#include "caf/error.hpp"
#include "caf/flow/observable_base.hpp"
#include "caf/flow/observer_base.hpp"
#include "caf/flow/subscription.hpp"

namespace caf::detail {

class CAF_CORE_EXPORT unsafe_flow_msg {
public:
  template <class T>
  static void
  attach_impl(flow::observable_base* source, flow::observer_base* sink) {
    auto src_dptr = static_cast<typename flow::observable<T>::impl*>(source);
    auto sink_dptr = static_cast<typename flow::observer<T>::impl*>(sink);
    src_dptr->attach(flow::observer<T>{sink_dptr});
  }

  using attach_fn = void (*)(flow::observable_base*, flow::observer_base*);

  struct nop {
    void exec() {
      // nop
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg() : event(nop{}) {
    // nop
  }

  struct on_batch {
    flow::observer_base_ptr sink;
    async::batch content;
    void exec() {
      sink->on_batch(content);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::observer_base_ptr sink, async::batch content)
    : event(on_batch{std::move(sink), std::move(content)}) {
    // nop
  }

  struct on_attach {
    flow::observer_base_ptr sink;
    flow::subscription sub;
    void exec() {
      sink->on_attach(sub);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::observer_base_ptr sink, flow::subscription sub)
    : event(on_attach{std::move(sink), std::move(sub)}) {
    // nop
  }

  struct on_complete {
    flow::observer_base_ptr sink;
    void exec() {
      sink->on_complete();
    }
    void render(std::string& str) const;
  };

  explicit unsafe_flow_msg(flow::observer_base_ptr sink)
    : event(on_complete{std::move(sink)}) {
    // nop
  }

  struct on_error {
    flow::observer_base_ptr sink;
    error what;
    void exec() {
      sink->on_error(what);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::observer_base_ptr sink, error what)
    : event(on_error{std::move(sink), std::move(what)}) {
    // nop
  }

  struct attach {
    flow::observable_base_ptr source;
    flow::observer_base_ptr sink;
    attach_fn fn;
    void exec() {
      fn(source.get(), sink.get());
    }
    void render(std::string& str) const;
  };

  template <class T>
  unsafe_flow_msg(flow::observable<T> source, flow::observer<T> sink)
    : event(attach{std::move(source).as_intrusive_ptr(),
                   std::move(sink).as_intrusive_ptr(), attach_impl<T>}) {
    // nop
  }

  struct request {
    flow::subscription sub;
    size_t demand;
    void exec() {
      sub.request(demand);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::subscription sub, size_t demand)
    : event(request{std::move(sub), demand}) {
    // nop
  }

  struct cancel {
    flow::subscription sub;
    void exec() {
      sub.cancel();
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::subscription sub) : event(cancel{std::move(sub)}) {
    // nop
  }

  struct on_event {
    async::notifiable::listener_ptr ptr;
    void exec() {
      ptr->on_event();
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(async::notifiable::listener_ptr ptr)
    : event(on_event{std::move(ptr)}) {
    // nop
  }

  struct on_close {
    async::notifiable::listener_ptr ptr;
    void exec() {
      ptr->on_close();
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(close_atom, async::notifiable::listener_ptr ptr)
    : event(on_close{std::move(ptr)}) {
    // nop
  }

  struct on_abort {
    async::notifiable::listener_ptr ptr;
    error reason;
    void exec() {
      ptr->on_abort(reason);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(async::notifiable::listener_ptr ptr, error reason)
    : event(on_abort{std::move(ptr), std::move(reason)}) {
    // nop
  }

  unsafe_flow_msg(const unsafe_flow_msg&) = default;
  unsafe_flow_msg& operator=(const unsafe_flow_msg&) = default;

  using event_type
    = std::variant<nop, on_batch, on_attach, on_complete, on_error, attach,
                   request, cancel, on_event, on_close, on_abort>;

  event_type event;

  void exec() {
    std::visit([](auto& ev) { ev.exec(); }, event);
  }
};

CAF_CORE_EXPORT std::string to_string(const unsafe_flow_msg& msg);

} // namespace caf::detail

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(caf::detail::unsafe_flow_msg)
