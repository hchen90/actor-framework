// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <variant>

#include "caf/allowed_unsafe_message_type.hpp"
#include "caf/detail/core_export.hpp"
#include "caf/error.hpp"
#include "caf/flow/batch.hpp"
#include "caf/flow/notifiable.hpp"
#include "caf/flow/publisher_base.hpp"
#include "caf/flow/subscriber_base.hpp"
#include "caf/flow/subscription.hpp"

namespace caf::detail {

class CAF_CORE_EXPORT unsafe_flow_msg {
public:
  template <class T>
  static void
  subscribe_impl(flow::publisher_base* source, flow::subscriber_base* sink) {
    auto dptr = static_cast<flow::publisher<T>*>(source);
    dptr->subscribe(static_cast<flow::subscriber<T>*>(sink));
  }

  using subscribe_fn = void (*)(flow::publisher_base*, flow::subscriber_base*);

  struct nop {
    void exec() const {
      // nop
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg() : event(nop{}) {
    // nop
  }

  struct on_batch {
    flow::subscriber_base_ptr sink;
    flow::batch content;
    void exec() const {
      sink->on_batch(content);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::subscriber_base_ptr sink, flow::batch content)
    : event(on_batch{std::move(sink), std::move(content)}) {
    // nop
  }

  struct on_subscribe {
    flow::subscriber_base_ptr sink;
    flow::subscription_ptr sub;
    void exec() const {
      sink->on_subscribe(sub);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::subscriber_base_ptr sink, flow::subscription_ptr sub)
    : event(on_subscribe{std::move(sink), std::move(sub)}) {
    // nop
  }

  struct on_complete {
    flow::subscriber_base_ptr sink;
    void exec() const {
      sink->on_complete();
    }
    void render(std::string& str) const;
  };

  template <class T>
  explicit unsafe_flow_msg(flow::subscriber_ptr<T> sink)
    : event(on_complete{std::move(sink)}) {
    // nop
  }

  struct on_error {
    flow::subscriber_base_ptr sink;
    error what;
    void exec() const {
      sink->on_error(what);
    }
    void render(std::string& str) const;
  };

  template <class T>
  unsafe_flow_msg(flow::subscriber_ptr<T> sink, error what)
    : event(on_error{std::move(sink), std::move(what)}) {
    // nop
  }

  struct subscribe {
    flow::publisher_base_ptr source;
    flow::subscriber_base_ptr sink;
    subscribe_fn fn;
    void exec() const {
      fn(source.get(), sink.get());
    }
    void render(std::string& str) const;
  };

  template <class T>
  unsafe_flow_msg(flow::publisher_ptr<T> source, flow::subscriber_ptr<T> sink)
    : event(subscribe{std::move(source), std::move(sink), subscribe_impl<T>}) {
    // nop
  }

  struct request {
    flow::subscription_ptr sub;
    size_t demand;
    void exec() const {
      sub->request(demand);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::subscription_ptr sub, size_t demand)
    : event(request{std::move(sub), demand}) {
    // nop
  }

  struct cancel {
    flow::subscription_ptr sub;
    void exec() const {
      sub->cancel();
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::subscription_ptr sub) : event(cancel{std::move(sub)}) {
    // nop
  }

  struct on_notify {
    flow::notifiable_ptr ptr;
    void exec() const {
      ptr->on_notify();
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::notifiable_ptr ptr) : event(on_notify{std::move(ptr)}) {
    // nop
  }

  struct on_close {
    flow::notifiable_ptr ptr;
    void exec() const {
      ptr->on_close();
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(close_atom, flow::notifiable_ptr ptr)
    : event(on_close{std::move(ptr)}) {
    // nop
  }

  struct on_abort {
    flow::notifiable_ptr ptr;
    error reason;
    void exec() const {
      ptr->on_abort(reason);
    }
    void render(std::string& str) const;
  };

  unsafe_flow_msg(flow::notifiable_ptr ptr, error reason)
    : event(on_abort{std::move(ptr), std::move(reason)}) {
    // nop
  }

  unsafe_flow_msg(const unsafe_flow_msg&) = default;
  unsafe_flow_msg& operator=(const unsafe_flow_msg&) = default;

  using event_type
    = std::variant<nop, on_batch, on_subscribe, on_complete, on_error,
                   subscribe, request, cancel, on_notify, on_close, on_abort>;

  event_type event;

  void exec() const {
    std::visit([](auto& ev) { ev.exec(); }, event);
  }
};

CAF_CORE_EXPORT std::string to_string(const unsafe_flow_msg& msg);

} // namespace caf::detail

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(caf::detail::unsafe_flow_msg)
