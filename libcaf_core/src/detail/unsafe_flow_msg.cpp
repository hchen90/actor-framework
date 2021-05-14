// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/detail/unsafe_flow_msg.hpp"

#include "caf/detail/print.hpp"

namespace caf::detail {

void unsafe_flow_msg::nop::render(std::string& str) const {
  str += "nop";
}

void unsafe_flow_msg::on_batch::render(std::string& str) const {
  str += "on_batch(sink: ";
  print(str, reinterpret_cast<intptr_t>(sink.get()));
  str += ", size: ";
  print(str, content.size());
  str += ')';
}

void unsafe_flow_msg::on_subscribe ::render(std::string& str) const {
  str += "on_subscribe(sink: ";
  print(str, reinterpret_cast<intptr_t>(sink.get()));
  str += ", sub: ";
  print(str, reinterpret_cast<intptr_t>(sub.get()));
  str += ')';
}

void unsafe_flow_msg::on_complete::render(std::string& str) const {
  str += "on_complete(sink: ";
  print(str, reinterpret_cast<intptr_t>(sink.get()));
  str += ')';
}

void unsafe_flow_msg::on_error::render(std::string& str) const {
  str += "on_error(sink: ";
  print(str, reinterpret_cast<intptr_t>(sink.get()));
  str += ", what: ";
  str += to_string(what);
  str += ')';
}

void unsafe_flow_msg::subscribe::render(std::string& str) const {
  str += "subscribe(source: ";
  print(str, reinterpret_cast<intptr_t>(source.get()));
  str += ", sink: ";
  print(str, reinterpret_cast<intptr_t>(sink.get()));
  str += ')';
}

void unsafe_flow_msg::request::render(std::string& str) const {
  str += "request(sub: ";
  print(str, reinterpret_cast<intptr_t>(sub.get()));
  str += ", demand: ";
  print(str, demand);
  str += ')';
}

void unsafe_flow_msg::cancel::render(std::string& str) const {
  str += "cancel(sub: ";
  print(str, reinterpret_cast<intptr_t>(sub.get()));
  str += ')';
}

std::string to_string(const unsafe_flow_msg& msg) {
  std::string result = "unsafe_msg_ptr(";
  std::visit([&result](auto& ev) { ev.render(result); }, msg.event);
  result += ')';
  return result;
}

void unsafe_flow_msg::on_notify::render(std::string& str) const {
  str += "on_notify(ptr: ";
  print(str, reinterpret_cast<intptr_t>(ptr.get()));
  str += ')';
}

void unsafe_flow_msg::on_close::render(std::string& str) const {
  str += "on_close(ptr: ";
  print(str, reinterpret_cast<intptr_t>(ptr.get()));
  str += ')';
}

void unsafe_flow_msg::on_abort::render(std::string& str) const {
  str += "on_abort(ptr: ";
  print(str, reinterpret_cast<intptr_t>(ptr.get()));
  str += ", reason: ";
  str += to_string(reason);
  str += ')';
}

} // namespace caf::detail
