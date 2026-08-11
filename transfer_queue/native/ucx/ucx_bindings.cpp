// Copyright 2026 The TransferQueue Team
// Licensed under the Apache License, Version 2.0 (the "License");

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <ucp/api/ucp.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace py = pybind11;

class Worker;

class Endpoint {
 public:
  Endpoint(std::shared_ptr<Worker> worker, ucp_ep_h endpoint) : worker_(std::move(worker)), endpoint_(endpoint) {}
  ~Endpoint();

  py::object post_send(uint64_t tag, py::buffer payload);
  void flush(double timeout_seconds);
  void close(double timeout_seconds);

 private:
  std::shared_ptr<Worker> worker_;
  ucp_ep_h endpoint_;
};

class ReceiveBuffer;

class Request : public std::enable_shared_from_this<Request> {
 public:
  enum class Kind { kSend, kReceive };

  Request(std::shared_ptr<Worker> worker, void* request, std::unique_ptr<uint8_t[]> buffer, size_t buffer_size)
      : worker_(std::move(worker)),
        request_(request),
        kind_(Kind::kReceive),
        buffer_(std::move(buffer)),
        buffer_size_(buffer_size),
        receive_data_(buffer_.get()) {}
  Request(std::shared_ptr<Worker> worker, void* request, py::object owner)
      : worker_(std::move(worker)), request_(request), kind_(Kind::kSend), owner_(std::move(owner)) {}
  ~Request();

  py::object wait(std::optional<double> timeout_seconds);
  // Non-blocking completion check.  Returns None while the request is in
  // progress, True for a completed send, or a receive buffer/list for a
  // completed receive.  It must be called by the UCX worker owner thread.
  py::object test();
  py::object test_cancel();
  void start_cancel();
  void cancel();

  const uint8_t* data() const { return receive_data_; }
  size_t size() const { return buffer_size_; }

 private:
  void complete();
  py::object receive_result();

  std::shared_ptr<Worker> worker_;
  void* request_;
  Kind kind_;
  // The receive target is overwritten completely by UCX.  Allocate it
  // without value-initializing/zeroing every byte; zeroing an 8 MiB payload
  // before each receive is pure CPU overhead and is not part of the wire
  // transfer.  The Request owns this allocation until wait() returns.
  std::unique_ptr<uint8_t[]> buffer_;
  size_t buffer_size_ = 0;
  uint8_t* receive_data_ = nullptr;
  // Keep the Python send buffer alive until UCX completes; no intermediate
  // intermediate native copy is needed for sends.
  py::object owner_ = py::none();
  bool complete_ = false;
};

class ReceiveBuffer {
 public:
  explicit ReceiveBuffer(std::shared_ptr<Request> request) : request_(std::move(request)) {}

  py::buffer_info buffer() {
    const uint8_t* data = request_->data();
    const size_t size = request_->size();
    return py::buffer_info(
        const_cast<uint8_t*>(data),
        sizeof(uint8_t),
        py::format_descriptor<uint8_t>::format(),
        {static_cast<py::ssize_t>(size)},
        {static_cast<py::ssize_t>(sizeof(uint8_t))});
  }

 private:
  std::shared_ptr<Request> request_;
};

class Worker : public std::enable_shared_from_this<Worker> {
 public:
  explicit Worker(const py::dict& options) {
    ucp_config_t* config = nullptr;
    ucs_status_t status = ucp_config_read(nullptr, nullptr, &config);
    check(status, "ucp_config_read");
    for (const auto& item : options) {
      const std::string name = py::cast<std::string>(item.first);
      const std::string value = py::cast<std::string>(item.second);
      status = ucp_config_modify(config, name.c_str(), value.c_str());
      if (status != UCS_OK) {
        ucp_config_release(config);
        check(status, ("ucp_config_modify(" + name + ")").c_str());
      }
    }

    ucp_params_t params{};
    params.field_mask = UCP_PARAM_FIELD_FEATURES;
    params.features = UCP_FEATURE_TAG;
    status = ucp_init(&params, config, &context_);
    ucp_config_release(config);
    check(status, "ucp_init");

    ucp_worker_params_t worker_params{};
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    // All UCP calls are serialized by mutex_.  SINGLE avoids relying on
    // cross-thread cancellation semantics while retaining safe Python use.
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
    status = ucp_worker_create(context_, &worker_params, &worker_);
    check(status, "ucp_worker_create");
  }

  ~Worker() { close(); }

  py::bytes address() {
    std::lock_guard<std::mutex> lock(mutex_);
    ucp_address_t* address = nullptr;
    size_t length = 0;
    check(ucp_worker_get_address(worker_, &address, &length), "ucp_worker_get_address");
    py::bytes result(reinterpret_cast<const char*>(address), length);
    ucp_worker_release_address(worker_, address);
    return result;
  }

  std::shared_ptr<Endpoint> connect(py::bytes remote_address) {
    std::string address = remote_address;
    ucp_ep_params_t params{};
    params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    params.address = reinterpret_cast<const ucp_address_t*>(address.data());
    ucp_ep_h endpoint = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      check_open();
      check(ucp_ep_create(worker_, &params, &endpoint), "ucp_ep_create");
    }
    return std::make_shared<Endpoint>(shared_from_this(), endpoint);
  }

  std::shared_ptr<Request> post_receive(uint64_t tag, size_t length) {
    auto buffer = std::make_unique<uint8_t[]>(length);
    ucp_request_param_t params{};
    void* request = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      check_open();
      request = ucp_tag_recv_nbx(worker_, buffer.get(), length, tag, UINT64_MAX, &params);
    }
    return make_receive_request(request, std::move(buffer), length, "ucp_tag_recv_nbx");
  }

  std::shared_ptr<Request> post_send(ucp_ep_h endpoint, uint64_t tag, py::buffer payload) {
    py::buffer_info info = payload.request();
    if (info.ndim != 1 || info.itemsize != 1 || info.strides[0] != 1) {
      throw std::runtime_error("UCX send payload must be a contiguous byte buffer");
    }
    py::object owner = payload;
    ucp_request_param_t params{};
    void* request = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      check_open();
      request = ucp_tag_send_nbx(endpoint, info.ptr, static_cast<size_t>(info.size), tag, &params);
    }
    return make_send_request(request, std::move(owner), "ucp_tag_send_nbx");
  }

  unsigned progress() {
    std::lock_guard<std::mutex> lock(mutex_);
    return worker_ != nullptr ? ucp_worker_progress(worker_) : 0;
  }

  void release_request(void* request) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (worker_ != nullptr && request != nullptr) ucp_request_free(request);
  }

  void cancel_request(void* request) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (worker_ != nullptr && request != nullptr) ucp_request_cancel(worker_, request);
  }

  void close_endpoint(ucp_ep_h endpoint, double timeout_seconds) {
    if (endpoint == nullptr) return;
    ucp_request_param_t params{};
    void* request = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (worker_ == nullptr) return;
      request = ucp_ep_close_nbx(endpoint, &params);
    }
    wait_native(request, timeout_seconds, "ucp_ep_close_nbx");
  }

  void flush_endpoint(ucp_ep_h endpoint, double timeout_seconds) {
    if (endpoint == nullptr) return;
    ucp_request_param_t params{};
    void* request = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (worker_ == nullptr) return;
      request = ucp_ep_flush_nbx(endpoint, &params);
    }
    wait_native(request, timeout_seconds, "ucp_ep_flush_nbx");
  }

  void close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (worker_ != nullptr) {
      ucp_worker_destroy(worker_);
      worker_ = nullptr;
    }
    if (context_ != nullptr) {
      ucp_cleanup(context_);
      context_ = nullptr;
    }
  }

  static void check(ucs_status_t status, const char* operation) {
    if (status != UCS_OK) throw std::runtime_error(std::string(operation) + ": " + ucs_status_string(status));
  }

  void wait_native(void* request, std::optional<double> timeout_seconds, const char* operation,
                   bool allow_canceled = false) {
    if (request == nullptr) return;
    if (UCS_PTR_IS_ERR(request)) check(UCS_PTR_STATUS(request), operation);
    std::optional<std::chrono::steady_clock::time_point> deadline;
    if (timeout_seconds.has_value()) {
      deadline = std::chrono::steady_clock::now() +
                 std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                     std::chrono::duration<double>(*timeout_seconds));
    }
    constexpr unsigned long sleep_us = 50;
    while (ucp_request_check_status(request) == UCS_INPROGRESS) {
      progress();
      if (deadline.has_value() && std::chrono::steady_clock::now() > *deadline) {
        cancel_request(request);
        while (ucp_request_check_status(request) == UCS_INPROGRESS) progress();
        ucp_request_free(request);
        throw std::runtime_error(std::string(operation) + ": timed out");
      }
      if (sleep_us != 0) std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }
    const ucs_status_t status = ucp_request_check_status(request);
    ucp_request_free(request);
    if (status != UCS_OK && !(allow_canceled && status == UCS_ERR_CANCELED)) {
      check(status, operation);
    }
  }

 private:
  std::shared_ptr<Request> make_receive_request(void* request, std::unique_ptr<uint8_t[]> buffer,
                                                size_t buffer_size, const char* operation) {
    if (UCS_PTR_IS_ERR(request)) check(UCS_PTR_STATUS(request), operation);
    return std::make_shared<Request>(shared_from_this(), request, std::move(buffer), buffer_size);
  }

  std::shared_ptr<Request> make_send_request(void* request, py::object owner, const char* operation) {
    if (UCS_PTR_IS_ERR(request)) check(UCS_PTR_STATUS(request), operation);
    return std::make_shared<Request>(shared_from_this(), request, std::move(owner));
  }

  void check_open() const {
    if (worker_ == nullptr) throw std::runtime_error("UCX worker is closed");
  }

  ucp_context_h context_ = nullptr;
  ucp_worker_h worker_ = nullptr;
  std::mutex mutex_;
};

Endpoint::~Endpoint() {
  try { close(0.0); } catch (...) {}
}

py::object Endpoint::post_send(uint64_t tag, py::buffer payload) {
  if (endpoint_ == nullptr) throw std::runtime_error("UCX endpoint is closed");
  return py::cast(worker_->post_send(endpoint_, tag, std::move(payload)));
}

void Endpoint::close(double timeout_seconds) {
  if (endpoint_ == nullptr) return;
  worker_->close_endpoint(endpoint_, timeout_seconds);
  endpoint_ = nullptr;
}

void Endpoint::flush(double timeout_seconds) {
  if (endpoint_ == nullptr) throw std::runtime_error("UCX endpoint is closed");
  worker_->flush_endpoint(endpoint_, timeout_seconds);
}

Request::~Request() {
  try { cancel(); } catch (...) {}
}

void Request::complete() {
  if (complete_) return;
  if (request_ != nullptr) worker_->release_request(request_);
  request_ = nullptr;
  complete_ = true;
}

py::object Request::receive_result() {
  return py::cast(std::make_shared<ReceiveBuffer>(shared_from_this()));
}

py::object Request::wait(std::optional<double> timeout_seconds) {
  if (complete_) {
    if (kind_ == Kind::kReceive) return receive_result();
    return py::none();
  }
  if (request_ != nullptr) {
    py::gil_scoped_release release;
    try {
      worker_->wait_native(request_, timeout_seconds, "UCX request");
    } catch (...) {
      // wait_native owns timeout cleanup; do not let the destructor cancel a
      // request that has already been freed.
      request_ = nullptr;
      complete_ = true;
      throw;
    }
  }
  request_ = nullptr;
  complete();
  if (kind_ == Kind::kReceive) return receive_result();
  return py::none();
}

py::object Request::test() {
  if (complete_) {
    if (kind_ == Kind::kReceive) return receive_result();
    return py::bool_(true);
  }
  if (request_ == nullptr) {
    // UCP may report an immediate completion with a null request handle.
    complete_ = true;
    if (kind_ == Kind::kReceive) return receive_result();
    return py::bool_(true);
  }

  const ucs_status_t status = ucp_request_check_status(request_);
  if (status == UCS_INPROGRESS) return py::none();
  if (status != UCS_OK) {
    worker_->release_request(request_);
    request_ = nullptr;
    complete_ = true;
    throw std::runtime_error(std::string("UCX request: ") + ucs_status_string(status));
  }

  worker_->release_request(request_);
  request_ = nullptr;
  complete_ = true;
  if (kind_ == Kind::kReceive) return receive_result();
  return py::bool_(true);
}

void Request::start_cancel() {
  if (!complete_ && request_ != nullptr) {
    // Some transports may spend measurable time in ucp_request_cancel().
    // Do not hold Python's process-wide GIL while the owner thread enters UCX:
    // the independent ZMQ control thread must remain able to acknowledge the
    // cancellation and serve unrelated requests.
    py::gil_scoped_release release;
    worker_->cancel_request(request_);
  }
}

py::object Request::test_cancel() {
  if (complete_ || request_ == nullptr) return py::bool_(true);
  const ucs_status_t status = ucp_request_check_status(request_);
  if (status == UCS_INPROGRESS) return py::none();
  worker_->release_request(request_);
  request_ = nullptr;
  complete_ = true;
  if (status != UCS_OK && status != UCS_ERR_CANCELED) {
    throw std::runtime_error(std::string("UCX request cancellation: ") +
                             ucs_status_string(status));
  }
  return py::bool_(true);
}

void Request::cancel() {
  if (complete_) return;
  if (request_ != nullptr) {
    worker_->cancel_request(request_);
    worker_->wait_native(request_, 1.0, "UCX request cancellation", true);
    request_ = nullptr;
  }
  complete();
}

PYBIND11_MODULE(_ucx, m) {
  m.doc() = "TransferQueue's narrow UCX UCP Tagged binding";
  py::class_<Worker, std::shared_ptr<Worker>>(m, "Worker")
      .def(py::init<py::dict>(), py::arg("config") = py::dict())
      .def("address", &Worker::address)
      .def("connect", &Worker::connect)
      .def("post_receive", &Worker::post_receive)
      .def("progress", &Worker::progress)
      .def("close", &Worker::close);
  py::class_<Endpoint, std::shared_ptr<Endpoint>>(m, "Endpoint")
      .def("post_send", &Endpoint::post_send)
      .def("flush", &Endpoint::flush)
      .def("close", &Endpoint::close);
  py::class_<ReceiveBuffer, std::shared_ptr<ReceiveBuffer>>(m, "ReceiveBuffer", py::buffer_protocol())
      .def_buffer(&ReceiveBuffer::buffer);
  py::class_<Request, std::shared_ptr<Request>>(m, "Request")
      .def("wait", &Request::wait)
      .def("test", &Request::test)
      .def("start_cancel", &Request::start_cancel)
      .def("test_cancel", &Request::test_cancel)
      .def("cancel", &Request::cancel);
}
