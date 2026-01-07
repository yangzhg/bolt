/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/functions/colocate/client/UDFClient.h"
#include <arrow/builder.h>
#include <arrow/flight/client.h>
#include <arrow/table.h>
#include <common/base/BoltException.h>
#include <common/base/Exceptions.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <sstream>
#include "bolt/functions/colocate/client/Exception.h"

using namespace std::chrono_literals;
namespace bytedance::bolt::functions {

#define TIMEOUT_ERROR \
  "Flight returned timeout error, with message: Deadline Exceeded."

#define CHECK_STATUS_TIMEOUT(STATUS, PREFIX_MSG)                       \
  if (!(STATUS).ok()) {                                                \
    if ((STATUS).IsIOError() &&                                        \
        (STATUS).message().find(TIMEOUT_ERROR) != std::string::npos) { \
      throw ColocateTimeoutException((STATUS).ToString());             \
    }                                                                  \
    BOLT_FAIL((PREFIX_MSG) + (STATUS).ToString());                     \
  }

UDFClient::UDFClient(const std::string& hostname, int port) {
  auto location_result = Location::ForGrpcTcp(hostname, port);
  if (!location_result.ok()) {
    std::ostringstream oss;
    oss << "Unable to create grpc location [" << hostname << ":" << port << "]";
    BOLT_FAIL(oss.str());
  }
  auto location = *location_result;
  auto ClientResult = FlightClient::Connect(location);
  if (ClientResult.ok()) {
    Client_ = std::move(*ClientResult);
  } else {
    std::ostringstream oss;
    oss << "Unable to connect  to server [" << location.ToString() << "]";
    BOLT_FAIL(oss.str());
  }
}

std::vector<std::shared_ptr<RecordBatch>> UDFClient::Call(
    const std::vector<std::string>& paths,
    std::shared_ptr<RecordBatch>& batch,
    TimeoutDuration cancel_timeout) const {
  arrow::flight::FlightCallOptions options;
  options.timeout = cancel_timeout;

  // Create a FlightDescriptor using a path
  FlightDescriptor descriptor = FlightDescriptor::Path(paths);
  auto exchange_result = Client_->DoExchange(options, descriptor);

  VLOG(1) << "UDFClient::Call - Sent descriptor";
  auto exchange = std::move(*exchange_result);
  // Get the writer and reader
  auto writer = std::move(exchange.writer);
  auto reader = std::move(exchange.reader);
  auto status = exchange_result.status();
  CHECK_STATUS_TIMEOUT(status, "Error exchange: ");

  status = writer->Begin(batch->schema());
  CHECK_STATUS_TIMEOUT(status, "Error write schema: ");
  VLOG(1) << "UDFClient::Call - Sent schema";
  status = writer->WriteRecordBatch(*batch);
  CHECK_STATUS_TIMEOUT(status, "Error write data: ");
  VLOG(1) << "UDFClient::Call - Sent data";
  // Mark the stream as complete
  status = writer->DoneWriting();
  CHECK_STATUS_TIMEOUT(status, "Error done writing: ");
  VLOG(1) << "UDFClient::Call - Done writing";
  auto read_result = reader->ToRecordBatches();
  status = read_result.status();
  CHECK_STATUS_TIMEOUT(status, "Error read result: ");
  VLOG(1) << "UDFClient::Call - Read reply";
  return read_result.ValueUnsafe();
}
} // namespace bytedance::bolt::functions
