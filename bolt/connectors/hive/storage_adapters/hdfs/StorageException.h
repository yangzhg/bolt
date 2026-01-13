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

#pragma once

#include <cerrno>
#include <string_view>
#include "bolt/common/base/BoltException.h"
namespace bytedance::bolt::filesystems {

class StorageException : public BoltException {
 public:
  enum StorageErrorType {
    Unknown,
    HdfsException,
    HdfsIOException,
    MissingBlockException,
    HdfsSlowReadIOException,
    HdfsNetworkException,
    HdfsNetworkConnectException,
    AccessControlException,
    AlreadyBeingCreatedException,
    ChecksumException,
    DSQuotaExceededException,
    FileAlreadyExistsException,
    FileNotFoundException,
    PathIsNotEmptyDirectoryException,
    HdfsBadBoolFoumat,
    HdfsBadConfigFoumat,
    HdfsBadNumFoumat,
    HdfsCanceled,
    HdfsFileSystemClosed,
    HdfsFileClosed,
    HdfsConfigInvalid,
    HdfsConfigNotFound,
    HdfsEndOfStream,
    HdfsInvalidBlockToken,
    HdfsFailoverException,
    HdfsRpcException,
    HdfsRpcServerException,
    HdfsRemoteException,
    HdfsTimeoutException,
    InvalidParameter,
    HadoopIllegalArgumentException,
    InvalidPath,
    NotReplicatedYetException,
    NSQuotaExceededException,
    ParentNotDirectoryException,
    ReplicaNotFoundException,
    SafeModeException,
    UnresolvedLinkException,
    UnsupportedOperationException,
    SaslException,
    NamenodeThrottlerException,
    ThrottlerLowLimitException,
    NameNodeStandbyException,
    RpcNoSuchMethodException,
    RecoveryInProgressException,
    LeaseExpiredException,
    ReadOnlyCoolFileException
  };

  StorageException(
      const char* file,
      size_t line,
      const char* function,
      std::string_view expression,
      std::string_view message,
      std::string_view storageErrorMessage,
      std::string_view /* errorSource */,
      std::string_view errorCode,
      bool isRetriable,
      std::string_view exceptionName = "StorageException")
      : BoltException(
            file,
            line,
            function,
            expression,
            message,
            error_source::kErrorSourceRuntime,
            errorCode,
            isRetriable,
            Type::kSystem,
            exceptionName),
        storageErrorMessage_(storageErrorMessage) {
    storageErrorType_ = getExceptionTypeFromErrorMsg(storageErrorMessage);
  }

  StorageErrorType getStorageErrorType() const {
    return storageErrorType_;
  }

  std::string getStorageErrorMessage() const {
    return storageErrorMessage_;
  }

  static StorageErrorType getExceptionTypeFromErrorMsg(
      std::string_view storageErrorMessage);

 private:
  StorageErrorType storageErrorType_;
  std::string storageErrorMessage_;
};

#define BOLT_STORAGE_CHECK(expr, storageErrorMessage, ...)                \
  if (UNLIKELY(!(expr))) {                                                \
    /* GCC 9.2.1 doesn't accept this code with constexpr. */              \
    auto message = ::bytedance::bolt::detail::errorMessage(__VA_ARGS__);  \
    static_assert(                                                        \
        !std::is_same_v<                                                  \
            typename ::bytedance::bolt::detail::BoltCheckFailStringType<  \
                decltype(message)>::type,                                 \
            std::string>,                                                 \
        "BUG: we should not pass std::string by value to boltCheckFail"); \
    throw ::bytedance::bolt::filesystems::StorageException(               \
        __FILE__,                                                         \
        __LINE__,                                                         \
        __FUNCTION__,                                                     \
        #expr,                                                            \
        message,                                                          \
        storageErrorMessage,                                              \
        ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(),     \
        ::bytedance::bolt::error_code::kInvalidState.c_str(),             \
        /* isRetriable */ false);                                         \
  }

} // namespace bytedance::bolt::filesystems
