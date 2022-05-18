//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>
#include <sstream>
#include <string>
#include <vector>
#include <thread>

#include "file/random_access_file_reader.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

// FilePrefetchBuffer is a smart buffer to store and read data from a file.
class FilePrefetchBuffer {
 public:
  std::vector<std::thread> read_thread_pool_;
  std::thread thread_;
  std::vector<std::thread> back_thread_pool_;
  static const int kMinNumFileReadsToStartAutoReadahead = 2;
  // Constructor.
  //
  // All arguments are optional.
  // file_reader        : the file reader to use. Can be a nullptr.
  // readahead_size     : the initial readahead size.
  // max_readahead_size : the maximum readahead size.
  //   If max_readahead_size > readahead_size, the readahead size will be
  //   doubled on every IO until max_readahead_size is hit.
  //   Typically this is set as a multiple of readahead_size.
  //   max_readahead_size should be greater than equal to readahead_size.
  // enable : controls whether reading from the buffer is enabled.
  //   If false, TryReadFromCache() always return false, and we only take stats
  //   for the minimum offset if track_min_offset = true.
  // track_min_offset : Track the minimum offset ever read and collect stats on
  //   it. Used for adaptable readahead of the file footer/metadata.
  // implicit_auto_readahead : Readahead is enabled implicitly by rocksdb after
  //   doing sequential scans for two times.
  //
  // Automatic readhead is enabled for a file if file_reader, readahead_size,
  // and max_readahead_size are passed in.
  // If file_reader is a nullptr, setting readahead_size and max_readahead_size
  // does not make any sense. So it does nothing.
  // A user can construct a FilePrefetchBuffer without any arguments, but use
  // `Prefetch` to load data into the buffer.
  FilePrefetchBuffer(RandomAccessFileReader* file_reader = nullptr,
                     size_t readahead_size = 0, size_t max_readahead_size = 0,
                     bool enable = true, bool track_min_offset = false,
                     bool implicit_auto_readahead = false)
      : buffer_offset_(0),
        file_reader_(file_reader),
        readahead_size_(readahead_size),
        max_readahead_size_(max_readahead_size),
        initial_readahead_size_(readahead_size),
        min_offset_read_(port::kMaxSizet),
        enable_(enable),
        track_min_offset_(track_min_offset),
        implicit_auto_readahead_(implicit_auto_readahead),
        prev_offset_(0),
        prev_len_(0),
        num_file_reads_(kMinNumFileReadsToStartAutoReadahead + 1) {
          buffer_ = new AlignedBuffer;
          buffer_2 = new AlignedBuffer;
          buffer_3 = new AlignedBuffer;
          buffer_2->Alignment(4096);
          buffer_2->AllocateNewBuffer(30*1024*1024);
          }

  ~FilePrefetchBuffer() {
      if(thread_.joinable()){
         thread_.join();
      }
      delete buffer_->Release();
      delete buffer_2->Release();
      delete buffer_3->Release();
    }

  // Load data into the buffer from a file.
  // reader : the file reader.
  // offset : the file offset to start reading from.
  // n      : the number of bytes to read.
  // for_compaction : if prefetch is done for compaction read.
  Status Prefetch(const IOOptions& opts, RandomAccessFileReader* reader,
                  uint64_t offset, size_t n, bool for_compaction = false);

//  Status Prefetch2(const IOOptions& opts, RandomAccessFileReader* reader,
//                  uint64_t offset, size_t n, bool for_compaction, AlignedBuffer *buf_);
  // Tries returning the data for a file raed from this buffer, if that data is
  // in the buffer.
  // It handles tracking the minimum read offset if track_min_offset = true.
  // It also does the exponential readahead when readahead_size is set as part
  // of the constructor.
  //
  // offset : the file offset.
  // n      : the number of bytes.
  // result : output buffer to put the data into.
  // for_compaction : if cache read is done for compaction read.
  bool TryReadFromCache(const IOOptions& opts, uint64_t offset, size_t n,
                        Slice* result, Status* s, bool for_compaction = false);

  // The minimum `offset` ever passed to TryReadFromCache(). This will nly be
  // tracked if track_min_offset = true.
  size_t min_offset_read() const { return min_offset_read_; }

  void UpdateReadPattern(const size_t& offset, const size_t& len) {
    prev_offset_ = offset;
    prev_len_ = len;
  }

  bool IsBlockSequential(const size_t& offset) {
    return (prev_len_ == 0 || (prev_offset_ + prev_len_ == offset));
  }

  void ResetValues() {
    num_file_reads_ = 1;
    readahead_size_ = initial_readahead_size_;
  }

  bool get_enable_() { return enable_; }
  uint64_t get_buffer_offset_() { return buffer_offset_; }
  void set_buffer_offset_(uint64_t input) { buffer_offset_ = input; }
 private:
  uint64_t buffer_offset_;
  bool enable_;
  AlignedBuffer *buffer_;
  AlignedBuffer *buffer_2;
  AlignedBuffer *buffer_3;

  RandomAccessFileReader* file_reader_;
  size_t readahead_size_;
  size_t max_readahead_size_;
  size_t initial_readahead_size_;
  // The minimum `offset` ever passed to TryReadFromCache().
  size_t min_offset_read_;
  // if false, TryReadFromCache() always return false, and we only take stats
  // for track_min_offset_ if track_min_offset_ = true
  // If true, track minimum `offset` ever passed to TryReadFromCache(), which
  // can be fetched from min_offset_read().
  bool track_min_offset_;

  // implicit_auto_readahead is enabled by rocksdb internally after 2 sequential
  // IOs.
  bool implicit_auto_readahead_;
  size_t prev_offset_;
  size_t prev_len_;
  int num_file_reads_;
};
}  // namespace ROCKSDB_NAMESPACE
