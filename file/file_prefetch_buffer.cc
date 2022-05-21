//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/file_prefetch_buffer.h"

#include <algorithm>
#include <mutex>

#include "file/random_access_file_reader.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

extern std::shared_ptr<Logger> _logger;

static void thread_reader_(RandomAccessFileReader* reader, const IOOptions& opts, uint64_t rounddown_offset,
                           uint64_t chunk_len, size_t read_len, Slice* result,
                                                      AlignedBuffer* buffer_, bool for_compaction, int i){
  Status ret = reader->Read(opts, rounddown_offset + chunk_len + (read_len*i), read_len, result,
                  buffer_->BufferStart() + chunk_len + (read_len*i), nullptr, for_compaction);
  assert(ret == Status::OK());
}

static void Prefetch2(FilePrefetchBuffer* p_buffer, const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t n,
                                    bool for_compaction, AlignedBuffer *buf_) {

  if (!p_buffer->get_enable_() || reader == nullptr) {
    return ;
  }
  TEST_SYNC_POINT("FilePrefetchBuffer::Prefetch:Start");
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t offset_ = static_cast<size_t>(offset);
  uint64_t rounddown_offset = Rounddown(offset_, alignment);
  uint64_t roundup_end = Roundup(offset_ + n, alignment);
  uint64_t roundup_len = roundup_end - rounddown_offset;
  assert(roundup_len >= alignment);
  assert(roundup_len % alignment == 0);

  // Check if requested bytes are in the existing buffer_.
  // If all bytes exist -- return.
  // If only a few bytes exist -- reuse them & read only what is really needed.
  //     This is typically the case of incremental reading of data.
  // If no bytes exist in buffer -- full pread.

  Status s;
  uint64_t chunk_offset_in_buffer = 0;
  uint64_t chunk_len = 0;
  bool copy_data_to_new_buffer = false;
  if (buf_->CurrentSize() > 0 && offset >= p_buffer->get_buffer_offset_() &&
      offset <= p_buffer->get_buffer_offset_() + buf_->CurrentSize()) {
    if (offset + n <= p_buffer->get_buffer_offset_() + buf_->CurrentSize()) {
      // All requested bytes are already in the buffer. So no need to Read
      // again.
      return ;
    } else {
      // Only a few requested bytes are in the buffer. memmove those chunk of
      // bytes to the beginning, and memcpy them back into the new buffer if a
      // new buffer is created.
      chunk_offset_in_buffer =
          Rounddown(static_cast<size_t>(offset - p_buffer->get_buffer_offset_()), alignment);
      chunk_len = buf_->CurrentSize() - chunk_offset_in_buffer;
      assert(chunk_offset_in_buffer % alignment == 0);
      assert(chunk_len % alignment == 0);
      assert(chunk_offset_in_buffer + chunk_len <=
             p_buffer->get_buffer_offset_() + buf_->CurrentSize());
      if (chunk_len > 0) {
        copy_data_to_new_buffer = true;
      } else {
        // this reset is not necessary, but just to be safe.
        chunk_offset_in_buffer = 0;
      }
    }
  }

  // Create a new buffer only if current capacity is not sufficient, and memcopy
  // bytes from old buffer if needed (i.e., if chunk_len is greater than 0).
  if (buf_->Capacity() < roundup_len) {
    buf_->Alignment(alignment);
    buf_->AllocateNewBuffer(static_cast<size_t>(roundup_len),
                              copy_data_to_new_buffer, chunk_offset_in_buffer,
                              static_cast<size_t>(chunk_len));
  } else if (chunk_len > 0) {
    // New buffer not needed. But memmove bytes from tail to the beginning since
    // chunk_len is greater than 0.
    buf_->RefitTail(static_cast<size_t>(chunk_offset_in_buffer),
                      static_cast<size_t>(chunk_len));
  }

  Slice result;
  size_t read_len = static_cast<size_t>(roundup_len - chunk_len);
  int R_NUM = 16;
  
  if(read_len%R_NUM == 0){
//    ROCKS_LOG_INFO(_logger,"%s prefetch start!!", reader->file_name().c_str());
    size_t div_len = read_len / R_NUM;
    
    for(int i=0;i<R_NUM;i++){ 
      p_buffer->read_thread_pool_.push_back(std::thread(thread_reader_, reader, opts, rounddown_offset, chunk_len, 
            div_len, &result, buf_, for_compaction, i));
    }

    for(auto& thread : p_buffer->read_thread_pool_){
      thread.join();
    }

//    ROCKS_LOG_INFO(_logger,"%s prefetch end!!", reader->file_name().c_str());
    p_buffer->read_thread_pool_.clear();
  }
  else{
    thread_reader_(reader, opts, rounddown_offset, chunk_len, read_len, &result, buf_, for_compaction, 0);
  }

#ifndef NDEBUG
  if (result.size() < read_len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
#endif
  p_buffer->set_buffer_offset_(rounddown_offset);
  buf_->Size(static_cast<size_t>(chunk_len) + read_len); 
}

static void background_read(FilePrefetchBuffer* p_buffer, const IOOptions& opts, RandomAccessFileReader* reader, 
                            uint64_t offset, size_t n, AlignedBuffer* buf_){
  Slice result;
  thread_reader_(reader, opts, offset, 0, n, &result, buf_, true, 0);
  buf_->Size(result.size());
}

Status FilePrefetchBuffer::Prefetch(const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t n,
                                    bool for_compaction) { 
  if(for_compaction) {
//    Prefetch2(this, opts, reader, offset, n, for_compaction, buffer_); 
#if 1
//cold miss
    if(buffer_->CurrentSize() == 0 && buffer_2->CurrentSize() == 0) {
      Prefetch2(this, opts, reader, offset, n, for_compaction, buffer_);
      thread_ = std::thread(background_read, this, opts, reader, buffer_offset_+buffer_->CurrentSize(), n, buffer_2);
    }
    else if(buffer_->CurrentSize() > 0 && offset >= buffer_offset_ &&
            offset <= buffer_offset_ + buffer_->CurrentSize()) { //buffer_ hit
      if(offset + n <= buffer_offset_ + buffer_->CurrentSize()) { //buffer_ full hit
        return Status::OK();
      }
      else { // refitail and buffer_2 get
#if 1    
        if(thread_.joinable()) thread_.join();
        size_t tmp_offset = static_cast<size_t>(offset);
        uint64_t tmp_rounddown_offset = Rounddown(tmp_offset, 4096);
        uint64_t tmp_roundup_end = Roundup(tmp_offset + n, 4096);
        uint64_t tmp_roundup_len = tmp_roundup_end - tmp_rounddown_offset;
        assert(tmp_roundup_len >= 4096);
        assert(tmp_roundup_len % 4096 == 0);

        uint64_t tmp_chunk_offset_in_buffer = 0;
        uint64_t tmp_chunk_len = 0;
        tmp_chunk_offset_in_buffer = Rounddown(static_cast<size_t>(offset - buffer_offset_), 4096);
        tmp_chunk_len = buffer_->CurrentSize() - tmp_chunk_offset_in_buffer;
        assert(tmp_chunk_offset_in_buffer % 4096 == 0);
        assert(tmp_chunk_len % 4096 == 0);
        assert(tmp_chunk_offset_in_buffer + tmp_chunk_len <=
               buffer_offset_ + buffer_->CurrentSize());

        if(buffer_->Capacity() < tmp_roundup_len){
          buffer_->Alignment(4096);
          buffer_->AllocateNewBuffer(static_cast<size_t>(tmp_roundup_len), true, tmp_chunk_offset_in_buffer, static_cast<size_t>(tmp_chunk_len));
        }
        else if(tmp_chunk_len >0){
          buffer_->RefitTail(static_cast<size_t>(tmp_chunk_offset_in_buffer),
                            static_cast<size_t>(tmp_chunk_len));
        }

        size_t tmp_read_len = static_cast<size_t>(tmp_roundup_len - tmp_chunk_len);
              
        memcpy(buffer_->BufferStart()+tmp_chunk_len, buffer_2->BufferStart(), tmp_read_len);
        buffer_->Size(static_cast<size_t>(tmp_chunk_len) + tmp_read_len); 
        buffer_offset_ += tmp_chunk_offset_in_buffer;
        thread_ = std::thread(background_read, this, opts, reader, buffer_offset_+buffer_->CurrentSize(), n, buffer_2);
#endif  
        return Status::OK();
      }
    }
    else if(offset >= buffer_offset_ + buffer_->CurrentSize()){ 
      ROCKS_LOG_INFO(_logger,"%s buffer_2 full hit, offset=%ld, length=%ld", reader->file_name().c_str(), offset, n);
      if(thread_.joinable()) thread_.join();
      //swap 
      buffer_3 = buffer_2;
      buffer_ = buffer_3;
      buffer_2 = buffer_3;
      thread_ = std::thread(background_read, this, opts, reader, buffer_offset_+buffer_->CurrentSize(), n, buffer_2);
      return Status::OK();
    }
#endif
    return Status::OK();
  }
  
  if (!enable_ || reader == nullptr) {
    return Status::OK();
  }
  TEST_SYNC_POINT("FilePrefetchBuffer::Prefetch:Start");
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t offset_ = static_cast<size_t>(offset);
  uint64_t rounddown_offset = Rounddown(offset_, alignment);
  uint64_t roundup_end = Roundup(offset_ + n, alignment);
  uint64_t roundup_len = roundup_end - rounddown_offset;
  assert(roundup_len >= alignment);
  assert(roundup_len % alignment == 0);

  // Check if requested bytes are in the existing buffer_.
  // If all bytes exist -- return.
  // If only a few bytes exist -- reuse them & read only what is really needed.
  //     This is typically the case of incremental reading of data.
  // If no bytes exist in buffer -- full pread.

  Status s;
  uint64_t chunk_offset_in_buffer = 0;
  uint64_t chunk_len = 0;
  bool copy_data_to_new_buffer = false;
  if (buffer_->CurrentSize() > 0 && offset >= buffer_offset_ &&
      offset <= buffer_offset_ + buffer_->CurrentSize()) { //buffer hit case!!
    if (offset + n <= buffer_offset_ + buffer_->CurrentSize()) { //all hit
      // All requested bytes are already in the buffer. So no need to Read
      // again.
      return s;
    } else { // partially hit
      // Only a few requested bytes are in the buffer. memmove those chunk of
      // bytes to the beginning, and memcpy them back into the new buffer if a
      // new buffer is created.
      chunk_offset_in_buffer =
          Rounddown(static_cast<size_t>(offset - buffer_offset_), alignment);
      chunk_len = buffer_->CurrentSize() - chunk_offset_in_buffer;
      assert(chunk_offset_in_buffer % alignment == 0);
      assert(chunk_len % alignment == 0);
      assert(chunk_offset_in_buffer + chunk_len <=
             buffer_offset_ + buffer_->CurrentSize());
      if (chunk_len > 0) {
        copy_data_to_new_buffer = true;
      } else {
        // this reset is not necessary, but just to be safe.
        chunk_offset_in_buffer = 0;
      }
    }
  }

  // Create a new buffer only if current capacity is not sufficient, and memcopy
  // bytes from old buffer if needed (i.e., if chunk_len is greater than 0).
  if (buffer_->Capacity() < roundup_len) {
    buffer_->Alignment(alignment);
    buffer_->AllocateNewBuffer(static_cast<size_t>(roundup_len),
                              copy_data_to_new_buffer, chunk_offset_in_buffer,
                              static_cast<size_t>(chunk_len));
  } else if (chunk_len > 0) {
    // New buffer not needed. But memmove bytes from tail to the beginning since
    // chunk_len is greater than 0.
    buffer_->RefitTail(static_cast<size_t>(chunk_offset_in_buffer),
                      static_cast<size_t>(chunk_len));
  }

  Slice result;
  size_t read_len = static_cast<size_t>(roundup_len - chunk_len);
  s = reader->Read(opts, rounddown_offset + chunk_len, read_len, &result,
                   buffer_->BufferStart() + chunk_len, nullptr, for_compaction);
  if (!s.ok()) {
    return s;
  }

#ifndef NDEBUG
  if (result.size() < read_len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
#endif
  buffer_offset_ = rounddown_offset;
  buffer_->Size(static_cast<size_t>(chunk_len) + result.size());
  return s;
}

bool FilePrefetchBuffer::TryReadFromCache(const IOOptions& opts,
                                          uint64_t offset, size_t n,
                                          Slice* result, Status* status,
                                          bool for_compaction) {
  if (track_min_offset_ && offset < min_offset_read_) {
    min_offset_read_ = static_cast<size_t>(offset);
  }
  if (!enable_ || offset < buffer_offset_) {
    return false;
  }

  // If the buffer contains only a few of the requested bytes:
  //    If readahead is enabled: prefetch the remaining bytes + readahead bytes
  //        and satisfy the request.
  //    If readahead is not enabled: return false.
  // exmaple: offset= 900KB, size = 400KB = 1300
  //          buffer_ 1000KB, buffer_2 1000KB
            //offset = 500KB, size =100KB

            //buffer miss case
  if (offset + n > buffer_offset_ + buffer_->CurrentSize()) {
    if (readahead_size_ > 0) {
      assert(file_reader_ != nullptr);
      assert(max_readahead_size_ >= readahead_size_);
      Status s;
      if (for_compaction) {
        s = Prefetch(opts, file_reader_, offset, std::max(n, readahead_size_),
                     for_compaction);
      } else {
        if (implicit_auto_readahead_) {
          // Prefetch only if this read is sequential otherwise reset
          // readahead_size_ to initial value.
          if (!IsBlockSequential(offset)) {
            UpdateReadPattern(offset, n);
            ResetValues();
            // Ignore status as Prefetch is not called.
            s.PermitUncheckedError();
            return false;
          }
          num_file_reads_++;
          if (num_file_reads_ <= kMinNumFileReadsToStartAutoReadahead) {
            UpdateReadPattern(offset, n);
            // Ignore status as Prefetch is not called.
            s.PermitUncheckedError();
            return false;
          }
        }
        s = Prefetch(opts, file_reader_, offset, n + readahead_size_,
                     for_compaction);
      }
      if (!s.ok()) {
        if (status) {
          *status = s;
        }
#ifndef NDEBUG
        IGNORE_STATUS_IF_ERROR(s);
#endif
        return false;
      }
      readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
    } else {
      return false;
    }
  }
  UpdateReadPattern(offset, n);
  uint64_t offset_in_buffer = offset - buffer_offset_;
  *result = Slice(buffer_->BufferStart() + offset_in_buffer, n);
  return true;
}
}  // namespace ROCKSDB_NAMESPACE
