/*
 * Copyright(c) 2023-present Mykola Garkusha.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "common.h"
#include "spin_lock.h"
#include <atomic>
#include <cstdint>
#include <emmintrin.h>
#include <memory>
#include <string>
#include <type_traits>

template <class T, class Derived, size_t _MAX_CONSUMER_N_, size_t _MAX_PRODUCER_N_, size_t _BATCH_NUM_,
          size_t _CPU_PAUSE_N_, bool _MULTICAST_, class Allocator, class VersionType>
class SPMCMulticastQueueBase
{
public:
  using type = T;
  static constexpr size_t CPU_PAUSE_N = _CPU_PAUSE_N_;
  static constexpr bool _synchronized_consumer_ = _MAX_CONSUMER_N_ > 1 && !_MULTICAST_;
  static constexpr bool _synchronized_producer_ = _MAX_PRODUCER_N_ > 1;
  static constexpr bool _spsc_ = _MAX_PRODUCER_N_ == 1 and _MAX_CONSUMER_N_ == 1;
  // static constexpr bool _reuse_single_bit_from_object_ = msb_always_0<T> && not _synchronized_consumer_;
  static constexpr bool _versionless_ = not _synchronized_producer_;
  static constexpr bool _binary_version_ = not _versionless_ and not _synchronized_consumer_;

  static constexpr bool _memcpyable_ =
    std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>;
  static constexpr bool _batch_consumption_enabled_ = _memcpyable_ && _versionless_ && not _synchronized_consumer_;
  static constexpr bool _remap_index_ = sizeof(T) <= _CACHE_LINE_SIZE_ and (not _batch_consumption_enabled_);

  static constexpr size_t _storage_alignment_ = (_remap_index_) ? power_of_2_far(sizeof(T)) : alignof(T);

  struct ConsumerTicket
  {
    size_t n;
    size_t consumer_id;
    size_t consumer_next_idx;
    size_t items_per_batch;
    size_t queue_idx;
    VersionType previous_version;
    ConsumerAttachReturnCode ret_code;
  };

  struct ProducerTicket
  {
    size_t producer_id;
    size_t items_per_batch;
    ProducerAttachReturnCode ret_code;
  };

protected:
  struct NodeWithVersion
  {
    alignas(_storage_alignment_) std::byte storage_[sizeof(T)];
    std::atomic<VersionType> version_;
  };

  struct alignas(_storage_alignment_) NodeWithoutVersion
  {
    alignas(_storage_alignment_) std::byte storage_[sizeof(T)];
  };

  using Node = std::conditional_t<_versionless_, NodeWithoutVersion, NodeWithVersion>;
  using NodeAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<Node>;
  using NodeAllocTraits = std::allocator_traits<NodeAllocator>;
  static_assert(std::is_default_constructible_v<Node>, "Node must be default-constructible");

  // these variables pretty much don't change through the lifetime of the queue
  size_t n_;
  size_t items_per_batch_;
  size_t power_of_two_idx_;
  size_t idx_mask_;
  size_t max_outstanding_non_consumed_items_;
  NodeAllocator alloc_;
  Node* nodes_;
  mutable Spinlock slow_path_guard_;

  // these variables update quite frequently

public:
  static constexpr size_t _item_per_prefetch_num_ = 2 * (_CACHE_PREFETCH_SIZE_ / sizeof(Node));
  static constexpr size_t _cache_line_num_ =
    power_of_2_far(std::max(_MAX_PRODUCER_N_, _item_per_prefetch_num_));
  static constexpr size_t _contention_window_size_ = _cache_line_num_ * _item_per_prefetch_num_;

  static_assert((_cache_line_num_ & (_cache_line_num_ - 1)) == 0);
  static_assert((_item_per_prefetch_num_ & (_item_per_prefetch_num_ - 1)) == 0);

  SPMCMulticastQueueBase(std::size_t N, const Allocator& alloc = Allocator())
    : n_(N), power_of_two_idx_(log2(n_)), items_per_batch_(n_ / _BATCH_NUM_), idx_mask_(n_ - 1), alloc_(alloc)
  {
    if ((N & (N - 1)) != 0)
    {
      throw std::runtime_error("N is not power of two");
    }

    if constexpr (_remap_index_ && _contention_window_size_ >= 1)
    {
      if (N % _contention_window_size_ != 0)
      {
        throw std::runtime_error(std::string("N [")
                                   .append(std::to_string(N))
                                   .append("] is not divisible by non-contention window [")
                                   .append(std::to_string(_contention_window_size_))
                                   .append("]"));
      }
    }

    if ((items_per_batch_ & (items_per_batch_ - 1)) != 0)
    {
      throw std::runtime_error("items_per_batch_ is not power of two");
    }

    nodes_ = std::allocator_traits<NodeAllocator>::allocate(alloc_, n_);
    std::uninitialized_value_construct(nodes_, nodes_ + n_);
  }

  ~SPMCMulticastQueueBase()
  {
    if constexpr (!std::is_trivially_destructible_v<T>)
    {
      size_t producer_last_idx = static_cast<const Derived*>(this)->get_producer_idx();
      for (size_t i = 0; i < this->n_; ++i)
      {
        // WARNING! need to revise if remap_index is used here!
        Node& node = this->nodes_[i];
        void* storage = node.storage_;
        if (i < producer_last_idx)
        {
          NodeAllocTraits::destroy(alloc_, static_cast<T*>(storage));
        }
      }
    }

    NodeAllocTraits::deallocate(alloc_, nodes_, n_);
  }

  template <class C>
  const T* do_peek(size_t queue_idx, C& consumer)
  {
    size_t original_idx = static_cast<Derived*>(this)->acquire_consumer_idx(consumer);
    size_t warped_idx = original_idx & consumer.idx_mask_;
    size_t node_idx = warped_idx;
    if constexpr (_remap_index_)
    {
      node_idx = map_index<_item_per_prefetch_num_, _cache_line_num_>(original_idx) & consumer.idx_mask_;
    }

    Node& node = this->nodes_[node_idx];
    if constexpr (C::blocking_v)
    {
      return do_peek_blocking(original_idx, node, warped_idx, consumer);
    }
    else
    {
      return do_peek_non_blocking(original_idx, node, warped_idx, consumer);
    }
  }

  template <class C>
  const T* do_peek_non_blocking(size_t original_idx, Node& node, size_t warped_idx, C& consumer)
  {
    if constexpr (_versionless_)
    {
      return static_cast<Derived*>(this)->peek(node, original_idx, warped_idx, consumer);
    }
    else
    {
      if constexpr (_binary_version_)
      {
        return static_cast<Derived*>(this)->peek(node, warped_idx, consumer);
      }
      else
      {
        static_assert(_synchronized_consumer_);
        size_t expected_version = 1 + div_by_power_of_two(original_idx, power_of_two_idx_);
        return static_cast<Derived*>(this)->peek(node, expected_version, consumer);
      }
    }
  }

  template <class C>
  const T* do_peek_blocking(size_t original_idx, Node& node, size_t warped_idx, C& consumer)
  {
    for (;;)
    {
      auto value = do_peek_non_blocking(original_idx, node, warped_idx, consumer);
      if (value)
      {
        return value;
      }

      unroll<_CPU_PAUSE_N_>([]() { _mm_pause(); });
    }
  }

  template <class C>
  void do_skip(size_t& queue_idx, C& consumer)
  {
    size_t original_idx = static_cast<Derived*>(this)->acquire_consumer_idx(consumer);
    size_t idx = original_idx & consumer.idx_mask_;
    size_t node_idx;
    if constexpr (_remap_index_)
    {
      node_idx = map_index<_item_per_prefetch_num_, _cache_line_num_>(original_idx) & consumer.idx_mask_;
    }
    else
    {
      node_idx = idx;
    }

    Node& node = this->nodes_[node_idx];
    if constexpr (_versionless_)
    {
      static_cast<Derived&>(*this).skip(idx, queue_idx, node, consumer);
    }
    else
    {
      VersionType version = node.version_.load(std::memory_order_acquire);
      if constexpr (_synchronized_consumer_)
      {
        idx = original_idx;
        size_t expected_version = 1 + div_by_power_of_two(idx, power_of_two_idx_);
        static_cast<Derived&>(*this).skip(idx, queue_idx, node, version, expected_version, consumer);
      }
      else
      {
        static_cast<Derived&>(*this).skip(idx, queue_idx, node, version, consumer);
      }
    }
  }

  size_t capacity() const { return this->n_; }
  size_t size() const { return capacity(); }

  template <class C>
  bool empty(size_t& queue_idx, C& consumer) requires(!_synchronized_consumer_)
  {
    return consumer.consumer_next_idx_ >= static_cast<const Derived*>(this)->get_producer_idx();
  }

  template <class C>
  bool empty(size_t& queue_idx, C& consumer) requires(_synchronized_consumer_)
  {
    return this->get_consumer_next_idx() >= static_cast<const Derived*>(this)->get_producer_idx();
  }
};
