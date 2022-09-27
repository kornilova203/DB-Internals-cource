/*
 * Copyright 2022 Dmitry Barashev, JetBrains s.r.o.
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

package net.barashev.dbi2022

data class CachedPageUsage(
    val accessCount: Int,
    val lastAccessTs: Long,
)

/**
 * Enhances DiskPage interface with "dirty" flag and usage counts.
 */
interface CachedPage : AutoCloseable, DiskPage {
    val isDirty: Boolean
    val usage: CachedPageUsage
}

/**
 * This interface represents a buffer cache which buffers disk pages in RAM.
 * The following features are supported:
 * - loading a single page or a sequence of pages into the cache without pinning
 * - pinning a particular page, which guarantees that it will not be evicted
 * - flushing the whole cache to the storage
 * - creating a subcache with its own eviction and flushing bounds
 */
interface PageCache {
    /**
     * Loads a sequence of pages into the cache without pinning, e.g. for read-ahead purposes.
     *
     */
    fun load(startPageId: PageId, pageCount: Int = 1)

    /**
     * Fetches a single page into the cache.
     */
    fun get(pageId: PageId): CachedPage

    /**
     * Fetches a single page into the cache and pins it. It is client's responsibility to close CachedPage instances
     * returned from this method after using.
     */
    fun getAndPin(pageId: PageId): CachedPage

    /**
     * Creates a subcache of the specified size, which is intended to be used for relatively short one-time bulk operations,
     * such as data sorting.
     *
     * A subcache shares the buffer pool with the main cache, namely, if some page is loaded into
     * the subcache, it appears in the main cache as well. If some page is loaded into the main cache, and is requested
     * through a subcache instance, it will be registered in the subcache too.
     *
     * Subcache eviction and flush policies are limited to the pages loaded via subcache only. That is, if a subcache
     * is full when a new page is requested from subcache instance, a victim page for eviction will be one of the pages
     * registered in the subcache. Also, if flush is called on a subcache instance, it will flush only those pages which
     * are registered in the subcache.
     *
     * Second-level subcaches are not supported.
     */
    fun createSubCache(size: Int): PageCache

    /**
     * Flushes the cached pages to the disk storage.
     */
    fun flush()

    /**
     * Returns this cache hit/miss statistics.
     */
    val stats: PageCacheStats

    /**
     * Returns the capacity (maximum size) of this cache in pages
     */
    val capacity: Int
}

interface PageCacheStats {
    val cacheHit: Int
    val cacheMiss: Int
}