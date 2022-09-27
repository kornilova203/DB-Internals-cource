package net.barashev.dbi2022

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun createCache(storage: Storage, maxCacheSize: Int = -1): PageCache = ClockSweepPageCacheImpl(storage, maxCacheSize)

class ClockSweepPageCacheTest {
    @Test
    fun `pages are evicted when cache is full`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = createCache(storage, maxCacheSize = 5)
        cache.load(1, 5)
        val cost1 = storage.totalAccessCost

        cache.getAndPin(10).close()
        val cost2 = storage.totalAccessCost
        assertEquals(1, cache.stats.cacheMiss)

        (5 downTo 1).forEach { cache.getAndPin(it) }
        val cost3 = storage.totalAccessCost
        assertEquals(4, cache.stats.cacheHit)
        assertEquals(2, cache.stats.cacheMiss)

        assertTrue(cost3 > cost2)
    }

    @Test
    fun `exception when all pages are pinned`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = createCache(storage, maxCacheSize = 3)
        (1 .. 3).forEach { cache.getAndPin(it) }

        assertThrows<IllegalStateException> {
            cache.getAndPin(10).close()
        }
    }

    @Test
    fun `first added page is evicted first`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = createCache(storage, maxCacheSize = 3)
        (1 .. 3).forEach { cache.get(it) }
        cache.getAndPin(10).close()
        val cacheMiss = cache.stats.cacheMiss
        cache.get(1)
        assertEquals(cacheMiss + 1, cache.stats.cacheMiss)
    }

    @Test
    fun `page is evicted later if it's accessed`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = createCache(storage, maxCacheSize = 3)
        (1 .. 3).forEach { cache.get(it) }
        cache.get(10) // evict page 1
        cache.get(2) // access second page
        cache.get(11)
        val cacheHit = cache.stats.cacheHit
        cache.get(2)
        assertEquals(cacheHit + 1, cache.stats.cacheHit)
        val cacheMiss = cache.stats.cacheMiss
        cache.get(3)
        assertEquals(cacheMiss + 1, cache.stats.cacheMiss)
    }
}