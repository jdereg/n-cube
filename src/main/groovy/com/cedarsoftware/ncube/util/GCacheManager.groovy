package com.cedarsoftware.ncube.util

import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.util.GuavaCache
import com.google.common.cache.CacheBuilder
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@Slf4j
@CompileStatic
class GCacheManager implements CacheManager
{
    private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>()
    private final int concurrencyLevel
    private final int maximumSize
    private final int evictionDuration
    private final String evictionTimeUnit
    private final String evictionType
    private final Closure removalClosure

    GCacheManager(Closure removalClosure = null, int maximumSize = 0, String evictionType = 'none', int evictionDuration = 0, String evictionTimeUnit = 'hours', int concurrencyLevel = 16)
    {
        this.removalClosure = removalClosure
        this.concurrencyLevel = concurrencyLevel
        this.maximumSize = maximumSize
        this.evictionType = evictionType
        this.evictionDuration = evictionDuration
        this.evictionTimeUnit = evictionTimeUnit
    }

    Cache getCache(String name)
    {
        Cache cache = caches[name]  // name = ApplicationID.toString()

        if (cache == null)
        {
            cache = createCache(name)
            Cache mapRef = caches.putIfAbsent(name, cache)
            if (mapRef != null)
            {
                cache = mapRef
            }
        }
        return cache
    }

    private GuavaCache createCache(String name)
    {
        CacheBuilder builder = CacheBuilder.newBuilder()
        builder.removalListener(new NCubeRemovalListener(removalClosure))
        builder.concurrencyLevel(concurrencyLevel)
        if (maximumSize > 0)
        {
            builder.maximumSize(maximumSize)
        }
        if (evictionType == 'expireAfterWrite')
        {
            builder.expireAfterWrite(evictionDuration, getTimeUnitFromString(evictionTimeUnit))
        }
        else if (evictionType == 'expireAfterAccess')
        {
            builder.expireAfterAccess(evictionDuration, getTimeUnitFromString(evictionTimeUnit))
        }
        GuavaCache guavaCache = new GuavaCache(name, builder.build() as com.google.common.cache.Cache)
        return guavaCache
    }

    private TimeUnit getTimeUnitFromString(String timeunit)
    {
        String lowerTimeunit = timeunit.toLowerCase()
        switch (lowerTimeunit)
        {
            case 'seconds':
                return TimeUnit.SECONDS
                break
            case 'minutes':
                return TimeUnit.MINUTES
                break
            case 'hours':
                return TimeUnit.HOURS
                break
            case 'days':
                return TimeUnit.DAYS
                break
            default:
                throw new IllegalArgumentException("Eviction time unit not understood: ${timeunit}. Please choose one of ['seconds', 'minutes', 'hours', 'days']")
        }
    }

    Collection<String> getCacheNames()
    {
        return caches.keySet()
    }

    /**
     * Apply the passed in Closure to all values in the cache.
     * @param name String name of the Cache
     * @param closure Closure to apply to each key-value pair
     */
    void applyToEntries(String name, Closure closure)
    {
        Cache cache = caches[name]
        if (cache == null)
        {
            return
        }
        GuavaCache gCache = cache as GuavaCache
        Iterator i = gCache.asMap().entrySet().iterator()
        while (i.hasNext())
        {
            Map.Entry entry = i.next()
            String key = entry.key as String
            Object value = entry.value
            closure(key, value)
        }
    }

    /**
     * Cube cache eviction listener.  This class exists to clear the class loader cache entries
     * associated to cells that have generated classes dynamically.  By removing the references
     * from those caches, the dynamically generated classes can be garbage collected (Java 8+).
     */
    private static class NCubeRemovalListener implements RemovalListener
    {
        private final Closure closure
        NCubeRemovalListener(Closure closure)
        {
            this.closure = closure
        }

        void onRemoval(RemovalNotification removalNotification)
        {
            if (closure != null)
            {
                if (removalNotification.value instanceof NCube)
                {
                    NCube ncube = (NCube) removalNotification.value
                    log.info("Cache eviction: n-cube: ${ncube.name}, app: ${ncube.applicationID}")
                }
                else
                {
                    log.info("Cache eviction: key=${removalNotification.key}, value=${removalNotification?.value?.toString()}")
                }
                closure.call(removalNotification.value)
            }
        }
    }
}
