package com.cedarsoftware.config

import com.cedarsoftware.controller.NCubeController
import com.cedarsoftware.controller.NCubeControllerAdvice
import com.cedarsoftware.ncube.GroovyBase
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeAppContext
import com.cedarsoftware.ncube.NCubeJdbcPersisterAdapter
import com.cedarsoftware.ncube.NCubeManager
import com.cedarsoftware.ncube.NCubePersister
import com.cedarsoftware.ncube.NCubeRuntime
import com.cedarsoftware.ncube.rules.RulesConfiguration
import com.cedarsoftware.ncube.rules.RulesController
import com.cedarsoftware.ncube.util.CdnClassLoader
import com.cedarsoftware.ncube.util.GCacheManager
import com.cedarsoftware.util.HsqlSchemaCreator
import com.cedarsoftware.util.JsonHttpProxy
import com.cedarsoftware.util.ReflectiveProxy
import groovy.transform.CompileStatic
import org.apache.http.HttpHost
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

import javax.annotation.PostConstruct

/**
 * This class defines allowable actions against persisted n-cubes
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License");
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

@CompileStatic
@Configuration
class NCubeConfiguration
{
    // Target server (storage-server)
    @Value('${ncube.target.host:localhost}') String host
    @Value('${ncube.target.port:9000}') int port
    @Value('${ncube.target.scheme:http}') String scheme

    // JsonHttpProxy
    @Value('${ncube.target.context:ncube}') String context
    @Value('${ncube.target.username:#{null}}') String username
    @Value('${ncube.target.password:#{null}}') String password
    @Value('${ncube.target.numConnections:200}') int numConnections

    // NCubeRuntime's cache
    @Value('${ncube.cache.max.size:0}') int maxSizeNCubeCache
    @Value('${ncube.cache.evict.type:expireAfterAccess}') String typeNCubeCache
    @Value('${ncube.cache.evict.duration:4}') int durationNCubeCache
    @Value('${ncube.cache.evict.units:hours}') String unitsNCubeCache
    @Value('${ncube.cache.concurrency:16}') int concurrencyNCubeCache

    // Permissions cache
    @Value('${ncube.perm.cache.max.size:100000}') int maxSizePermCache
    @Value('${ncube.perm.cache.evict.type:expireAfterAccess}') String typePermCache
    @Value('${ncube.perm.cache.evict.duration:3}') int durationPermCache
    @Value('${ncube.perm.cache.evict.units:minutes}') String unitsPermCache
    @Value('${ncube.perm.cache.concurrency:16}') int concurrencyPermCache

    // Location for generated source (optional) and compiled classes (optional)
    @Value('${ncube.sources.dir:#{null}}') String sourcesDirectory
    @Value('${ncube.classes.dir:#{null}}') String classesDirectory

    // Limit size of coordinate displayed in each CommandCell exception list (--> [coordinate])
    @Value('${ncube.stackEntry.coordinate.value.max:1000}') int stackEntryCoordinateValueMaxSize

    @Bean(name = 'ncubeRemoval')
    Closure getNcubeRemoval()
    {   // Clear all compiled classes associated to this n-cube (so ClassLoader may be freed).
        return { Object obj ->
            if (obj instanceof NCube)
            {
                NCube ncube = (NCube)obj
                ncube.unloadAnyGeneratedClasses()
            }
            return true
        }
    }

    @Bean(name = 'ncubeCacheManager')
    @Profile(['ncube-client', 'combined-client', 'ncube-runtime', 'combined-server'])
    GCacheManager getNcubeCacheManager()
    {
        GCacheManager cacheManager = new GCacheManager(ncubeRemoval, maxSizeNCubeCache, typeNCubeCache, durationNCubeCache, unitsNCubeCache, concurrencyNCubeCache)
        return cacheManager
    }

    @Bean(name = 'permCacheManager')
    GCacheManager getPermCacheManager()
    {
        GCacheManager cacheManager = new GCacheManager(null, maxSizePermCache, typePermCache, durationPermCache, unitsPermCache, concurrencyPermCache)
        return cacheManager
    }

    @Bean(name = 'ncubeHost')
    @Profile(['ncube-client','runtime-server'])
    HttpHost getHttpHost()
    {
        return new HttpHost(host, port, scheme)
    }

    @Bean(name = 'callableBean')
    @Profile(['ncube-client','runtime-server'])
    JsonHttpProxy getJsonHttpProxy()
    {
        return new JsonHttpProxy(getHttpHost(), context, username, password, numConnections)
    }

    @Bean(name = 'callableBean')
    @Profile(['combined-client','combined-server'])
    ReflectiveProxy getReflectiveProxy()
    {
        return new ReflectiveProxy(getNCubeManager())
    }

    @Bean('ncubeRuntime')
    @Profile(['combined-server', 'combined-client'])
    NCubeRuntime getNCubeRuntimeLocal()
    {
        return new NCubeRuntime(getReflectiveProxy(), getNcubeCacheManager())
    }

    @Bean('ncubeRuntime')
    @Profile(['ncube-client', 'runtime-server'])
    NCubeRuntime getNCubeRuntimeRemote()
    {
        return new NCubeRuntime(getJsonHttpProxy(), getNcubeCacheManager())
    }

    @Bean('ncubeManager')
    @Profile(['storage-server', 'combined-server', 'combined-client'])
    NCubeManager getNCubeManager()
    {
        return new NCubeManager(getNCubePersister(), getPermCacheManager())
    }

    @Bean(name = 'rulesController')
    @Profile(['ncube-client'])
    RulesController getRulesController()
    {
        return new RulesController(getRulesConfiguration())
    }

    @Bean(name = 'rulesConfiguration')
    @Profile(['ncube-client'])
    RulesConfiguration getRulesConfiguration()
    {
        return new RulesConfiguration()
    }

    // v========== runtime-server ==========v

    @Bean('ncubeControllerAdvice')
    @Profile('runtime-server')
    NCubeControllerAdvice getNCubeControllerAdvice2()
    {
        return new NCubeControllerAdvice(getNCubeController2())
    }

    @Bean('ncubeController')
    @Profile('runtime-server')
    NCubeController getNCubeController2()
    {
        return new NCubeController(getNCubeRuntimeRemote(), true)
    }

    // v========== storage-server ==========v

    @Bean('ncubeControllerAdvice')
    @Profile('storage-server')
    NCubeControllerAdvice getNCubeControllerAdvice3()
    {
        return new NCubeControllerAdvice(getNCubeController3())
    }

    @Bean('ncubeController')
    @Profile('storage-server')
    NCubeController getNCubeController3()
    {
        return new NCubeController(getNCubeManager(), false)
    }

    // v========== combined-server ==========v

    @Bean('ncubeControllerAdvice')
    @Profile('combined-server')
    NCubeControllerAdvice getNCubeControllerAdvice4()
    {
        return new NCubeControllerAdvice(getNCubeController4())
    }

    @Bean('ncubeController')
    @Profile('combined-server')
    NCubeController getNCubeController4()
    {
        return new NCubeController(getNCubeRuntimeLocal(), true)
    }

    // ========== Persistance Configuration ==========

    @Bean('persister')
    @Profile(['storage-server', 'combined-server', 'combined-client'])
    NCubePersister getNCubePersister()
    {
        return new NCubeJdbcPersisterAdapter()
    }

    // ========== App Context ==========
    @Bean('appContext')
    NCubeAppContext getAppContext()
    {
        return new NCubeAppContext()
    }

    @PostConstruct
    void init()
    {
        CdnClassLoader.generatedClassesDirectory = classesDirectory
        GroovyBase.generatedSourcesDirectory = sourcesDirectory
        NCube.stackEntryCoordinateValueMaxSize = stackEntryCoordinateValueMaxSize
    }

    @Profile('test-database')
    @Bean('hsqlSetup')
    HsqlSchemaCreator getSchemaCreator()
    {
        HsqlSchemaCreator schemaCreator = new HsqlSchemaCreator(
                'org.hsqldb.jdbcDriver',
                'jdbc:hsqldb:mem:testdb',
                'sa',
                '',
                '/config/hsqldb-schema.sql')
        return schemaCreator
    }
}
