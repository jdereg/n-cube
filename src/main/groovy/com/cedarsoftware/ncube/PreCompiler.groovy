package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.rules.RulesConfiguration
import com.cedarsoftware.ncube.rules.RulesEngine
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.cache.Cache
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.event.EventListener

import static com.cedarsoftware.ncube.ApplicationID.DEFAULT_TENANT
import static com.cedarsoftware.ncube.ApplicationID.HEAD
import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_ACTIVE_RECORDS_ONLY
import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_INCLUDE_CUBE_DATA
import static com.cedarsoftware.ncube.ReleaseStatus.RELEASE
import static com.cedarsoftware.ncube.ReleaseStatus.SNAPSHOT
import static com.cedarsoftware.ncube.rules.RulesConfiguration.APP_BRANCH
import static com.cedarsoftware.ncube.rules.RulesConfiguration.APP_NAME
import static com.cedarsoftware.ncube.rules.RulesConfiguration.APP_STATUS
import static com.cedarsoftware.ncube.rules.RulesConfiguration.APP_TENANT
import static com.cedarsoftware.ncube.rules.RulesConfiguration.APP_VERSION
import static com.cedarsoftware.util.StringUtilities.hasContent

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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
 *         limitatœions under the License.
 */

@Slf4j
@CompileStatic
class PreCompiler implements ApplicationContextAware
{
    private ApplicationContext ctx
    private final RulesConfiguration rulesConfiguration
    private final List<Map<String, String>> precompiledAppIds

    PreCompiler(RulesConfiguration rulesConfiguration, List<Map<String, String>> precompiledAppIds)
    {
        this.rulesConfiguration = rulesConfiguration
        this.precompiledAppIds = precompiledAppIds
    }

    @EventListener
    void onApplicationEvent(ApplicationStartedEvent event)
    {
        Closure closure = {
            precompile()
        }

        Thread t = new Thread(closure)
        t.name = 'Ncube.PreCompiler'
        t.daemon = true
        t.start()
    }

    protected void precompile()
    {
        long start = System.nanoTime()

        Set<ApplicationID> appIds = new LinkedHashSet<>()
        Map<String, RulesEngine> rulesEngines = rulesConfiguration.rulesEngines
        for (RulesEngine rulesEngine : rulesEngines.values())
        {
            appIds.add(rulesEngine.appId)
        }
        appIds.addAll(configuredAppIds)

        boolean hasCompileErrors = false
        Map<String, Object> searchOptions = [(SEARCH_ACTIVE_RECORDS_ONLY): true, (SEARCH_INCLUDE_CUBE_DATA): true] as Map

        NCubeRuntime fullRuntime = (NCubeRuntime) ncubeRuntime

        // compile all NCube cells
        for (ApplicationID appId : appIds)
        {
            log.info("Compiling NCubes for appId: ${appId}")
            Cache cache = fullRuntime.getCacheForApp(appId)
            List<NCubeInfoDto> dtos = fullRuntime.search(appId, '*', null, searchOptions)
            for (NCubeInfoDto dto : dtos)
            {
                NCube ncube = cache.get(dto.name, NCube.class)
                if (ncube == null)
                {
                    NCube ncubeFromBytes = NCube.createCubeFromBytes(dto.bytes)
                    ncubeFromBytes.applicationID = dto.applicationID
                    hasCompileErrors = checkCompileErrors(ncubeFromBytes.compile(), hasCompileErrors)
                    NCube ncubeFromCache = cache.get(dto.name, NCube.class)
                    if (ncubeFromCache == null)
                    {
                        fullRuntime.addCube(ncubeFromBytes)
                    }
                    else
                    {
                        hasCompileErrors = checkCompileErrors(ncubeFromCache.compile(), hasCompileErrors)
                    }
                }
                else
                {
                    hasCompileErrors = checkCompileErrors(ncube.compile(), hasCompileErrors)
                }
            }
        }

        long stop = System.nanoTime()
        long total = Math.round((stop - start) / 1000000.0d)

        if (hasCompileErrors)
        {
            log.error('Error compiling NCubes. See previous logs.')
            ((ConfigurableApplicationContext) ctx).close()
        }
        else
        {
            log.info("Compiled all NCubes in: ${total} ms")
        }
    }

    private static boolean checkCompileErrors(CompileInfo compileInfo, boolean hasCompileErrors)
    {
        if (!hasCompileErrors)
        {
            return !compileInfo.getExceptions().empty
        }
        else
        {
            return hasCompileErrors
        }
    }

    private Set<ApplicationID> getConfiguredAppIds()
    {
        Set<ApplicationID> appIds = new HashSet<>()
        for (Map<String, String> precompiledAppId : precompiledAppIds)
        {
            String tenant = precompiledAppId[APP_TENANT] ?: DEFAULT_TENANT
            String app = precompiledAppId[APP_NAME]
            if (!hasContent(app))
            {
                log.warn("Missing 'app' property in ncube.performance.precompileApps: ${precompiledAppId}.")
                continue
            }

            String status = precompiledAppId[APP_STATUS] ?: RELEASE.name()
            if (![RELEASE.name(), SNAPSHOT.name()].contains(status))
            {
                log.warn("Illegal 'status' property in ncube.performance.precompileApps: ${precompiledAppId}. Must be SNAPSHOT or RELEASE.")
                continue
            }

            String version = precompiledAppId[APP_VERSION]
            if (!hasContent(version))
            {
                Object[] versions = ncubeRuntime.getVersions(app)
                for (Object ver : versions)
                {
                    String versionStatus = (String) ver
                    if (versionStatus.endsWith(RELEASE.name()))
                    {
                        version = versionStatus - '-RELEASE'
                        break
                    }
                }
            }

            String branch = precompiledAppId[APP_BRANCH] ?: HEAD
            ApplicationID appId = new ApplicationID(tenant, app, version, status, branch)
            appIds.add(appId)
        }
        return appIds
    }

    void setApplicationContext(ApplicationContext applicationContext)
    {
        ctx = applicationContext
    }
}
