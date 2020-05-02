package com.cedarsoftware.ncube

import com.cedarsoftware.util.TimedSynchronize
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 * @author Ken Partlow (kpartlow@gmail.com)
 * <br>
 * Copyright (c) Cedar Software LLC
 * <br><br>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <br><br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br><br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Slf4j
@CompileStatic
abstract class UrlCommandCell implements CommandCell
{
    private String cmd
    private final String url
    private int hash
    public static final char EXTENSION_SEPARATOR = '.'
    private volatile boolean hasBeenCached = false
    protected def cache
    private Lock hasBeenCachedLock = new ReentrantLock()
    // would prefer this was a final
    private boolean cacheable

    //  Private constructor only for serialization.
    protected UrlCommandCell()
    {
        this.url = null
    }

    UrlCommandCell(String cmd, String url = null, boolean cacheable = false)
    {
        this.url = url
        if (cmd == null && url == null)
        {
            throw new IllegalArgumentException("Both 'cmd' and 'url' cannot be null")
        }

        if (cmd != null && cmd.empty)
        {   // Because of this, cmdHash() never has to worry about an empty ("") command (when url is null)
            throw new IllegalArgumentException("'cmd' cannot be empty")
        }

        this.cmd = cmd
        this.cacheable = cacheable
        this.hash = cmd == null ? url.hashCode() : cmd.hashCode()
    }

    String getUrl()
    {
        return url
    }

    boolean isCacheable()
    {
        return cacheable
    }

    void clearClassLoaderCache(ApplicationID appId)
    {
        TimedSynchronize.synchronize(hasBeenCachedLock, 250, TimeUnit.MILLISECONDS, 'Dead lock detected attempting to clear ClassLoader cache.')
        hasBeenCached = false
        def localVar = cache

        try
        {
            // classpath case, lets clear all classes before setting to null.
            if (localVar instanceof GroovyClassLoader)
            {
                ((GroovyClassLoader)localVar).clearCache()
            }
            cache = null
        }
        finally
        {
            hasBeenCachedLock.unlock()
        }
    }
    
    protected URL getActualUrl(Map<String, Object> ctx)
    {
        for (int i=0; i < 2; i++)
        {   // Try URL resolution twice (HTTP HEAD called for connecting relative URLs to sys.classpath)
            try
            {
                return ncubeRuntime.getActualUrl(getNCube(ctx).applicationID, url, getInput(ctx))
            }
            catch(Exception e)
            {
                NCube cube = getNCube(ctx)
                String where = "url: ${url}, cube: ${cube.name}, app: ${cube.applicationID}"
                if (i == 1)
                {   // Note: Error is marked, it will not be retried in the future
                    log.warn("${getClass().simpleName}: failed 2nd attempt [will NOT retry in future] getActualUrl() - unable to resolve against sys.classpath, ${where}")
                    throw new IllegalStateException("Invalid URL in cell (unable to resolve against sys.classpath), ${where}", e)
                }
                else
                {
                    log.warn("${getClass().simpleName}: retrying getActualUrl() - unable to resolve against sys.classpath, ${where}")
                    Thread.sleep(100)
                }
            }
        }
        // will never happen - loop will throw exception on 2nd iteration
        return null
    }

    static NCube getNCube(Map<String, Object> ctx)
    {
        return (NCube) ctx.get('ncube')
    }

    static Map getInput(Map<String, Object> ctx)
    {
        return (Map) ctx.get('input')
    }

    static Map getOutput(Map<String, Object> ctx)
    {
        return (Map) ctx.get('output')
    }

    boolean equals(Object other)
    {
        if (!(other instanceof UrlCommandCell))
        {
            return false
        }

        UrlCommandCell that = other as UrlCommandCell

        if (cmd != null)
        {
            return cmd == that.cmd
        }

        if (cacheable != that.cacheable)
        {
            return false
        }

        return url == that.url
    }

    int hashCode()
    {
        return hash
    }

    String getCmd()
    {
        return cmd
    }

    String toString()
    {
        return url == null ? cmd : url
    }

    int compareTo(CommandCell cmdCell)
    {
        String cmd1 = cmd == null ? '' : cmd
        String cmd2 = cmdCell.cmd == null ? '' : cmdCell.cmd

        int comp = cmd1 <=> cmd2

        if (comp == 0)
        {
            String url1 = url == null ? '' : url
            String url2 = cmdCell.url == null ? '' : cmdCell.url
            return url1 <=> url2
        }

        return comp
    }

    void getCubeNamesFromCommandText(Set<String> cubeNames)
    {
    }

    void getScopeKeys(Set<String> scopeKeys)
    {
    }

    def execute(Map<String, Object> ctx)
    {
        if (!cacheable)
        {
            return fetchResult(ctx)
        }

        if (hasBeenCached)
        {
            return cache
        }

        TimedSynchronize.synchronize(hasBeenCachedLock, 250, TimeUnit.MILLISECONDS, 'Dead lock detected attempting to execute cell')

        try
        {
            if (hasBeenCached)
            {
                return cache
            }
            cache = fetchResult(ctx)
            hasBeenCached = true
            return cache
        }
        finally
        {
            hasBeenCachedLock.unlock()
        }
    }

    protected abstract def fetchResult(Map<String, Object> ctx)
}
