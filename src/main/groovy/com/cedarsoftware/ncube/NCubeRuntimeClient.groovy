package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.springframework.cache.Cache

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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

@CompileStatic
interface NCubeRuntimeClient extends NCubeClient
{
    /**
     * This JVM is running NCube in a read-only mode, meaning that it is not configured to be allowed to make calls
     * to the mutable APIs (CRUD APIs against the NCube storage server).
     * @return false if configured to allow mutable method calls (not readonly) or true (readonly) otherwise.
     */
    boolean isTrackBindingsOn()
    
    void clearCache(ApplicationID appId)

    void clearCache(ApplicationID appId, Collection<String> cubeNames)

    boolean isCached(ApplicationID appId, String cubeName)

    Cache getCacheForApp(ApplicationID appId)

    URL getActualUrl(ApplicationID appId, String url, Map input)

    String getUrlContent(ApplicationID appId, String url, Map input)

    URLClassLoader getLocalClassloader(ApplicationID appId)

    URLClassLoader getUrlClassLoader(ApplicationID appId, Map input)

    NCube getNCubeFromResource(ApplicationID appId, String name)

    List<NCube> getNCubesFromResource(ApplicationID appId, String name)

    void addAdvice(ApplicationID appId, String wildcard, Advice advice)

    void addCube(NCube ncube)

    void addCube(NCube ncube, boolean force)

    Map<String, Object> getSystemParams()

    ApplicationID getApplicationID(String tenant, String app, Map<String, Object> coord)

    ApplicationID getBootVersion(String tenant, String app)

    Map runTests(ApplicationID appId)

    Map runTests(ApplicationID appId, String cubeName, Object[] tests)

    Map runTest(ApplicationID appId, String cubeName, NCubeTest test)

    String getTestCauses(Throwable t)

    Map getMenu(ApplicationID appId)

    Map mapReduce(ApplicationID appId, String cubeName, String colAxisName, String where, Map options)

    Map<String, Object> getVisualizerGraph(ApplicationID appId, Map options)

    Map<String, Object> getVisualizerNodeDetails(ApplicationID appId, Map options)

    Map<String, Object> getVisualizerScopeChange(ApplicationID appId, Map options)

    Map getCell(ApplicationID appId, String cubeName, Map coordinate, defaultValue)

    Map execute(ApplicationID appId, String cubeName, String method, Map args)

    Object[] getCells(ApplicationID appId, String cubeName, Object[] idArrays, Map input)

    Object[] getCells(ApplicationID appId, String cubeName, Object[] idArrays, Map input, Map output)

    Object[] getCells(ApplicationID appId, String cubeName, Object[] idArrays, Map input, Map output, Object defaultValue)
}