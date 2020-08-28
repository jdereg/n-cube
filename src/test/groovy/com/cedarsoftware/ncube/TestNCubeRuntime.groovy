package com.cedarsoftware.ncube

import com.cedarsoftware.util.GuavaCache
import groovy.transform.CompileStatic
import org.junit.Test
import org.springframework.cache.Cache

import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.fail

@CompileStatic
class TestNCubeRuntime extends NCubeBaseTest
{
    @Test
    void testClearCubesFromCache()
    {
        populateCache()
        ncubeRuntime.clearCache(ApplicationID.testAppId, ['testbranch', 'testage'])
        Map cache = ((GuavaCache) testClient.getCacheForApp(ApplicationID.testAppId)).asMap()
        assert 2 == cache.size()
        assert !cache.containsKey('testbranch')
        assert !cache.containsKey('testage')
        assert cache.containsKey('testcube')
    }

    @Test
    void testIsCubeCached()
    {
        populateCache()
        assert ncubeRuntime.isCached(ApplicationID.testAppId, 'testbranch')
        assert ncubeRuntime.isCached(ApplicationID.testAppId, 'testage')
        assert ncubeRuntime.isCached(ApplicationID.testAppId, 'testcube')
        ncubeRuntime.clearCache(ApplicationID.testAppId)
        Cache cache = testClient.getCacheForApp(ApplicationID.testAppId)
        assert !cache.get('testbranch')
        assert !cache.get('testage')
        assert !cache.get('testcube')
    }

    @Test
    void testClearCubesFromCacheNullCubeNames()
    {
        populateCache()
        ncubeRuntime.clearCache(ApplicationID.testAppId, null)
        Map cache = ((GuavaCache) testClient.getCacheForApp(ApplicationID.testAppId)).asMap()
        assert 0 == cache.size()
    }

    @Test
    void testClearCubesFromCacheNullApp()
    {
        populateCache()
        try
        {
            ncubeRuntime.clearCache(null, ['testbranch', 'testage'])
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'ApplicationID', 'null')
        }
    }

    @Test
    void testClearCubesFromCacheNameNotInCache()
    {
        Map cache = populateCache()
        ncubeRuntime.clearCache(ApplicationID.testAppId, ['testnone'])
        assert 4 == cache.size()
        assert cache.containsKey('testbranch')
        assert cache.containsKey('testage')
        assert cache.containsKey('testcube')
    }

    private static Map populateCache()
    {
        createRuntimeCubeFromResource('test.branch.1.json')
        createRuntimeCubeFromResource('test.branch.age.1.json')
        createRuntimeCubeFromResource('testCube1.json')
        Map cache = ((GuavaCache) testClient.getCacheForApp(ApplicationID.testAppId)).asMap()
        return cache
    }
}
