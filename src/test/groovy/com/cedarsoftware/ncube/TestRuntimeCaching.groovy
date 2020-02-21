package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Test

import java.lang.reflect.Method

/**
 * @author Josh Snyder (joshsnyder@gmail.com)
 *         John DeRegnaucourt (jdereg@gmail.com)
 *
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestRuntimeCaching extends NCubeBaseTest
{
    @Test
    void testBar()
    {
        NCubeRuntimeClient runtime = NCubeAppContext.ncubeRuntime
        NCube cube1 = runtime.getNCubeFromResource(ApplicationID.testAppId, 'testCube1.json')
        NCube cube2 = runtime.getNCubeFromResource(ApplicationID.testAppId, 'testCube5.json')
        assert cube1 != cube2
        
        runtime.clearCache(ApplicationID.testAppId)
        runtime.addCube(cube1)
        cube1 = runtime.getCube(ApplicationID.testAppId, 'TestCube')    // cube 1

        Method allowMutable = NCubeRuntime.class.getDeclaredMethod('setAllowMutable', Boolean.class)
        allowMutable.accessible = true
        allowMutable.invoke(runtime, false) // allow MutableMethods false

        Method cacheCube = NCubeRuntime.class.getDeclaredMethod('cacheCube', NCube.class, boolean.class)
        cacheCube.accessible = true
        NCube rogue = (NCube) cacheCube.invoke(runtime, cube2, false) // calling cacheCube(cube2, false) - but it should ignore this

        // With allowMutable false, cacheCube does not allow caching another cube overtop original
        assert cube1.is(rogue)

        allowMutable.invoke(runtime, true) // allow MutableMethods true
        rogue = (NCube) cacheCube.invoke(runtime, cube2, false)

        // With allowMutable true, cacheCube does allow caching another cube overtop original
        assert !cube1.is(rogue)

        // Make sure allowMutable setting is re-read from the Spring Environment.
        allowMutable.invoke(runtime, null)
    }
}
