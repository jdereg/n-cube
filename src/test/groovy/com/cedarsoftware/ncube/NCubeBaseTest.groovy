package com.cedarsoftware.ncube

import com.cedarsoftware.config.NCubeConfiguration
import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Ignore
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.util.StringUtilities.hasContent

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = [NCubeApplication, NCubeConfiguration])
@TestPropertySource(properties = ['ncube.allow.mutable.methods=true', 'ncube.accepted.domains='])
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
//@ActiveProfiles(profiles = ['ncube-client'])  // requires server running
@ActiveProfiles(profiles = ['combined-server','test-database'])
@Ignore
class NCubeBaseTest implements NCubeConstants
{
    static String baseRemoteUrl
    private static NCubeTestClient testClient = null
    private static NCubeMutableClient mutableClient = null

    @After
    void teardown()
    {
        getTestClient().clearCache()
    }
    
    static NCubeMutableClient getMutableClient()
    {
        if (mutableClient)
        {
            return mutableClient
        }

        try
        {
            mutableClient = NCubeAppContext.ncubeMutableClient
        }
        catch (Exception ignored)
        {
            mutableClient = NCubeAppContext.getBean(MANAGER_BEAN) as NCubeMutableClient
        }
        return mutableClient
    }

    static NCubeTestClient getTestClient()
    {
        if (testClient)
        {
            return testClient
        }
        return testClient = NCubeAppContext.getBean(RUNTIME_BEAN) as NCubeTestClient
    }

    /**
     * Loads ncube into the runtimeClient, replacing references to ${baseRemoteUrl}, if found in the json
     */
    static NCube createRuntimeCubeFromResource(ApplicationID appId = ApplicationID.testAppId, String fileName)
    {
        String json = NCubeRuntime.getResourceAsString(fileName)
        if (hasContent(baseRemoteUrl))
        {
            json = json.replaceAll('\\$\\{baseRemoteUrl\\}',baseRemoteUrl)
        }
        NCube ncube = NCube.fromSimpleJson(json)
        ncube.applicationID = appId
        ncubeRuntime.addCube(ncube)
        return ncube
    }
}