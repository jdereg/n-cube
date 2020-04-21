package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware

import static com.cedarsoftware.ncube.NCubeConstants.MANAGER_BEAN
import static com.cedarsoftware.ncube.NCubeConstants.NCUBE_CLIENT_BEAN
import static com.cedarsoftware.ncube.NCubeConstants.RUNTIME_BEAN

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

@CompileStatic
class NCubeAppContext implements ApplicationContextAware
{
    private static ApplicationContext ctx
    private static NCubeClient client
    private static NCubeRuntimeClient runtime
    private static NCubeMutableClient mutableClient

    static Object getBean(String beanName)
    {
        return ctx?.getBean(beanName)
    }

    static NCubeClient getNcubeClient()
    {
        if (client != null) {
            return client
        }

        String beanName = ctx.containsBean(RUNTIME_BEAN) ? RUNTIME_BEAN : MANAGER_BEAN
        return client = ctx.getBean(beanName) as NCubeClient
    }

    static NCubeRuntimeClient getNcubeRuntime()
    {
        if (runtime != null) {
            return runtime
        }

        return runtime = getBean(RUNTIME_BEAN) as NCubeRuntimeClient
    }

    static NCubeMutableClient getNcubeMutableClient()
    {
        if (mutableClient)
        {
            return mutableClient
        }
        mutableClient = getBean(RUNTIME_BEAN) as NCubeMutableClient
        return mutableClient
    }
    
    static NCubeTestServer getTestServer()
    {
        return getBean(MANAGER_BEAN) as NCubeTestServer
    }

    static boolean isTest()
    {
        return ctx.containsBean('hsqlSetup') as boolean
    }

    static boolean isClientTest()
    {
        return ctx.environment.activeProfiles.contains(NCUBE_CLIENT_BEAN)
    }

    void setApplicationContext(ApplicationContext applicationContext)
    {
        ctx = applicationContext
        client = null
        runtime = null
    }
}