package com.cedarsoftware.ncube

import com.cedarsoftware.controller.NCubeController
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.servlet.JsonCommandServlet
import com.cedarsoftware.util.JsonHttpProxy
import com.google.common.base.Joiner
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito

import javax.servlet.http.HttpServletRequest
import java.lang.reflect.Method

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.ncube.ReferenceAxisLoader.*
import static com.cedarsoftware.util.Converter.convertToLong
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
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
class TestJavascriptAPIs extends NCubeCleanupBaseTest
{
    private static final ApplicationID SYSAPP = new ApplicationID(ApplicationID.DEFAULT_TENANT, NCubeConstants.SYS_APP, ApplicationID.SYS_BOOT_VERSION, ReleaseStatus.SNAPSHOT.name(), 'FOO')
    private static final ApplicationID BRANCH1 = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'test', '1.28.0', ReleaseStatus.SNAPSHOT.name(), 'FOO')
    private static final ApplicationID BRANCH2 = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'test', '1.28.0', ReleaseStatus.SNAPSHOT.name(), 'BAR')

    @Before
    void setup()
    {
        super.setup()
        if (!NCubeAppContext.clientTest)
        {
            HttpServletRequest request = Mockito.mock(HttpServletRequest.class)
            Mockito.when(request.headerNames).thenReturn(new Enumeration<String>() {
                boolean hasMoreElements()
                {
                    return false
                }

                String nextElement()
                {
                    return null
                }
            })
            JsonCommandServlet.servletRequest.set(request)
        }
    }

    @Test
    void testControllerImplementsInterfaces()
    {
        // runtime methods removed because they are runtime only concerns (never go across the wire)
        Set<String> allowedRunTimeMethods = [
                'getCube(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String)',
                'addCube(class com.cedarsoftware.ncube.NCube)',
                'addCube(class com.cedarsoftware.ncube.NCube, boolean)',
                'getBootVersion(class java.lang.String, class java.lang.String)',
                'getSystemParams()',
                'getTestCauses(class java.lang.Throwable)',
                'getUrlClassLoader(class com.cedarsoftware.ncube.ApplicationID, interface java.util.Map)',
                'getLocalClassloader(class com.cedarsoftware.ncube.ApplicationID)',
                'getApplicationID(class java.lang.String, class java.lang.String, interface java.util.Map)',
                'getNCubeFromResource(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String)',
                'getNCubesFromResource(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String)',
                'addAdvice(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, interface com.cedarsoftware.ncube.Advice)',
                'getActualUrl(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, interface java.util.Map)',
                'getUrlContent(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, interface java.util.Map)',
                'clearCache(class com.cedarsoftware.ncube.ApplicationID, interface java.util.Collection)',
                'isCached(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String)',
                'getCacheForApp(class com.cedarsoftware.ncube.ApplicationID)',
                'isTrackBindingsOn()',
                'getCells(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, class [Ljava.lang.Object;, interface java.util.Map, interface java.util.Map)',
                'getCells(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, class [Ljava.lang.Object;, interface java.util.Map, interface java.util.Map, class java.lang.Object)'
        ] as Set

        // mutable methods removed because of transformed return type (usually Object[] for Javascript clients)
        Set<String> allowedMutableMethods = [
                'getCube(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String)',
                'updateCube(class com.cedarsoftware.ncube.NCube)',
                'getCellAnnotation(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, interface java.util.Set)',
                'getCellAnnotation(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, interface java.util.Set, boolean)',
                'mergeDeltas(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, interface java.util.List)',
                'assertPermissions(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, class com.cedarsoftware.ncube.Action)',
                'checkPermissions(class com.cedarsoftware.ncube.ApplicationID, class java.lang.String, class com.cedarsoftware.ncube.Action)'
        ] as Set

        Set<String> runtimeMethods = getMethods(NCubeRuntimeClient.class.methods)
        Set<String> mutableMethods = getMethods(NCubeMutableClient.class.methods)
        Set<String> controllerMethods = getMethods(NCubeController.class.methods)

        runtimeMethods.removeAll(allowedRunTimeMethods)
        runtimeMethods.removeAll(controllerMethods)
        assert runtimeMethods.empty

        mutableMethods.removeAll(allowedMutableMethods)
        mutableMethods.removeAll(controllerMethods)
        assert mutableMethods.empty
    }

    private static Set<String> getMethods(Method[] methods)
    {
        Set<String> methodNames = [] as Set
        for (Method method : methods)
        {
            methodNames.add(methodToString(method))
        }
        return methodNames
    }

    private static String methodToString(Method method)
    {
        StringBuilder sb = new StringBuilder(method.name)
        sb.append('(')
        sb.append(Joiner.on(', ').join(method.parameterTypes))
        sb.append(')')
        return sb.toString()
    }
    
    @Test
    void testGetJson()
    {
        if (NCubeAppContext.clientTest)
        {
            return
        }
        NCube ncube1 = NCubeBuilder.discrete1D
        NCube ncube2 = NCubeBuilder.discrete1D
        ncube1.applicationID = BRANCH1
        ncube2.applicationID = BRANCH2
        mutableClient.createCube(ncube1)
        String cubeName = ncube1.name

        Map<String, Object> args = [
                (REF_TENANT): BRANCH1.tenant,
                (REF_APP): BRANCH1.app,
                (REF_VERSION): BRANCH1.version,
                (REF_STATUS): BRANCH1.status,
                (REF_BRANCH): BRANCH1.branch,
                (REF_CUBE_NAME): cubeName,
                (REF_AXIS_NAME): 'state'
        ] as Map<String, Object>

        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(cubeName, 'stateSource', args)
        Axis axis = new Axis('stateSource', 1, false, refAxisLoader)
        ncube2.addAxis(axis)
        mutableClient.createCube(ncube2)

        // test valid cube json calls
        String json = call('getJson', [BRANCH2, cubeName, [mode:'json']])
        assert hasContent(json)
        json = call('getJson', [BRANCH2, cubeName, [mode:'json-index']])
        assert hasContent(json)

        mutableClient.deleteBranch(BRANCH1)
        call('clearCache', [BRANCH2])

        // invalid cube json, but raw json should still load
        json = call('getJson', [BRANCH2, cubeName, [mode:'json']])
        assert hasContent(json)

        try
        { // invalid cube json should cause exception
            call('getJson', [BRANCH2, cubeName, [mode:'json-index']])
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'error reading cube from stream')
        }
    }

    @Test
    void testCommitCube()
    {
        NCube cube = NCubeBuilder.discrete1D
        cube.applicationID = BRANCH1
        mutableClient.createCube(cube)
        call('commitCube', [BRANCH1, cube.name])
        List<NCubeInfoDto> dtos = mutableClient.search(BRANCH1, cube.name, null, null)
        assert 1 == dtos.size()
    }

    @Test
    void testRuntimeSearchRemovesExistingSearchClosure()
    {
        if (!NCubeAppContext.clientTest)
        {
            return
        }
        NCube cube = NCubeBuilder.discrete1D
        cube.applicationID = BRANCH1
        mutableClient.createCube(cube)
        List<NCube> cubeList = []
        Map options = [:]
        options[SEARCH_CLOSURE] = { NCubeInfoDto dto, List<NCube> cubes ->
            NCube ncube = NCube.createCubeFromRecord(dto)
            cubes.add(ncube)
        }
        options[SEARCH_OUTPUT] = cubeList
        List<NCubeInfoDto> dtos = mutableClient.search(BRANCH1, cube.name, null, options)
        assert 1 == dtos.size()
        assert cubeList.empty
    }

    @Test
    void testIsCubeUpToDate()
    {
        NCube ncube1 = NCubeBuilder.discrete1D
        NCube ncube2 = NCubeBuilder.discrete1D
        ncube1.applicationID = BRANCH1
        mutableClient.createCube(ncube1)
        ncube2.applicationID = BRANCH2
        mutableClient.createCube(ncube2)

        assert mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // new state (cube not in HEAD) is considered up-to-date
        mutableClient.commitBranch(ncube1.applicationID)

        assert !mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)    // Not up-to-date because BRANCH2 created cube (no HEAD sha1) which matches a cube in HEAD

        assert mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // same as HEAD, up-to-date
        assert !mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)    // same as HEAD, but no HEAD SHA1

        mutableClient.updateBranch(BRANCH2)                                       // pick up changes from HEAD
        assert mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)    // same as HEAD, with HEAD-SHA1 now on branch cube

        ncube2.addColumn('state', 'AL')
        mutableClient.updateCube(ncube2)
        mutableClient.commitBranch(BRANCH2)
        assert !mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // out of date
        assert mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)

        mutableClient.updateBranch(BRANCH1)                                       // pick up changes from HEAD
        assert mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // up to date

        assert mutableClient.isCubeUpToDate(BRANCH1.asHead(), ncube1.name)          // HEAD is always true
    }

    @Test
    void testHeartBeat()
    {
        Map health = call('heartBeat', [[:]]) as Map
        assert health
        Map stats = health.compareResults as Map
        assert stats.isEmpty()
    }

    @Test
    void testGetApplicationID()
    {
        System.setProperty("NCUBE_PARAMS", '{"branch":"jose"}')

        ApplicationID appId = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'ncube.test', '0.0.0', 'SNAPSHOT', 'jose')
        createCubeFromResource(appId, "sys.bootstrap.test.1.json")

        ApplicationID bootId = ncubeRuntime.getApplicationID(appId.tenant, appId.app, [env:null] as Map) as ApplicationID
        assert bootId.tenant == 'NONE'
        assert bootId.app == 'ncube.test'
        assert bootId.version == '1.28.0'
        assert bootId.status == 'RELEASE'
        assert bootId.branch == 'HEAD'
    }
    
    @Test
    void testFetchJsonRevDiffs()
    {
        createCubeFromResource(BRANCH1, 'test.branch.1.json')
        List<NCubeInfoDto> cubes = ncubeRuntime.search(BRANCH1, 'TestBranch', null, null)
        assert cubes.size() == 1
        NCubeInfoDto origDto = cubes[0]
        long origId = convertToLong(origDto.id)
        NCube foo = ncubeRuntime.getNCubeFromResource(BRANCH1, 'test.branch.2.json')
        mutableClient.updateCube(foo)
        List<NCubeInfoDto> cubes2 = ncubeRuntime.search(BRANCH1, 'TestBranch', null, null)
        assert cubes2.size() == 1
        NCubeInfoDto newDto = cubes2[0]
        long newId = convertToLong(newDto.id)

        List<Delta> result = call('fetchJsonRevDiffs', [newId, origId]) as List
        assert result.size() == 4
    }

    @Test
    void testFetchJsonRevDiffsWrongTenant()
    {
        if (NCubeAppContext.clientTest)
        { // You can't spoof an ApplicationID in a client test. The storage server controller will put the correct one on.
            return
        }
        ApplicationID acme1 = new ApplicationID('Acme', BRANCH1.app, BRANCH1.version, BRANCH1.status, BRANCH1.branch)
        createCubeFromResource(acme1, 'test.branch.1.json')
        List<NCubeInfoDto> cubes = ncubeRuntime.search(acme1, 'TestBranch', null, null)
        assert cubes.size() == 1
        NCubeInfoDto origDto = cubes[0]
        long origId = convertToLong(origDto.id)
        NCube foo = ncubeRuntime.getNCubeFromResource(acme1, 'test.branch.2.json')
        mutableClient.updateCube(foo)
        List<NCubeInfoDto> cubes2 = ncubeRuntime.search(acme1, 'TestBranch', null, null)
        assert cubes2.size() == 1
        NCubeInfoDto newDto = cubes2[0]
        long newId = convertToLong(newDto.id)

        try
        {
            call('fetchJsonRevDiffs', [newId, origId]) as List
            fail()
        }
        catch (SecurityException e)
        {
            assertContainsIgnoreCase(e.message, 'not performed', 'permission for cube', 'user:')
        }
    }

    @Test
    void testFetchJsonBranchDiffs()
    {
        createCubeFromResource(BRANCH1, 'test.branch.1.json')
        createCubeFromResource(BRANCH2, 'test.branch.2.json')
        List<NCubeInfoDto> cubes = ncubeRuntime.search(BRANCH1, 'TestBranch', null, null)
        assert cubes.size() == 1
        NCubeInfoDto origDto = cubes[0]
        List<NCubeInfoDto> cubes2 = ncubeRuntime.search(BRANCH2, 'TestBranch', null, null)
        assert cubes2.size() == 1
        NCubeInfoDto newDto = cubes2[0]

        List<Delta> result = call('fetchJsonBranchDiffs', [newDto, origDto]) as List
        assert result.size() == 4
    }

    @Test
    void testGetCellBadInput()
    {
        try
        {
            createCubeFromResource(BRANCH1, 'test.branch.1.json')
            NCube foo = ncubeRuntime.getCube(BRANCH1, 'TestBranch')
            assert foo != null  // Prove we have stored a valid n-cube

            // Prove that bad AppId causes an exception that is better than NPE.
            ncubeRuntime.getCell(BRANCH1.asBranch('quux'), 'test.branch.1.json', [:], null)
            fail()
        }
        catch(IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'cannot getCell', 'unable', 'load')
        }
    }

    @Test
    void testGetCellsBadInput()
    {
        try
        {
            createCubeFromResource(BRANCH1, 'test.branch.1.json')
            NCube foo = ncubeRuntime.getCube(BRANCH1, 'TestBranch')
            assert foo != null  // Prove we have stored a valid n-cube

            // Prove that bad AppId causes an exception that is better than NPE.
            ncubeRuntime.getCells(BRANCH1.asBranch('quux'), 'test.branch.1.json', [] as Object[], [:])
            fail()
        }
        catch(IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'unable', 'fetch', 'requested', 'cells')
        }
    }

    @Test
    void testGetCellNoExecuteByCoordinate()
    {
        NCube cube = createCubeFromResource(BRANCH1, 'test.branch.1.json')
        String cubeCall = cube.getCellNoExecute([Code:0]) as String
        CellInfo apiCall = call('getCellNoExecuteByCoordinate', [BRANCH1, cube.name, [Code:0]]) as CellInfo
        assert cubeCall == apiCall.value

        try
        {
            call('getCellNoExecuteByCoordinate', [BRANCH1, cube.name, [Code:1]])
            fail()
        }
        catch(CoordinateNotFoundException e)
        {
            assertContainsIgnoreCase(e.message, 'not found on axis')
        }
    }

    @Test
    void testGetMenuDefault()
    {
        try
        {
            Map menu = call('getMenu', [BRANCH1]) as Map
            assert MENU_TITLE_DEFAULT == menu[MENU_TITLE]
            assert 6 == (menu[MENU_TAB] as Map).size()
            assert (menu[MENU_NAV] as Map).isEmpty()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'user code', 'cannot', 'executed', 'attempted', 'getMenu')
        }
    }

    @Test
    void testGetMenuAppOnly()
    {
        if (NCubeAppContext.clientTest)
        {
            return
        }
        createCubeFromResource(BRANCH1.asBootVersion(), 'sysMenu1.json')
        Map menu = call('getMenu', [BRANCH1]) as Map
        assert 'App Title' == menu[MENU_TITLE]

        Map tabMenu = menu[MENU_TAB] as Map
        assert 1 == tabMenu.size()
        assert tabMenu.containsKey('Visualizer')

        Map navMenu = menu[MENU_NAV] as Map
        assert 3 == navMenu.size()

        Map nav1 = navMenu['Nav1'] as Map
        assert 1 == nav1.size()
        assert nav1.containsKey('App Plugin')

        Map nav2 = navMenu['Nav2'] as Map
        assert 1 == nav2.size()
        assert nav2.containsKey('Plugin 2')
        assert 'somethingElse' == ((nav2['Plugin 2'] as Map)['expression'] as Map)['method']

        Map nav3 = navMenu['Nav3'] as Map
        assert 1 == nav3.size()
        assert nav3.containsKey('App Plugin')
    }

    @Test
    void testGetMenuSystemOnly()
    {
        if (NCubeAppContext.clientTest)
        {
            return
        }
        createCubeFromResource(SYSAPP, 'globalSysMenu1.json')
        mutableClient.commitBranch(SYSAPP)
        Map menu = call('getMenu', [BRANCH1]) as Map
        assert 'Global Title' == menu[MENU_TITLE]

        Map tabMenu = menu[MENU_TAB] as Map
        assert 5 == tabMenu.size()
        assert tabMenu.containsKey('n-cube')
        assert tabMenu.containsKey('n-cube-old')
        assert tabMenu.containsKey('JSON')
        assert tabMenu.containsKey('Details')
        assert tabMenu.containsKey('Test')
        assert !tabMenu.containsKey('Visualizer')

        Map navMenu = menu[MENU_NAV] as Map
        assert 2 == navMenu.size()

        Map nav1 = navMenu['Nav1'] as Map
        assert 1 == nav1.size()
        assert nav1.containsKey('Global Plugin')

        Map nav2 = navMenu['Nav2'] as Map
        assert 1 == nav2.size()
        assert nav2.containsKey('Plugin 2')
        assert 'searchForString' == ((nav2['Plugin 2'] as Map)['expression'] as Map)['method']
    }

    @Test
    void testGetMenuAppAndSystem()
    {
        if (NCubeAppContext.clientTest)
        {
            return
        }
        createCubeFromResource(BRANCH1.asBootVersion(), 'sysMenu1.json')
        createCubeFromResource(SYSAPP, 'globalSysMenu1.json')
        mutableClient.commitBranch(SYSAPP)
        Map menu = call('getMenu', [BRANCH1]) as Map
        assert 'App Title' == menu[MENU_TITLE]

        Map tabMenu = menu[MENU_TAB] as Map
        assert 6 == tabMenu.size()
        assert tabMenu.containsKey('n-cube')
        assert tabMenu.containsKey('n-cube-old')
        assert tabMenu.containsKey('JSON')
        assert tabMenu.containsKey('Details')
        assert tabMenu.containsKey('Test')
        assert tabMenu.containsKey('Visualizer')

        Map navMenu = menu[MENU_NAV] as Map
        assert 3 == navMenu.size()

        Map nav1 = navMenu['Nav1'] as Map
        assert 2 == nav1.size()
        assert nav1.containsKey('App Plugin')
        assert nav1.containsKey('Global Plugin')

        Map nav2 = navMenu['Nav2'] as Map
        assert 1 == nav2.size()
        assert nav2.containsKey('Plugin 2')
        assert 'somethingElse' == ((nav2['Plugin 2'] as Map)['expression'] as Map)['method']

        Map nav3 = navMenu['Nav3'] as Map
        assert 1 == nav3.size()
        assert nav3.containsKey('App Plugin')
    }

    @Test
    void testExecute()
    {
        createCubeFromResource(BRANCH1, 'test.execute.json')
        try
        {
            def result = call('execute', [BRANCH1, 'test.execute', 'plus', [value: 2.0d, term: 3.0d]])
            assert result != null
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'user code', 'cannot', 'executed', 'attempted', 'execute')
        }
    }

    @Test
    void testCheckPermissions()
    {
        String read = Action.READ.name()
        String update = Action.UPDATE.name()
        NCube cube = createCubeFromResource(BRANCH1, 'test.branch.1.json')
        Map perms = call('checkMultiplePermissions', [BRANCH1, cube.name, [read, update].toArray()]) as Map
        assertTrue perms[read] as Boolean
        assertTrue perms[update] as Boolean
    }

    @Test
    void testMergePullRequestWhenBranchDeleted()
    {
        NCube ncube = createCubeFromResource(BRANCH1, 'test.branch.1.json')
        List<NCubeInfoDto> dtos = mutableClient.search(BRANCH1, ncube.name, null, null)
        String prId = mutableClient.generatePullRequestHash(BRANCH1, dtos.toArray())

        mutableClient.deleteBranch(BRANCH1)

        try
        {
            call('mergePullRequest', [prId])
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'branch', 'request', 'obsolete', 'requested', 'applicationid')
            Map<String, Object> pr = mutableClient.pullRequests.first() as Map
            assert pr[PR_STATUS] == PR_OBSOLETE
        }
    }

    @Test
    void testMergePullRequestCubeNotExist()
    {
        NCube ncube = createCubeFromResource(BRANCH1, 'test.branch.1.json')
        List<NCubeInfoDto> dtos = mutableClient.search(BRANCH1, ncube.name, null, null)
        String prId = mutableClient.generatePullRequestHash(BRANCH1, dtos.toArray())

        mutableClient.deleteBranch(BRANCH1)
        createCubeFromResource(BRANCH1, 'test.branch.age.1.json')

        try
        {
            call('mergePullRequest', [prId])
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'cube', 'valid', 'request', 'obsolete', 'requested', 'applicationid')
            Map<String, Object> pr = mutableClient.pullRequests.first() as Map
            assert pr[PR_STATUS] == PR_OBSOLETE
        }
    }

    @Test
    void testMergePullRequestCubeChanged()
    {
        NCube ncube = createCubeFromResource(BRANCH1, 'test.branch.1.json')
        List<NCubeInfoDto> dtos = mutableClient.search(BRANCH1, ncube.name, null, null)
        String prId = mutableClient.generatePullRequestHash(BRANCH1, dtos.toArray())
        ncube.setCell('FOO', [Code : 10])
        mutableClient.updateCube(ncube)

        try
        {
            call('mergePullRequest', [prId])
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'cube', 'changed', 'request', 'obsolete', 'requested', 'applicationid')
            Map<String, Object> pr = mutableClient.pullRequests.first() as Map
            assert pr[PR_STATUS] == PR_OBSOLETE
        }
    }

    @Test
    void testMapReduce()
    {
        try
        {
            // Validate test when combined
            createCubeFromResource(BRANCH1, 'sys.classpath.tests.json')
            createCubeFromResource(BRANCH1, 'selectQueryTest.json')
            Map queryResult = call('mapReduce', [BRANCH1, 'Test.Select', 'query', "{ Map input -> input.foo == 'AL' }", [:]]) as Map

            assert queryResult.size() == 1

            Map row = queryResult['A'] as Map
            assert row['foo'] == 'AL'
            assert row['bar'] == 123
        }
        catch (IllegalStateException e)
        {   // When running as separate client and server processes...
            assertContainsIgnoreCase(e.message, 'user code', 'cannot', 'executed', 'attempted', 'mapReduce')
        }
    }

    @Test
    void testMapReduceBadInput()
    {
        try
        {
            createCubeFromResource(BRANCH1, 'sys.classpath.tests.json')
            createCubeFromResource(BRANCH1, 'selectQueryTest.json')
            call('mapReduce', [BRANCH1.asBranch('garply'), 'Test.Select', 'query', "{ Map input -> input.foo == 'AL' }", [:]]) as Map
            fail("should not make it here")
        }
        catch (IllegalArgumentException e)
        {   // When running combined...
            assertContainsIgnoreCase(e.message, 'Cannot mapReduce', 'Unable', 'load cube')
        }
        catch (IllegalStateException e)
        {   // When running as separate client and server processes
            assertContainsIgnoreCase(e.message, 'user code', 'cannot', 'executed', 'attempted', 'mapReduce')
        }
    }

    @Test
    void testPromoteRevision()
    {
        // setup NCube and verify initial cell value
        Map coord = [Code: -10]
        NCube ncube = createCubeFromResource(BRANCH1, 'test.branch.1.json')
        String code = ncube.getCell(coord)
        assert code == 'ABC'

        // update cell and verify value changed
        call('updateCellAt', [BRANCH1, ncube.name, coord, new CellInfo('XYZ')])
        NCubeInfoDto record1 = call('loadCubeRecord', [BRANCH1, ncube.name, [:]]) as NCubeInfoDto
        ncube = NCube.createCubeFromRecord(record1)
        code = ncube.getCell(coord)
        assert code == 'XYZ'

        // get revisions, promote original revision and verify cell value
        List<NCubeInfoDto> revisions = call('getRevisionHistory', [BRANCH1, ncube.name]) as List<NCubeInfoDto>
        assert revisions.size() == 2
        NCubeInfoDto record0 = revisions.find { it.revision == '0' }
        long id = convertToLong(record0.id)
        call('promoteRevision', [id])
        revisions = call('getRevisionHistory', [BRANCH1, ncube.name]) as List
        assert revisions.size() == 3
        NCubeInfoDto record2 = call('loadCubeRecord', [BRANCH1, ncube.name, [:]]) as NCubeInfoDto
        ncube = NCube.createCubeFromRecord(record2)
        code = ncube.getCell(coord)
        assert code == 'ABC'
    }

    private static Object call(String methodName, List args)
    {
        if (NCubeAppContext.clientTest)
        {
            JsonHttpProxy proxy = NCubeAppContext.getBean('callableBean') as JsonHttpProxy
            proxy.invokeMethod('call', ['ncubeController', methodName, args])
        }
        else
        {
            NCubeController controller = NCubeAppContext.getBean(CONTROLLER_BEAN) as NCubeController
            controller.invokeMethod(methodName, args)
        }
    }
}
