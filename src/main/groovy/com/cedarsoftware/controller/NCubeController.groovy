package com.cedarsoftware.controller

import com.cedarsoftware.ncube.Action
import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.AxisType
import com.cedarsoftware.ncube.AxisValueType
import com.cedarsoftware.ncube.CellInfo
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.Delta
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeConstants
import com.cedarsoftware.ncube.NCubeInfoDto
import com.cedarsoftware.ncube.NCubeManager
import com.cedarsoftware.ncube.NCubeMutableClient
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.ncube.NCubeTest
import com.cedarsoftware.ncube.ReferenceAxisLoader
import com.cedarsoftware.ncube.ReleaseStatus
import com.cedarsoftware.ncube.util.VersionComparator
import com.cedarsoftware.servlet.JsonCommandServlet
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.InetAddressUtilities
import com.cedarsoftware.util.ThreadAwarePrintStream
import com.cedarsoftware.util.ThreadAwarePrintStreamErr
import com.cedarsoftware.util.io.JsonObject
import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import com.google.common.util.concurrent.AtomicDouble
import groovy.transform.CompileStatic
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.info.InfoEndpoint
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.bind.annotation.RestController

import javax.management.MBeanServer
import javax.management.ObjectName
import javax.servlet.http.HttpServletRequest
import java.lang.management.ManagementFactory
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.NCubeAppContext.testServer
import static com.cedarsoftware.ncube.ReferenceAxisLoader.*
import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.Converter.convertToLong
import static com.cedarsoftware.util.Converter.convertToString
import static com.cedarsoftware.util.StringUtilities.isEmpty

/**
 * NCubeController API.
 *
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
@RestController
class NCubeController implements NCubeConstants
{
    @Autowired
    private InfoEndpoint infoEndpoint

    @Autowired(required = false)
    NCubeManager ncubeManager   // optional (on runtime-server will be null, on combined-server and storage-server will be valid)
    
    @Value('${server.tomcat.max-connections:1000}') int tomcatMaxConnections
    @Value('${server.tomcat.max-threads:200}') int tomcatMaxThreads

    private static final Pattern IS_NUMBER_REGEX = ~/^[\d,.e+-]+$/
    private static final Pattern NO_QUOTES_REGEX = ~/"/

    private NCubeMutableClient mutableClient
    private static String servletHostname = null
    private static String inetHostname = null
    private static AtomicDouble processLoadPeak = new AtomicDouble(0.0d)
    private static AtomicDouble systemLoadPeak = new AtomicDouble(0.0d)

    private static final Map NO_CELL = [type:null, value:null]
    private static final String EXECUTE_ERROR = 'User code cannot be executed on this server. Attempted method: '
    private final boolean allowExecute

    NCubeController(NCubeMutableClient mutableClient, boolean allowExecute)
    {
        System.out = new ThreadAwarePrintStream(System.out)
        System.err = new ThreadAwarePrintStreamErr(System.err)
        this.mutableClient = mutableClient
        this.allowExecute = allowExecute
    }

    protected String getUserForDatabase()
    {
        String user = null
        String principal = SecurityContextHolder.context?.authentication?.principal

        if (!isEmpty(principal))
        {
            user = principal.split('@').first()
        }

        if (isEmpty(user))
        {
            HttpServletRequest request = JsonCommandServlet.servletRequest.get()
            Enumeration e = request.headerNames
            while (e.hasMoreElements())
            {
                String headerName = (String) e.nextElement()
                if ('smuser'.equalsIgnoreCase(headerName))
                {
                    user = request.getHeader(headerName)
                    break
                }
            }
        }

        if (isEmpty(user))
        {
            user = System.getProperty('user.name')
        }

        if (ncubeManager)
        {
            ncubeManager.userId = user
        }
        return user
    }

    // ============================================= Begin API =========================================================

    @SuppressWarnings("GroovyUnusedDeclaration")
    String getUserId()
    {
        return mutableClient.userId
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean assertPermissions(ApplicationID appId, String resource, String actionName)
    {
        appId = addTenant(appId)
        Action action = actionName == null ? null : Action.valueOf(actionName.toUpperCase())
        return mutableClient.assertPermissions(appId, resource, action)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map checkMultiplePermissions(ApplicationID appId, String resource, Object[] actions)
    {
        if (ArrayUtilities.isEmpty(actions))
        {
            throw new IllegalArgumentException('Must send at least action to check permissions')
        }
        appId = addTenant(appId)
        for (int i = 0; i < actions.length; i++)
        {
            actions[i] = Action.valueOf((actions[i] as String).toUpperCase())
        }
        return mutableClient.checkMultiplePermissions(appId, resource, actions)
    }

    Boolean checkPermissions(ApplicationID appId, String resource, String actionName)
    {
        appId = addTenant(appId)
        Action action = actionName == null ? null : Action.valueOf(actionName.toUpperCase())
        return mutableClient.checkPermissions(appId, resource, action)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean isSysAdmin()
    {
        return mutableClient.isSysAdmin()
    }

    Boolean isAppAdmin(ApplicationID appId)
    {
        appId = addTenant(appId)
        return mutableClient.isAppAdmin(appId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String getAppLockedBy(ApplicationID appId)
    {
        appId = addTenant(appId)
        return mutableClient.getAppLockedBy(appId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean isAppLocked(ApplicationID appId)
    {
        appId = addTenant(appId)
        String lockedBy = mutableClient.getAppLockedBy(appId)
        return lockedBy != null
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean lockApp(ApplicationID appId, boolean shouldLock)
    {
        appId = addTenant(appId)
        return mutableClient.lockApp(appId, shouldLock)
    }

    Integer moveBranch(ApplicationID appId, String newSnapVer)
    {
        appId = addTenant(appId)
        return mutableClient.moveBranch(appId, newSnapVer)
    }

    Integer releaseVersion(ApplicationID appId, String newSnapVer = null)
    {
        appId = addTenant(appId)
        int rowCount = mutableClient.releaseVersion(appId, newSnapVer)
        return rowCount
    }

    Object[] search(ApplicationID appId, String cubeNamePattern = null, String content = null, Map options = [(SEARCH_ACTIVE_RECORDS_ONLY):true])
    {
        appId = addTenant(appId)
        if (options)
        {
            options.remove(SEARCH_CLOSURE)
            options.remove(SEARCH_OUTPUT)
        }

        List<NCubeInfoDto> cubeInfos = mutableClient.search(appId, cubeNamePattern, content, options)
        return cubeInfos as Object[]
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Integer getSearchCount(ApplicationID appId, String cubeNamePattern = null, String content = null, Map options = [(SEARCH_ACTIVE_RECORDS_ONLY):true])
    {
        return search(appId, cubeNamePattern, content, options).length
    }

    Boolean restoreCubes(ApplicationID appId, Object[] cubeNames)
    {
        appId = addTenant(appId)
        return mutableClient.restoreCubes(appId, cubeNames)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion = false)
    {
        appId = addTenant(appId)
        List<NCubeInfoDto> cubeInfos = mutableClient.getRevisionHistory(appId, cubeName, ignoreVersion)
        return cubeInfos.toArray()
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getCellAnnotation(ApplicationID appId, String cubeName, Object[] ids, boolean ignoreVersion = false)
    {
        appId = addTenant(appId)
        List<NCubeInfoDto> cubeInfos = mutableClient.getCellAnnotation(appId, cubeName, getCoordinate(ids), ignoreVersion)
        return cubeInfos.toArray()
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String getHtml(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        // The Strings below are hints to n-cube to tell it which axis to place on top
        String html = toHtmlWithColumnHints(ncube)
        return html
    }

    private static String toHtmlWithColumnHints(NCube ncube)
    {
        ncube.toHtml('trait', 'traits', 'businessDivisionCode', 'bu', 'month', 'months', 'col', 'column', 'cols', 'columns', 'attribute', 'attributes')
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String getJson(ApplicationID appId, String cubeName)
    {
        return getJson(appId, cubeName, [mode:"json-index"])
    }

    NCubeInfoDto loadCubeRecord(ApplicationID appId, String cubeName, Map options)
    {
        appId = addTenant(appId)
        NCubeInfoDto record = mutableClient.loadCubeRecord(appId, cubeName, options)
        return record
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    NCubeInfoDto loadCubeRecordById(long id, Map options = null)
    {
        NCubeInfoDto record = mutableClient.loadCubeRecordById(id, options)
        return record
    }

    String getJson(ApplicationID appId, String cubeName, Map options)
    {
        appId = addTenant(appId)
        String json = mutableClient.getJson(appId, cubeName, options)
        return json
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map<String, Object> getVisualizerGraph(ApplicationID appId, Map options)
    {
        verifyAllowExecute('getVisualizerGraph')
        appId = addTenant(appId)
        Map graph = runtimeClient.getVisualizerGraph(appId, options)
        return graph
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map<String, Object> getVisualizerScopeChange(ApplicationID appId, Map options)
    {
        verifyAllowExecute('getVisualizerScopeChange')
        appId = addTenant(appId)
        Map graph = runtimeClient.getVisualizerScopeChange(appId, options)
        return graph
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map<String, Object>  getVisualizerNodeDetails(ApplicationID appId, Map options)
    {
        verifyAllowExecute('getVisualizerNodeDetails')
        appId = addTenant(appId)
        Map graph = runtimeClient.getVisualizerNodeDetails(appId, options)
        return graph
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean updateCubeMetaProperties(ApplicationID appId, String cubeName, Map<String, Object> newMetaProperties)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        ncube.clearMetaProperties()
        ncube.addMetaProperties(newMetaProperties)
        mutableClient.updateCube(ncube)
        return true
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map getCubeMetaProperties(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        return valuesToCellInfo(ncube.metaProperties)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)
    {
        appId = addTenant(appId)
        mutableClient.updateAxisMetaProperties(appId, cubeName, axisName, newMetaProperties)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map getAxisMetaProperties(ApplicationID appId, String cubeName, String axisName)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, null)
        NCube ncube = getCubeInternal(appId, cubeName)
        Axis axis = ncube.getAxis(axisName)
        return valuesToCellInfo(axis.metaProperties)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean updateColumnMetaProperties(ApplicationID appId, String cubeName, String axisName, long colId, Map<String, Object> newMetaProperties)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        NCube ncube = getCubeInternal(appId, cubeName)
        Axis axis = ncube.getAxis(axisName)
        Column column = axis.getColumnById(colId)
        column.clearMetaProperties()
        column.addMetaProperties(newMetaProperties)
        ncube.clearSha1()
        mutableClient.updateCube(ncube)
        return true
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map getColumnMetaProperties(ApplicationID appId, String cubeName, String axisName, long colId)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, null)
        NCube ncube = getCubeInternal(appId, cubeName)
        Axis axis = ncube.getAxis(axisName)
        Column col = axis.getColumnById(colId)
        return valuesToCellInfo(col.metaProperties)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map mapReduce(ApplicationID appId, String cubeName, String colAxisName, String where = 'true', Map options = [:])
    {
        verifyAllowExecute('mapReduce')
        appId = addTenant(appId)
        Map result = runtimeClient.mapReduce(appId, cubeName, colAxisName, where, options)
        return result
    }

    private static Map<String, CellInfo> valuesToCellInfo(Map<String, Object> metaProps)
    {
        Map<String, CellInfo> map = new CompactCILinkedMap<>()
        for (item in metaProps.entrySet())
        {
            if (item.value instanceof CellInfo)
            {
                CellInfo cellInfo = (CellInfo) item.value
                cellInfo.collapseToUiSupportedTypes()       // byte/short/int => long, float => double
                map.put(item.key, cellInfo)
            }
            else
            {
                CellInfo cellInfo = new CellInfo(item.value)
                cellInfo.collapseToUiSupportedTypes()       // byte/short/int => long, float => double
                map.put(item.key, cellInfo)
            }
        }
        return map
    }

    // TODO: Filter APP names by Access Control List data
    Object[] getAppNames()
    {
        // TODO: Snag tenant based on authentication
        String tenantName = tenant
        ApplicationID.validateTenant(tenantName)

        Object[] appNames = mutableClient.appNames
        if (appNames.length == 0)
        {
            ApplicationID defaultAppId = new ApplicationID(tenantName, ApplicationID.DEFAULT_APP, '1.0.0', ReleaseStatus.SNAPSHOT.name(), 'DEFAULT_BRANCH')
            createCube(defaultAppId, 'defaultNewAppCube')
        }
        return appNames
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getAppVersions(String app)
    {
        getAppVersions(app, null)
    }

    Object[] getAppVersions(String app, String status)
    {
        ApplicationID.validateApp(app)
        Object[] vers = getVersions(app)
        if (ArrayUtilities.isEmpty(vers))
        {
            return vers
        }

        // Filter out duplicates using Set, order by VersionComparator, remove trailing '-SNAPSHOT' and '-RELEASE'
        Set<String> versions = new TreeSet<>(new VersionComparator())
        for (int i = 0; i < vers.length; i++)
        {
            String mvnVer = vers[i] as String
            String[] verArr = mvnVer.split('-')
            if (status == null || verArr[1] == status)
            {
                versions.add(verArr[0])
            }
        }
        return versions.toArray()
    }

    Object[] getVersions(String app)
    {
        ApplicationID.validateApp(app)
        Object[] versions = mutableClient.getVersions(app)
        return versions
    }

    /**
     * Create an n-cube (SNAPSHOT only) for non-Java clients.
     */
    void createCube(ApplicationID appId, String cubeName)
    {
        NCube ncube = new NCube(cubeName)
        ncube.applicationID = appId
        Axis cols = new Axis("Column", AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        cols.addColumn("A")
        cols.addColumn("B")
        cols.addColumn("C")
        cols.addColumn("D")
        cols.addColumn("E")
        cols.addColumn("F")
        cols.addColumn("G")
        cols.addColumn("H")
        cols.addColumn("I")
        cols.addColumn("J")
        Axis rows = new Axis("Row", AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY, 2)
        rows.addColumn(1)
        rows.addColumn(2)
        rows.addColumn(3)
        rows.addColumn(4)
        rows.addColumn(5)
        rows.addColumn(6)
        rows.addColumn(7)
        rows.addColumn(8)
        rows.addColumn(9)
        rows.addColumn(10)
        ncube.addAxis(cols)
        ncube.addAxis(rows)
        createCube(ncube)
    }

    /**
     * Create an n-cube (SNAPSHOT only) for Java clients.
     */
    void createCube(NCube ncube)
    {
        ApplicationID appId = ncube.applicationID
        if (!appId)
        {
            throw new IllegalArgumentException("New n-cube: ${ncube.name} must have an ApplicationID")
        }
        appId = addTenant(appId)
        ncube.applicationID = appId
        mutableClient.createCube(ncube)
    }

    void createCube(ApplicationID appId, String cubeName, byte[] cubeBytes)
    {
        if (!appId)
        {
            throw new IllegalArgumentException("New n-cube: ${cubeName} must have an ApplicationID")
        }
        appId = addTenant(appId)
        mutableClient.createCube(appId, cubeName, cubeBytes)
    }

    Boolean updateCube(ApplicationID appId, String cubeName, byte[] cubeBytes)
    {
        addTenant(appId)
        return mutableClient.updateCube(appId, cubeName, cubeBytes)
    }

    /**
     * Delete an n-cube (SNAPSHOT only).
     * @return boolean true if successful, otherwise a String error message.
     */
    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames)
    {
        if (ArrayUtilities.isEmpty(cubeNames))
        {
            throw new IllegalArgumentException('Must send at least one cube name')
        }
        appId = addTenant(appId)

        if (!mutableClient.deleteCubes(appId, cubeNames))
        {
            throw new IllegalArgumentException("Cannot delete RELEASE n-cube.")
        }
        return true
    }

    /**
     * Find all references from (out going) an n-cube.
     * @return Object[] of String cube names that the passed in (named) cube references,
     * otherwise a String error message.
     */
    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getReferencesFrom(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        Set<String> references = mutableClient.getReferencesFrom(appId, cubeName)
        Object[] refs = references.toArray()
        caseInsensitiveSort(refs)
        return refs
    }

    /**
     * Find all referenced input variables for a given n-cube (and through any n-cubes it
     * references).
     * @return Object[] of String names of each scope variable, otherwise a String error message.
     */
    Object[] getRequiredScope(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        Set<String> refs = ncube.getRequiredScope([:], [:])
        Object[] scopeKeys = refs.toArray()
        caseInsensitiveSort(scopeKeys)
        return scopeKeys
    }

    /**
     * Duplicate the passed in cube, but change the name to newName AND the status of the new
     * n-cube will be SNAPSHOT.
     */
    Boolean duplicate(ApplicationID appId, ApplicationID destAppId, String cubeName, String newName)
    {
        appId = addTenant(appId)
        destAppId = addTenant(destAppId)
        return mutableClient.duplicate(appId, destAppId, cubeName, newName)
    }

    /**
     * Release the passed in SNAPSHOT version (update their status_cd to RELEASE), and then
     * duplicate all the n-cubes in the release, creating new ones in SNAPSHOT status with
     * the version number set to the newSnapVer.
     */
    Integer releaseCubes(ApplicationID appId, String newSnapVer = null)
    {
        appId = addTenant(appId)
        int rowCount = mutableClient.releaseCubes(appId, newSnapVer)
        return rowCount
    }

    /**
     * Change the SNAPSHOT version number of an n-cube.
     */
    @SuppressWarnings("GroovyUnusedDeclaration")
    void changeVersionValue(ApplicationID appId, String newSnapVer)
    {
        appId = addTenant(appId)
        mutableClient.changeVersionValue(appId, newSnapVer)
    }

    /**
     * Add axis to an existing SNAPSHOT n-cube.
     */
    void addAxis(ApplicationID appId, String cubeName, String axisName, String type, String valueType, Map<String, Boolean> axisOpts = [hasDefault:true, isSorted:false, fireAll:false])
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        if (isEmpty(axisName))
        {
            throw new IllegalArgumentException("Axis name cannot be empty.")
        }

        NCube ncube = getCubeInternal(appId, cubeName)

        long maxId = -1
        Iterator<Axis> i = ncube.axes.iterator()
        while (i.hasNext())
        {
            Axis axis = i.next()
            if (axis.id > maxId)
            {
                maxId = axis.id
            }
        }
        int sortedVal = axisOpts.isSorted ? Axis.SORTED : Axis.DISPLAY
        Axis axis = new Axis(axisName, AxisType.valueOf(type), AxisValueType.valueOf(valueType), axisOpts.hasDefault, sortedVal, maxId + 1, axisOpts.fireAll)
        ncube.addAxis(axis)
        mutableClient.updateCube(ncube)
    }

    /**
     * Add axis to an existing SNAPSHOT n-cube that is a reference to an axis in another cube.
     */
    void addAxis(ApplicationID appId, String cubeName, String axisName, ApplicationID refAppId, String refCubeName, String refAxisName, ApplicationID transformAppId, String transformCubeName, Map<String, Boolean> axisOpts = [hasDefault:true])
    {
        appId = addTenant(appId)
        NCube nCube = getCubeInternal(appId, cubeName)

        if (isEmpty(axisName))
        {
            axisName = refAxisName
        }

        long maxId = -1
        Iterator<Axis> i = nCube.axes.iterator()
        while (i.hasNext())
        {
            Axis axis = i.next()
            if (axis.id > maxId)
            {
                maxId = axis.id
            }
        }

        Map args = [:]
        args[REF_TENANT] = refAppId.tenant
        args[REF_APP] = refAppId.app
        args[REF_VERSION] = refAppId.version
        args[REF_STATUS] = refAppId.status
        args[REF_BRANCH] = refAppId.branch
        args[REF_CUBE_NAME] = refCubeName  // cube name of the holder of the referring (pointing) axis
        args[REF_AXIS_NAME] = refAxisName    // axis name of the referring axis (the variable that you had missing earlier)
        if (transformAppId?.app)
        {
            args[TRANSFORM_APP] = transformAppId.app // Notice no target tenant.  User MUST stay within TENENT boundary
            args[TRANSFORM_VERSION] = transformAppId.version
            args[TRANSFORM_STATUS] = transformAppId.status
            args[TRANSFORM_BRANCH] = transformAppId.branch
            args[TRANSFORM_CUBE_NAME] = transformCubeName
        }
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(cubeName, axisName, args)

        Axis axis = new Axis(axisName, maxId + 1, axisOpts.hasDefault, refAxisLoader)
        nCube.addAxis(axis)
        mutableClient.updateCube(nCube)
    }

    /**
     * Return the requested axis.  The returned axis has some 'massaging' applied to it before
     * being returned.  First, it is being returned using the 'map-of-maps' format from json-io
     * so that the column IDs can be converted from Longs to Strings, because Javascript cannot
     * process a 64-bit long value (it stores numbers using a double, which means it can only
     * reliably process 53-bits of a long).  Converting the longs to Strings first, allows the
     * column ID to round-trip to the UI and back, and json-io will 'mash' the String column ID
     * into the Long column ID (within the JsonCommandServlet) as it receives the String.  It
     * senses the data-type mismatch (json-io does) and then attempts to convert the String to a
     * numeric value (which succeeds).  This allows the full 64-bit id to make it round trip.
     */
    Map getAxis(ApplicationID appId, String cubeName, String axisName)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, null)
        NCube ncube = getCubeInternal(appId, cubeName)
        Axis axis = ncube.getAxis(axisName)
        return convertAxis(axis)
    }

    /**
     * Delete the passed in axis.
     */
    void deleteAxis(ApplicationID appId, String cubeName, String axisName)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        NCube ncube = getCubeInternal(appId, cubeName)

        if (ncube.numDimensions == 1)
        {
            throw new IllegalArgumentException("Could not delete axis '${axisName}' - at least one axis must exist on n-cube.")
        }

        ncube.deleteAxis(axisName)
        mutableClient.updateCube(ncube)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void updateAxis(ApplicationID appId, String cubeName, String origAxisName, String axisName, boolean hasDefault, boolean isSorted, boolean fireAll)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${origAxisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        NCube ncube = getCubeInternal(appId, cubeName)

        // Rename axis
        if (!origAxisName.equalsIgnoreCase(axisName))
        {
            ncube.renameAxis(origAxisName, axisName)
        }

        // Update default column setting (if changed)
        Axis axis = ncube.getAxis(axisName)
        if (axis.hasDefaultColumn() && !hasDefault)
        {   // If it went from having default column to NOT having default column...
            ncube.deleteColumn(axisName, null)
        }
        else if (!axis.hasDefaultColumn() && hasDefault)
        {
            if (axis.type != AxisType.NEAREST)
            {
                ncube.addColumn(axisName, null)
            }
        }

        // update preferred column order
        if (axis.type == AxisType.RULE)
        {
            axis.fireAll = fireAll
        }
        else
        {
            axis.columnOrder = isSorted ? Axis.SORTED : Axis.DISPLAY
        }

        ncube.clearSha1()
        mutableClient.updateCube(ncube)
    }

    /**
     * Update an entire set of columns on an axis at one time.  The updatedAxis is not a real axis,
     * but treated like an Axis-DTO where the list of columns within the axis are in display order.
     */
    @SuppressWarnings("GroovyUnusedDeclaration")
    void updateAxisColumns(ApplicationID appId, String cubeName, String axisName, Object[] cols)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        Set<Column> columns = new LinkedHashSet<>()

        if (cols != null)
        {
            cols.each {
                Object col ->
                    Column column = (Column) col
                    Object value = column.value
                    if (value == null || "".equals(value))
                    {
                        throw new IllegalArgumentException("Column cannot have empty value, n-cube: ${cubeName}, axis: ${axisName}")
                    }
                    columns.add(column)
            }
        }

        NCube ncube = getCubeInternal(appId, cubeName)
        ncube.updateColumns(axisName, columns)
        mutableClient.updateCube(ncube)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void breakAxisReference(ApplicationID appId, String cubeName, String axisName)
    {
        appId = addTenant(appId)
        String resourceName = "${cubeName}/${axisName}"
        mutableClient.assertPermissions(appId, resourceName, Action.UPDATE)
        NCube ncube = getCubeInternal(appId, cubeName)

        // Update default column setting (if changed)
        ncube.breakAxisReference(axisName)
        mutableClient.updateCube(ncube)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void createRefAxis(ApplicationID appId, String cubeName, String axisName, ApplicationID refAppId, String refCubeName, String refAxisName)
    {
        appId = addTenant(appId)
        refAppId = addTenant(refAppId)
        mutableClient.createRefAxis(appId, cubeName, axisName, refAppId, refCubeName, refAxisName)
    }

    Boolean renameCube(ApplicationID appId, String oldName, String newName)
    {
        appId = addTenant(appId)
        return mutableClient.renameCube(appId, oldName, newName)
    }

    /**
     * Promote a previous revision of an NCube.
     * @param cubeId long
     * @return NCubeInfoDto for the revision of the passed in cubeId, NOT the newly created revision.
     * NCubeInfDto does not contain bytes or testData
     * Note: the restored dto and the dto to restore from differ only in revision number
     */
    @SuppressWarnings("GroovyUnusedDeclaration")
    NCubeInfoDto promoteRevision(long cubeId)
    {
        NCubeInfoDto record = mutableClient.promoteRevision(cubeId)
        return record
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void saveJson(ApplicationID appId, String json)
    {
        appId = addTenant(appId)
        json = json.trim()
        List<NCube> cubes
        if (json.startsWith("["))
        {
            cubes = getCubes(json)
        }
        else
        {
            cubes = new ArrayList<>()
            cubes.add(NCube.fromSimpleJson(json))
        }

        for (ncube in cubes)
        {
            ncube.applicationID = appId
            try
            {
                mutableClient.updateCube(ncube)
            }
            catch (Exception ignore)
            {
                try
                {
                    mutableClient.createCube(ncube)
                }
                catch (Exception ex)
                {
                    throw new IllegalArgumentException("Unable to update or create cube: ${ncube.name}", ex)
                }
            }
        }
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map runTests(ApplicationID appId)
    {
        verifyAllowExecute('runTests')
        appId = addTenant(appId)
        return runtimeClient.runTests(appId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map runTests(ApplicationID appId, String cubeName, Object[] tests)
    {
        verifyAllowExecute('runTests')
        appId = addTenant(appId)
        return runtimeClient.runTests(appId, cubeName, tests)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map runTest(ApplicationID appId, String cubeName, NCubeTest test)
    {
        verifyAllowExecute('runTest')
        appId = addTenant(appId)
        return runtimeClient.runTest(appId, cubeName, test)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getTests(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        Object[] tests = mutableClient.getTests(appId, cubeName)
        return tests
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map getAppTests(ApplicationID appId)
    {
        appId = addTenant(appId)
        Map appTests = mutableClient.getAppTests(appId)
        return appTests
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean saveTests(ApplicationID appId, String cubeName, Object[] tests)
    {
        appId = addTenant(appId)
        NCube cube = getCubeInternal(appId, cubeName)
        cube.testData = tests
        return mutableClient.updateCube(cube)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    NCubeTest createNewTest(ApplicationID appId, String cubeName, String testName)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)

        if (isEmpty(testName))
        {
            throw new IllegalArgumentException("Test name cannot be empty, cube: ${cubeName}, app: ${appId}")
        }

        Set<String> items = ncube.getRequiredScope([:], [:])

        Map<String, CellInfo> coords = new CompactCILinkedMap<>()
        if (items?.size())
        {
            for (String s : items)
            {
                coords[s] = (CellInfo)null
            }
        }

        CellInfo[] assertions = [ new CellInfo("exp", "output.return", false, false) ] as CellInfo[]
        NCubeTest test = new NCubeTest(testName, coords, assertions)
        return test
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    {
        appId = addTenant(appId)
        return mutableClient.updateNotes(appId, cubeName, notes)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String getNotes(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        return mutableClient.getNotes(appId, cubeName)
    }

    /**
     * In-place update of a cell.
     */
    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean updateCell(ApplicationID appId, String cubeName, Object[] ids, CellInfo cellInfo)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        Set<Long> colIds = getCoordinate(ids)

        if (cellInfo == null)
        {
            ncube.removeCellById(colIds)
        }
        else
        {
            ncube.setCellById(cellInfo.recreate(), colIds)
        }
        mutableClient.updateCube(ncube)
        return true
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean updateCellAt(ApplicationID appId, String cubeName, Map coordinate, CellInfo cellInfo)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)

        if (cellInfo == null)
        {
            ncube.removeCell(coordinate)
        }
        else
        {
            ncube.setCell(cellInfo.recreate(), coordinate)
        }
        mutableClient.updateCube(ncube)
        return true
    }

    Map getCell(ApplicationID appId, String cubeName, Map coordinate, defaultValue = null)
    {
        verifyAllowExecute('getCell')
        appId = addTenant(appId)
        Map output = runtimeClient.getCell(appId, cubeName, coordinate, defaultValue)
        return output
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object getCellNoExecute(ApplicationID appId, String cubeName, Object[] ids)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        Set<Long> colIds = getCoordinate(ids)
        Object cell = ncube.getCellByIdNoExecute(colIds)

        CellInfo cellInfo = new CellInfo(cell)
        cellInfo.collapseToUiSupportedTypes()
        return cellInfo
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object getCellNoExecuteByCoordinate(ApplicationID appId, String cubeName, Map coordinate)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        Object cell = ncube.getCellNoExecute(coordinate)
        CellInfo cellInfo = new CellInfo(cell)
        cellInfo.collapseToUiSupportedTypes()
        return cellInfo
    }

    /**
     * This API will fetch and execute particular cell values (identified by the idArrays) for the passed
     * in appId and named cube.  The idArrays is an Object[] of Object[]'s:<pre>
     * [
     *  [1, 2, 3],
     *  [4, 5, 6],
     *  [7, 8, 9],
     *   ...
     *]
     * In the example above, the 1st entry [1, 2, 3] identifies the 1st cell to fetch.  The 2nd entry [4, 5, 6]
     * identifies the 2nd cell to fetch, and so on.
     * </pre>
     * @return Object[] The return value is an Object[] containing Object[]'s with the original coordinate
     *  as the first entry and the executed cell value as the 2nd entry:<pre>
     * [
     *  [[1, 2, 3], {"type":"int", "value":75}],
     *  [[4, 5, 6], {"type":"double", "value":3.14159}],
     *  [[7, 8, 9], {"type":"string", "value":"hello"}],
     *   ...
     * ]
     * </pre>
     */
    Object[] getCells(ApplicationID appId, String cubeName, Object[] idArrays, Map input, Object defaultValue = null)
    {
        verifyAllowExecute("getCells")
        appId = addTenant(appId)
        Object[] ret = runtimeClient.getCells(appId, cubeName, idArrays, input, [:], defaultValue)
        return ret
    }

    /**
     * This API will fetch particular cell values (identified by the idArrays) for the passed
     * in appId and named cube.  The idArrays is an Object[] of Object[]'s:<pre>
     * [
     *  [1, 2, 3],
     *  [4, 5, 6],
     *  [7, 8, 9],
     *   ...
     *]
     * In the example above, the 1st entry [1, 2, 3] identifies the 1st cell to fetch.  The 2nd entry [4, 5, 6]
     * identifies the 2nd cell to fetch, and so on.
     * </pre>
     * @return Object[] The return value is an Object[] containing Object[]'s with the original coordinate
     *  as the first entry and the cell value as the 2nd entry:<pre>
     * [
     *  [[1, 2, 3], {"type":"int", "value":75}],
     *  [[4, 5, 6], {"type":"exp", "cache":false, "value":"return 25"}],
     *  [[7, 8, 9], {"type":"string", "value":"hello"}],
     *   ...
     * ]
     * </pre>
     */
    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getCellsNoExecute(ApplicationID appId, String cubeName, Object[] idArrays)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Unable to fetch requested cells. NCube: ${cubeName} not found, app: ${appId}")
        }
        Object[] ret = new Object[idArrays.length]
        Set key = new HashSet()
        int idx = 0

        for (coord in idArrays)
        {
            for (item in coord)
            {
                key.add(convertToLong(item))
            }
            if (ncube.containsCellById(key))
            {
                CellInfo cellInfo = new CellInfo(ncube.getCellByIdNoExecute(key))
                cellInfo.collapseToUiSupportedTypes()
                ret[idx++] = [coord, cellInfo as Map]
            }
            else
            {
                ret[idx++] = [coord, NO_CELL]
            }
            key.clear()
        }

        return ret
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map getCellCoordinate(ApplicationID appId, String cubeName, Object[] ids)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        Set<Long> colIds = getCoordinate(ids)
        Map<String, Object> coord = ncube.getDisplayCoordinateFromIds(colIds)
        Map<String, Object> niceCoord = [:]
        coord.each { k, v ->
            Comparable c = v as Comparable
            niceCoord[k] = CellInfo.formatForDisplay(c)
        }
        return niceCoord
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String copyCells(ApplicationID appId, String cubeName, Object[] ids, boolean isCut)
    {
        appId = addTenant(appId)
        if (ids == null || ids.length == 0)
        {
            throw new IllegalArgumentException("No IDs of cells to cut/clear were given.")
        }

        NCube ncube = getCubeInternal(appId, cubeName)
        List<Object[]> cells = new ArrayList<>()

        for (Object id : ids)
        {
            Object[] cellId = (Object[]) id
            if (ArrayUtilities.isEmpty(cellId))
            {
                cells.add(null)
                continue
            }
            Set<Long> colIds = getCoordinate(cellId)
            Object content = ncube.getCellByIdNoExecute(colIds)
            CellInfo cellInfo = new CellInfo(content)
            cells.add([cellInfo.value, cellInfo.dataType, cellInfo.isUrl, cellInfo.isCached] as Object[])

            if (isCut)
            {
                ncube.removeCellById(colIds)
            }
        }

        if (isCut)
        {
            mutableClient.updateCube(ncube)
        }
        return JsonWriter.objectToJson(cells.toArray())
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean pasteCellsNce(ApplicationID appId, String cubeName, Object[] clipboard)
    {
        if (ArrayUtilities.isEmpty(clipboard))
        {
            throw new IllegalArgumentException("Could not paste cells, no data available on clipboard.")
        }

        NCube ncube = getCubeInternal(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not paste cells, cube: ${cubeName} not found for app: ${appId}")
        }

        final int len = clipboard.length
        for (int i=0; i < len; i++)
        {
            Object[] cell = clipboard[i] as Object[]
            if (ArrayUtilities.isEmpty(cell))
            {   // null is EOL marker
                continue
            }

            Object lastElem = cell[cell.length - 1i]

            if (lastElem instanceof Object[])
            {   // If last element is an Object[], we have a coordinate (destination cell)
                Object[] ids = lastElem as Object[]
                Set<Long> cellId = getCoordinate(ids)
                CellInfo info = new CellInfo(cell[1] as String, cell[0] as String, cell[2], cell[3])
                Object value = info.recreate()
                if (value == null)
                {
                    ncube.removeCellById(cellId)
                }
                else
                {
                    ncube.setCellById(value, cellId)
                }
            }
        }
        mutableClient.updateCube(ncube)
        return true
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean pasteCells(ApplicationID appId, String cubeName, Object[] values, Object[] coords)
    {
        if (values == null || values.length == 0 || coords == null || coords.length == 0)
        {
            throw new IllegalArgumentException("Could not paste cells, values and coordinates must not be empty or length of 0.")
        }

        NCube ncube = getCubeInternal(appId, cubeName)

        for (int i=0; i < coords.length; i++)
        {
            Object[] row = (Object[]) coords[i]
            if (ArrayUtilities.isEmpty(row))
            {
                break
            }

            for (int j=0; j < row.length; j++)
            {
                Object[] ids = (Object[]) row[j]
                Set<Long> cellId = getCoordinate(ids)
                Object value = convertStringToValue(getValueRepeatIfNecessary(values, i, j))
                if (value == null)
                {
                    ncube.removeCellById(cellId)
                }
                else
                {
                    ncube.setCellById(value, cellId)
                }
            }
        }
        mutableClient.updateCube(ncube)
        return true
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String resolveRelativeUrl(ApplicationID appId, String relativeUrl)
    {
        verifyAllowExecute('resolveRelativeUrl')
        appId = addTenant(appId)
        URL absUrl = runtimeClient.getActualUrl(appId, relativeUrl, [:])
        if (absUrl == null)
        {
            throw new IllegalStateException("Unable to resolve the relative URL (${relativeUrl}) to a physical URL, app: ${appId}")
        }
        return absUrl
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String getUrlContent(ApplicationID appId, String relativeUrl)
    {
        verifyAllowExecute('getUrlContent')
        appId = addTenant(appId)
        String content = runtimeClient.getUrlContent(appId, relativeUrl, [:])
        return content
    }

    void clearCache(ApplicationID appId)
    {
        appId = addTenant(appId)
        if (isAppAdmin(appId) || !appId.head)
        {
            runtimeClient.clearCache(appId)
        }
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void createBranch(ApplicationID appId)
    {
        appId = addTenant(appId)
        mutableClient.copyBranch(appId.asHead(), appId)
    }

    Integer copyBranch(ApplicationID srcAppId, ApplicationID targetAppId, boolean copyWithHistory = false)
    {
        srcAppId = addTenant(srcAppId)
        targetAppId = addTenant(targetAppId)
        Integer rows = mutableClient.copyBranch(srcAppId, targetAppId, copyWithHistory)
        return rows
    }

    Object[] getBranches(ApplicationID appId)
    {
        appId = addTenant(appId)
        Object[] realBranches = mutableClient.getBranches(appId)
        return realBranches
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Integer getBranchCount(ApplicationID appId)
    {
        appId = addTenant(appId)
        // Run against database as this is used to verify live record counts
        return mutableClient.getBranchCount(appId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getHeadChangesForBranch(ApplicationID appId)
    {
        appId = addTenant(appId)
        List<NCubeInfoDto> branchChanges = mutableClient.getHeadChangesForBranch(appId)
        return branchChanges.toArray()
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getBranchChangesForHead(ApplicationID appId)
    {
        appId = addTenant(appId)
        List<NCubeInfoDto> branchChanges = mutableClient.getBranchChangesForHead(appId)
        return branchChanges.toArray()
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getBranchChangesForMyBranch(ApplicationID appId, String branch)
    {
        appId = addTenant(appId)
        List<NCubeInfoDto> branchChanges = mutableClient.getBranchChangesForMyBranch(appId, branch)
        return branchChanges.toArray()
    }

    String generatePullRequestHash(ApplicationID appId, Object[] infoDtos, String notes = '')
    {
        appId = addTenant(appId)
        String prId = mutableClient.generatePullRequestHash(appId, infoDtos, notes)
        return prId
    }

    Object mergePullRequest(String prId)
    {
        Map result = mutableClient.mergePullRequest(prId)
        return result
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void cancelPullRequest(String prId)
    {
        mutableClient.cancelPullRequest(prId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void reopenPullRequest(String prId)
    {
        mutableClient.reopenPullRequest(prId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void obsoletePullRequest(String prId)
    {
        mutableClient.obsoletePullRequest(prId)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getPullRequests(Date startDate = null, Date endDate = null, String prId = null)
    {
        Object[] pullRequests = mutableClient.getPullRequests(startDate, endDate, prId)
        return pullRequests
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object commitCube(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        Map options = [:]
        options[(SEARCH_EXACT_MATCH_NAME)] = true
        options[(SEARCH_ACTIVE_RECORDS_ONLY)] = true
        List<NCubeInfoDto> list = mutableClient.search(appId, cubeName, null, options)
        return mutableClient.commitBranch(appId, list.toArray())
    }

    Object commitBranch(ApplicationID appId, Object[] infoDtos = null, String notes = null)
    {
        appId = addTenant(appId)
        return mutableClient.commitBranch(appId, infoDtos, notes)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Integer rollbackBranch(ApplicationID appId, Object[] cubeNames)
    {
        appId = addTenant(appId)
        return mutableClient.rollbackBranch(appId, cubeNames)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object updateCubeFromHead(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        NCubeInfoDto dto = mutableClient.getHeadChangesForBranch(appId).find { it.name == cubeName }
        if (dto == null)
        {
            throw new IllegalStateException("${cubeName} is already up-to-date")
        }
        mutableClient.updateBranch(appId, dto)
    }

    Object updateBranch(ApplicationID appId, Object[] cubeDtos = null)
    {
        appId = addTenant(appId)
        Map<String, Object> result = mutableClient.updateBranch(appId, cubeDtos)
        return result
    }

    Boolean deleteBranch(ApplicationID appId)
    {
        appId = addTenant(appId)
        Boolean result = mutableClient.deleteBranch(appId)
        return result
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean deleteApp(ApplicationID appId)
    {
        appId = addTenant(appId)
        Boolean result = mutableClient.deleteApp(appId)
        return result
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Integer acceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch = ApplicationID.HEAD)
    {
        appId = addTenant(appId)
        return mutableClient.acceptTheirs(appId, cubeNames, sourceBranch)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Integer acceptMine(ApplicationID appId, Object[] cubeNames, String notNeeded = null)
    {
        appId = addTenant(appId)
        return mutableClient.acceptMine(appId, cubeNames)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    String loadCubeById(ApplicationID appId, long id, String mode)
    {
        NCubeInfoDto record = mutableClient.loadCubeRecordById(id, null)
        NCube ncube = NCube.createCubeFromRecord(record)
        return NCube.formatCube(ncube, [mode: mode])
    }

    /**
     * @return Map of HTTP headers for debugging display.
     */
    Map getHeaders()
    {
        HttpServletRequest request = JsonCommandServlet.servletRequest.get()
        Enumeration e = request.headerNames
        Map<String, String> headers = [:]

        while (e.hasMoreElements())
        {
            String headerName = (String) e.nextElement()
            headers[(headerName)] = request.getHeader(headerName)
        }

        return headers
    }

    Map execute(ApplicationID appId, String cubeName, String method, Map args)
    {
        verifyAllowExecute('execute')
        appId = addTenant(appId)
        Map output = runtimeClient.execute(appId, cubeName, method, args)
        return output
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map getMenu(ApplicationID appId)
    {
        verifyAllowExecute('getMenu')
        appId = addTenant(appId)
        Map menu = runtimeClient.getMenu(appId)
        return menu
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object getDefaultCell(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        NCube menuCube = getCubeInternal(appId, cubeName)
        CellInfo cellInfo = new CellInfo(menuCube.defaultCellValue)
        cellInfo.collapseToUiSupportedTypes()
        return cellInfo
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean clearDefaultCell(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        NCube ncube = getCubeInternal(appId, cubeName)
        ncube.defaultCellValue = null
        mutableClient.updateCube(ncube)
        return true
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean updateDefaultCell(ApplicationID appId, String cubeName, CellInfo cellInfo)
    {
        appId = addTenant(appId)
        Object cellValue = cellInfo.isUrl ?
                CellInfo.parseJsonValue(null, cellInfo.value, cellInfo.dataType, cellInfo.isCached) :
                CellInfo.parseJsonValue(cellInfo.value, null, cellInfo.dataType, cellInfo.isCached)

        NCube ncube = getCubeInternal(appId, cubeName)
        ncube.defaultCellValue = cellValue
        mutableClient.updateCube(ncube)
        return true
    }

    Boolean mergeDeltas(ApplicationID appId, String cubeName, Object[] deltas)
    {
        appId = addTenant(appId)
        List<Delta> deltaList = deltas as List<Delta>
        return mutableClient.mergeDeltas(appId, cubeName, deltaList)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    List<Delta> fetchJsonRevDiffs(long newCubeId, long oldCubeId)
    {
        List<Delta> deltas = mutableClient.fetchJsonRevDiffs(newCubeId, oldCubeId)
        return deltas
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    List<Delta> fetchJsonBranchDiffs(NCubeInfoDto newInfoDto, NCubeInfoDto oldInfoDto)
    {
        List<Delta> deltas = mutableClient.fetchJsonBranchDiffs(newInfoDto, oldInfoDto)
        return deltas
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Object[] getReferenceAxes(ApplicationID appId)
    {
        appId = addTenant(appId)
        List refAxes = mutableClient.getReferenceAxes(appId)
        return refAxes as Object[]
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void updateReferenceAxes(Object[] axisRefs)
    {
        mutableClient.updateReferenceAxes(axisRefs)
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void clearTestDatabase()
    {
        testServer.clearTestDatabase()
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    void clearPermCache()
    {
        testServer.clearPermCache()
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map health(boolean showAll = false)
    {
        // If remotely accessing server, use the following to get the MBeanServerConnection...
//        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:/jmxrmi")
//        JMXConnector jmxc = JMXConnectorFactory.connect(url, null)
//
//        MBeanServerConnection conn = jmxc.getMBeanServerConnection()
//        String[] domains = conn.getDomains()
//        Set result = conn.queryMBeans(null, "Catalina:type=DataSource,path=/appdb,host=localhost,class=javax.sql.DataSource")
//        jmxc.close()

        Map results = [:]

        // Snag the platform mbean server (singleton)
        MBeanServer mbs = ManagementFactory.platformMBeanServer

        // App server name and version
        Map serverStats = [:]

        String username
        try
        {
            username = mutableClient.userId
        }
        catch (Exception ignored)
        {
            username = '--'
        }

        Map info = infoEndpoint.info()
        Map buildInfo = info.build as Map
        if (buildInfo)
        {
            putIfNotNull(serverStats, 'n-cube-editor version', buildInfo.version)
            putIfNotNull(serverStats, 'n-cube version', buildInfo.'ncube-version')
        }
        putIfNotNull(serverStats, 'User ID', username)
        putIfNotNull(serverStats, 'Java version', getAttribute(mbs, 'JMImplementation:type=MBeanServerDelegate', 'ImplementationVersion'))
        putIfNotNull(serverStats, 'hostname, servlet', getServletHostname())
        putIfNotNull(serverStats, 'hostname, OS', getInetHostname())
        putIfNotNull(serverStats, 'Context', JsonCommandServlet.servletRequest.get().contextPath)

        // OS
        putIfNotNull(serverStats, 'OS', getAttribute(mbs, 'java.lang:type=OperatingSystem', 'Name'))
        putIfNotNull(serverStats, 'OS version', getAttribute(mbs, 'java.lang:type=OperatingSystem', 'Version'))
        putIfNotNull(serverStats, 'CPU', getAttribute(mbs, 'java.lang:type=OperatingSystem', 'Arch'))
        double processLoad = getAttribute(mbs, 'java.lang:type=OperatingSystem', 'ProcessCpuLoad') as Double
        if (processLoad > processLoadPeak.get())
        {
            processLoadPeak.set(processLoad)
        }
        double systemLoad = getAttribute(mbs, 'java.lang:type=OperatingSystem', 'SystemCpuLoad') as Double
        if (systemLoad > systemLoadPeak.get())
        {
            systemLoadPeak.set(systemLoad)
        }
        putIfNotNull(serverStats, 'Process CPU Load', processLoad)
        putIfNotNull(serverStats, 'System CPU Load', systemLoad)
        putIfNotNull(serverStats, 'Peak Process CPU Load', processLoadPeak.get())
        putIfNotNull(serverStats, 'Peak System CPU Load', systemLoadPeak.get())
        putIfNotNull(serverStats, 'CPU Cores', getAttribute(mbs, 'java.lang:type=OperatingSystem', 'AvailableProcessors'))
        double machMem = (long) getAttribute(mbs, 'java.lang:type=OperatingSystem', 'TotalPhysicalMemorySize')
        long K = 1024L
        long MB = K * 1024L
        long GB = MB * 1024L
        machMem = machMem / GB
        putIfNotNull(serverStats, 'Physical Memory', (machMem.round(2)) + ' GB')

        // JVM
        putIfNotNull(serverStats, 'Loaded class count', getAttribute(mbs, 'java.lang:type=ClassLoading', 'LoadedClassCount'))

        // JVM Memory
        Runtime rt = Runtime.runtime
        double maxMem = rt.maxMemory() / MB
        double freeMem = rt.freeMemory() / MB
        double usedMem = maxMem - freeMem
        putIfNotNull(serverStats, 'Heap size (-Xmx)', (maxMem.round(1)) + ' MB')
        putIfNotNull(serverStats, 'Used memory', (usedMem.round(1)) + ' MB')
        putIfNotNull(serverStats, 'Free memory', (freeMem.round(1)) + ' MB')
        putIfNotNull(serverStats, 'Tomcat Max Connections', tomcatMaxConnections)
        putIfNotNull(serverStats, 'Tomcat Max Threads', tomcatMaxThreads)

        serverStats['----------'] = ''

        putIfNotNull(results, 'serverStats', serverStats)
        return results
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Map heartBeat(Map openCubes = null)
    {
        Map results = [:]
        putIfNotNull(results, 'compareResults', [:])
        return results
    }

    @SuppressWarnings("GroovyUnusedDeclaration")
    Boolean isCubeUpToDate(ApplicationID appId, String cubeName)
    {
        appId = addTenant(appId)
        return mutableClient.isCubeUpToDate(appId, cubeName)
    }

    // ============================================= End API ===========================================================

    // ===================================== utility (non-API) methods =================================================

    private static Object getAttribute(MBeanServer mbs, String beanName, String attribute)
    {
        try
        {
            ObjectName objectName = new ObjectName(beanName)
            mbs.getAttribute(objectName, attribute)
        }
        catch (Exception ignored)
        {
//            log.info('Unable to fetch attribute: ' + attribute + ' from mbean: ' + beanName)
            null
        }
    }

    private static void putIfNotNull(Map map, String key, Object value)
    {
        if (value != null)
        {
            if (value instanceof Integer)
            {
                value = ((Integer)value).longValue()
            }
            else if (value instanceof Float)
            {
                value = ((Float)value).doubleValue()
            }
            map[key] = value
        }
    }

    private static String getValueRepeatIfNecessary(Object[] values, int row, int col)
    {
        if (row > (values.length - 1))
        {
            row %= values.length
        }
        Object[] valueRow = (Object[]) values[row]
        if (ArrayUtilities.isEmpty(valueRow))
        {
            return null
        }
        if (col > (valueRow.length - 1i))
        {
            col %= valueRow.length
        }
        return (String) valueRow[col]
    }

    private static Object convertStringToValue(String origValue)
    {
        if (isEmpty(origValue))
        {
            return null
        }

        String value = origValue.trim()

        if ('0'.equals(value))
        {
            return 0L
        }
        else if ('true'.equalsIgnoreCase(value))
        {
            return true
        }
        else if ('false'.equalsIgnoreCase(value))
        {
            return false
        }

        if (isNumeric(value))
        {
            value = removeCommas(value)
            if (!value.contains("."))
            {
                try
                {
                    return convertToLong(value)
                }
                catch (Exception ignored) { }
            }

            try
            {
                return new BigDecimal(value)
            }
            catch (Exception ignored) { }
        }

        // Try as a date (the code below supports numerous different date formats)
        try
        {
            return convertToDate(value)
        }
        catch (Exception ignored) { }

        // OK, if all else fails, return it as the string it was
        return origValue
    }

    /**
     * Convert Axis to Map of Map representation (using json-io) and modify the
     * column ID to a String in the process.  This allows the column ID to work on
     * clients (like Javascript) that cannot support 64-bit values.
     */
    static Map convertAxis(Axis axis) throws IOException
    {
        String json = JsonWriter.objectToJson(axis, [(JsonWriter.TYPE): false] as Map)
        Map axisConverted = (Map) JsonReader.jsonToJava(json, [(JsonReader.USE_MAPS):true] as Map)
        axisConverted.'@type' = axis.class.name
        Object[] cols = axis.columns as Object[]
        axisConverted.remove('idToCol')

        for (int i = 0; i < cols.length; i++)
        {
            Column actualCol = (Column) cols[i]
            Map col = columnToMap(actualCol)
            CellInfo cellInfo = new CellInfo(actualCol.value)
            String value = cellInfo.value
            if (axis.valueType == AxisValueType.DATE && axis.type != AxisType.SET && value != null)
            {
                value = NO_QUOTES_REGEX.matcher(value).replaceAll("")
            }
            col.value = value   // String version for Discrete, Range, or Set support
            col.isUrl = cellInfo.isUrl
            col.dataType = cellInfo.dataType
            col.isCached = cellInfo.isCached
            cols[i] = col
        }
        axisConverted.columns = cols
        return axisConverted
    }

    private static Map columnToMap(Column col)
    {
        Map map = [:]
        map.id = convertToString(col.id)  // Stringify Long ID (Javascript safe if quoted)
        map.'@type' = Column.class.name
        if (col.metaProperties.size() > 0)
        {
            map.metaProps = [:]
        }
        for (Map.Entry<String, Object> entry : col.metaProperties)
        {
            map.metaProps[entry.key] = entry.value == null ? 'null' : entry.value
        }
        map.displayOrder = col.displayOrder as long
        return map
    }

    static boolean isNumeric(String str)
    {
        return IS_NUMBER_REGEX.matcher(str).matches()  // match a number with optional '-' and decimal.
    }

    private static String removeCommas(String str)
    {
        StringBuilder s = new StringBuilder()
        final int len = str.length()
        for (int i=0; i < len; i++)
        {
            char x = str.charAt(i)
            if (x != ',')
            {
                s.append(x)
            }
        }
        return s.toString()
    }

    private static Set<Long> getCoordinate(Object[] ids)
    {
        // Convert String column IDs to Longs
        Set<Long> colIds = new LinkedHashSet<>()
        for (Object id : ids)
        {
            colIds.add(convertToLong(id))
        }
        return colIds
    }

    private NCube getCubeInternal(ApplicationID appId, String ncubeName)
    {
        NCube ncube = mutableClient.getCube(appId, ncubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Unable to load cube: ${ncubeName} for app: ${appId}")
        }
        return ncube
    }

    private ApplicationID addTenant(ApplicationID appId)
    {
        if (appId == null)
        {
            throw new IllegalArgumentException('ApplicationID cannot be null')
        }
        String tenant = tenant
        return new ApplicationID(tenant, appId.app, appId.version, appId.status, appId.branch)
    }

    private String getTenant()
    {
        // TODO: Should be based on authentication
        return ApplicationID.DEFAULT_TENANT
    }

    private NCubeRuntimeClient getRuntimeClient()
    {
        return mutableClient as NCubeRuntimeClient
    }

    private void verifyAllowExecute(String methodName)
    {
        if (!allowExecute)
        {
            throw new IllegalStateException("${EXECUTE_ERROR} ${methodName}()")
        }
    }

    private static Object[] caseInsensitiveSort(Object[] items)
    {
        Arrays.sort(items, new Comparator<Object>() {
            int compare(Object o1, Object o2)
            {
                ((String) o1)?.toLowerCase() <=> ((String) o2)?.toLowerCase()
            }
        })
        return items
    }

    private static String getInetHostname()
    {
        if (inetHostname == null)
        {
            inetHostname = InetAddressUtilities.hostName
        }
        return inetHostname
    }

    private static String getServletHostname()
    {
        if (servletHostname == null)
        {
            servletHostname = JsonCommandServlet.servletRequest.get().serverName
        }
        return servletHostname
    }

    private static List<NCube> getCubes(String json)
    {
        String lastSuccessful = ""
        try
        {
            Object[] cubes = (Object[]) JsonReader.jsonToJava(json)
            List cubeList = new ArrayList(cubes.length)

            for (Object cube : cubes)
            {
                JsonObject ncube = (JsonObject) cube
                if (ncube.containsKey("action"))
                {
                    cubeList.add(ncube)
                    lastSuccessful = (String) ncube.get("ncube")
                }
                else
                {
                    // TODO - if this method is too slow, add a new method to NCube to parse multiple NCubes using stream processing for the entire json
                    String json1 = JsonWriter.objectToJson(ncube, [(JsonWriter.TYPE): false] as Map)
                    NCube nCube = NCube.fromSimpleJson(json1)
                    cubeList.add(nCube)
                    lastSuccessful = nCube.name
                }
            }

            return cubeList
        }
        catch (Exception e)
        {
            String s = "Failed to load n-cubes from passed in JSON, last successful cube read: ${lastSuccessful}"
            throw new IllegalArgumentException(s, e)
        }
    }
}