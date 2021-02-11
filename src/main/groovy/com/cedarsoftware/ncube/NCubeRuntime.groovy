package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.formatters.TestResultsFormatter
import com.cedarsoftware.ncube.util.CdnClassLoader
import com.cedarsoftware.ncube.util.GCacheManager
import com.cedarsoftware.ncube.util.LocalFileCache
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.CallableBean
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.GuavaCache
import com.cedarsoftware.util.JsonHttpProxy
import com.cedarsoftware.util.ThreadAwarePrintStream
import com.cedarsoftware.util.ThreadAwarePrintStreamErr
import com.cedarsoftware.util.TrackingMap
import com.cedarsoftware.util.io.JsonObject
import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import com.cedarsoftware.visualizer.RpmVisualizer
import com.cedarsoftware.visualizer.Visualizer
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import ncube.grv.method.NCubeGroovyController
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager
import org.springframework.core.env.Environment

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import static SnapshotPolicy.FORCE
import static com.cedarsoftware.ncube.NCubeConstants.*
import static com.cedarsoftware.util.Converter.convertToBoolean
import static com.cedarsoftware.util.IOUtilities.close
import static com.cedarsoftware.util.IOUtilities.uncompressBytes
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.StringUtilities.isEmpty
import static com.cedarsoftware.util.StringUtilities.wildcardToRegexString
import static com.cedarsoftware.util.SystemUtilities.getExternalVariable
import static com.cedarsoftware.visualizer.RpmVisualizerConstants.RPM_CLASS

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
@Slf4j
@CompileStatic
class NCubeRuntime implements NCubeMutableClient, NCubeRuntimeClient, NCubeTestClient, DisposableBean
{
    private static final String MUTABLE_ERROR = 'Non-runtime method called:'
    private final CacheManager ncubeCacheManager
    private final CacheManager adviceCacheManager
    private final ConcurrentMap<ApplicationID, GroovyClassLoader> localClassLoaders = new ConcurrentHashMap<>()
    private static AtomicInteger instanceCount = new AtomicInteger(0)
    // not private in case we want to tweak things for testing.
    protected volatile ConcurrentMap<String, Object> systemParams = null
    protected final CallableBean bean
    private volatile boolean alive = true
    private final String beanName
    @Value('${ncube.cache.refresh.min:90}') int cacheRefreshIntervalMin

    @Autowired(required = false)
    private LocalFileCache localFileCache

    @Autowired
    Environment env

    // Test overrideable. When set to null, these values reflect the spring environment settings.
    // When set to not-null, the value set (by a test) is the value used.
    private Boolean allowMutable = null
    private Boolean trackBindings = null
    private String acceptedDomains = null

    private final ThreadLocal<GroovyShell> groovyShellThreadLocal = new ThreadLocal<GroovyShell>() {
        GroovyShell initialValue()
        {
            return new GroovyShell()
        }
    }

    private GroovyShell getGroovyShell()
    {
        return groovyShellThreadLocal.get()
    }

    LocalFileCache getLocalFileCache() {
        return localFileCache
    }

    void setLocalFileCache(LocalFileCache localFileCache) {
        this.localFileCache = localFileCache
    }

    NCubeRuntime(CallableBean bean, CacheManager ncubeCacheManager, boolean unused = false)
    {
        this.bean = bean
        this.ncubeCacheManager = ncubeCacheManager
        this.adviceCacheManager = new GCacheManager()
        // Set the 'beanName' that the CallableBean will call.  When NCubeRuntime is targeting JsonHttpProxy,
        // the name will be 'ncubeController' - the name JsonCommandServlet will use to find the controller
        // from Spring.  When NCubeRuntime is targeting ReflectiveProxy(NCubeManager), the name is not used
        // to locate the NCubeManager - that is bound directly to the ReflectProxy.  In this case, the name is
        // used as part of a key in a the method cache: beanName.methodName.numberOfArgs -> Method.
        this.beanName = bean instanceof JsonHttpProxy ? CONTROLLER_BEAN : MANAGER_BEAN

        // This thread below is periodically accessing particular NCube's in the cache so that they are not evicted
        // by Guava's cache (which evicts 4 hours after last access).  The SYS_CLASSPATH cube is always accessed to
        // prevent eviction, and any NCube with the meta property "evict":false will not be evicted.
        def refresh = {
            while (alive)
            {
                Thread.sleep(1000 * 60 * cacheRefreshIntervalMin)
                GCacheManager cacheManager = (GCacheManager) ncubeCacheManager
                Iterator<String> i = cacheManager.cacheNames.iterator()
                
                while (i.hasNext())
                {
                    String cacheName = i.next()
                    Cache cache = cacheManager.getCache(cacheName)
                    if (cache != null)
                    {
                        cacheManager.applyToEntries(cacheName, { key, value ->
                            if (value instanceof NCube)
                            {
                                NCube cube = (NCube) value
                                boolean evict = true
                                if (cube.containsMetaProperty(CUBE_EVICT))
                                {
                                    evict = convertToBoolean(cube.getMetaProperty(CUBE_EVICT))
                                }

                                if (cube.name == SYS_CLASSPATH || !evict)
                                {   // refresh last-accessed time
                                    cache.get(key)
                                }
                            }
                        })
                    }
                }
            }
        }
        if (ncubeCacheManager instanceof GCacheManager)
        {
            Thread t = new Thread(refresh)
            t.name = "NcubeCacheRefresher${instanceCount.incrementAndGet()}"
            t.daemon = true
            t.start()
        }
    }

    void destroy() throws Exception
    {
        alive = false
    }

    /**
     * @return String of accepted domains in format acceptable for grapes.  Example: "org.apache."  This method
     * returns the value set in Spring context when the member variable is null, otherwise the member variable
     * 'acceptedDomains' value is returned.  This allows tests to override it, and then restore it (by setting
     * this value to null).  Spring environment property of 'ncube.accepted.domains'
     */
    String getAcceptedDomains()
    {
        if (acceptedDomains == null)
        {
            acceptedDomains = env.getProperty('ncube.accepted.domains', '')
        }
        return acceptedDomains
    }

    /**
     * Override the acceptedDomains property from the Spring context.  The property is 'ncube.accepted.domains'.
     * @param domains String list of accepted domains to force (e.g. "org.apache."), or null to have the
     * values configured in Spring be returned.
     */
    void setAcceptedDomains(String domains)
    {
        acceptedDomains = domains
    }

    /**
     * @return boolean true if running ncube.allow.mutable.methods is set to true or null, false otherwise.
     */
    boolean isAllowMutableOn()
    {
        if (allowMutable == null)
        {
            allowMutable = env.getProperty('ncube.allow.mutable.methods', (String) 'true')
        }
        return allowMutable
    }

    /**
     * @param state Boolean - 3 state value: null = use Spring Environment property, true = allow mutable on,
     * false = allow mutable turned off.  Used by testing.
     */
    void setAllowMutable(Boolean state)
    {
        allowMutable = state
    }

    /**
     * @return Boolean that indicates whether the output will tracking all cell bindings throughout an n-cube
     * execution path.  The default is true, but can be turned off to save memory during execution.
     */
    boolean isTrackBindingsOn()
    {
        if (trackBindings == null)
        {
            trackBindings = env.getProperty('ncube.track.bindings', (String)'true')
        }
        return trackBindings
    }

    /**
     * @param state Boolean - 3 state value: null = use Spring Environment property, true = track binding is on,
     * false = track binding has been turned off.  Used by testing.
     */
    void setTrackBindings(Boolean state)
    {
        trackBindings = state
    }

    Map getMenu(ApplicationID appId)
    {
        Map appMenu = [:]
        Map globalMenu = [:]
        ApplicationID sysAppId = new ApplicationID(appId.tenant, SYS_APP, ApplicationID.SYS_BOOT_VERSION, ReleaseStatus.SNAPSHOT.name(), ApplicationID.HEAD)
        NCube globalMenuCube = getCubeInternal(sysAppId, GLOBAL_MENU)
        if (globalMenuCube)
        {
            globalMenu =  globalMenuCube.getCell([:]) as Map
        }
        try
        {   // Do not remove try-catch handler in favor of advice handler
            ApplicationID bootVersionAppId = appId.asBootVersion()
            NCube menuCube = getCubeInternal(bootVersionAppId, SYS_MENU)
            if (menuCube == null)
            {
                menuCube = getCubeInternal(bootVersionAppId.asHead(), SYS_MENU)
            }
            appMenu = menuCube.getCell([:]) as Map
        }
        catch (Exception e)
        {
            log.debug("Unable to load ${SYS_MENU} (${SYS_MENU} cube likely not in appId: ${appId}, exception: ${e.message}")
            if (!globalMenu)
            {
                appMenu = [(MENU_TITLE):MENU_TITLE_DEFAULT,
                           (MENU_TAB):
                                   ['n-cube':[html:'html/ntwobe.html',img:'img/letter-n.png'],
                                    'n-cube-old':[html:'html/ncube.html',img:'img/letter-o.png'],
                                    'JSON':[html:'html/jsonEditor.html',img:'img/letter-j.png'],
                                    'Details':[html:'html/details.html',img:'img/letter-d.png'],
                                    'Test':[html:'html/test.html',img:'img/letter-t.png'],
                                    'Visualizer':[html:'html/visualize.html', img:'img/letter-v.png']],
                           (MENU_NAV):[:]
                ]
            }
        }

        String title = appMenu[MENU_TITLE] ?: globalMenu[MENU_TITLE]
        Map tabMenu = globalMenu[MENU_TAB] as Map ?: [:]
        tabMenu.putAll((appMenu[MENU_TAB] ?: [:]) as Map)

        Map navMenu = globalMenu[MENU_NAV] as Map ?: [:]
        Map appNavMenu = appMenu[MENU_NAV] as Map
        for (Map.Entry appNavEntry : appNavMenu)
        {
            String navKey = appNavEntry.key
            Map navVal = appNavEntry.value as Map
            if (navMenu.containsKey(navKey))
            {
                (navMenu[navKey] as Map).putAll(navVal)
            }
            else
            {
                navMenu[navKey] = navVal
            }
        }

        return [(MENU_TITLE):title, (MENU_TAB):tabMenu, (MENU_NAV):navMenu]
    }

    Map mapReduce(ApplicationID appId, String cubeName, String colAxisName, String where = 'true', Map options = [:])
    {
        NCube ncube = getCubeInternal(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Cannot mapReduce(). Unable to load cube: ${cubeName} for app: ${appId}")
        }

        Closure whereClosure = evaluateWhereClosure(where)
        Map result = ncube.mapReduce(colAxisName, whereClosure, options)
        return result
    }

    private Closure evaluateWhereClosure(String where)
    {
        Object whereClosure = groovyShell.evaluate(where)
        if (!(whereClosure instanceof Closure))
        {
            throw new IllegalArgumentException("Passed in 'where' clause: ${where}, is not evaluating to a Closure.  Make sure it is in the form (example): { Map input -> input.state == 'AZ' }")
        }
        return (Closure)whereClosure
    }

    Map<String, Object> getVisualizerGraph(ApplicationID appId, Map options)
    {
        Visualizer vis = getVisualizer(options.startCubeName as String)
        return vis.loadGraph(appId, options)
    }

    Map<String, Object> getVisualizerScopeChange(ApplicationID appId, Map options)
    {
        Visualizer vis = getVisualizer(options.startCubeName as String)
        return vis.loadScopeChange(appId, options)
    }

    Map<String, Object> getVisualizerNodeDetails(ApplicationID appId, Map options)
    {
        Visualizer vis = getVisualizer(options.startCubeName as String)
        return vis.loadNodeDetails(appId, options)
    }

    // TODO: This needs to be externalized (loaded via Grapes)
    private Visualizer getVisualizer(String cubeName)
    {
        return cubeName.startsWith(RPM_CLASS) ? new RpmVisualizer(this) : new Visualizer(this)
    }

    Map getCell(ApplicationID appId, String cubeName, Map coordinate, defaultValue = null)
    {
        Map output = [:]
        NCube ncube = getCubeInternal(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Cannot getCell(). Unable to load cube: ${cubeName} for app: ${appId}")
        }

        ncube.getCell(coordinate, output, defaultValue)
        return output
    }

    Map execute(ApplicationID appId, String cubeName, String method, Map args)
    {
        Map coordinate = ['method' : method, 'service': this]
        coordinate.putAll(args)
        return getCell(appId, cubeName, coordinate)
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
    Object[] getCells(ApplicationID appId, String cubeName, Object[] idArrays, Map input, Map output = [:], Object defaultValue = null)
    {
        NCube ncube = getCubeInternal(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Unable to fetch requested cells. NCube: ${cubeName} not found, app: ${appId}")
        }
        return ncube.getCells(idArrays, input, output, defaultValue)
    }

    /**
     *
     * Fetch an array of NCubeInfoDto's where the cube names match the cubeNamePattern (contains) and
     * the content (in JSON format) 'contains' the passed in content String.
     * @param appId ApplicationID on which we are working
     * @param cubeNamePattern cubeNamePattern String pattern to match cube names
     * @param content String value that is 'contained' within the cube's JSON
     * @param options map with possible keys:
     *                changedRecordsOnly - default false ->  Only searches changed records if true.
     *                activeRecordsOnly - default false -> Only searches non-deleted records if true.
     *                deletedRecordsOnly - default false -> Only searches deleted records if true.
     *                cacheResult - default false -> Cache the cubes that match this result..
     * @return List<NCubeInfoDto>
     */
    List<NCubeInfoDto> search(ApplicationID appId, String cubeNamePattern, String content, Map options)
    {
        if (cubeNamePattern != null)
        {
            cubeNamePattern = cubeNamePattern.trim()
        }
        List<NCubeInfoDto> cubeInfos = (List<NCubeInfoDto>) bean.call(beanName, 'search', [appId, cubeNamePattern, content, options])
        Collections.sort(cubeInfos, new Comparator<NCubeInfoDto>() {
            int compare(NCubeInfoDto info1, NCubeInfoDto info2)
            {
                return info1.name.compareToIgnoreCase(info2.name)
            }
        })
        return cubeInfos
    }

    /**
     * Fetch an n-cube by name from the given ApplicationID.  If no n-cubes
     * are loaded, then a loadCubes() call is performed and then the
     * internal cache is checked again.  If the cube is not found, null is
     * returned.
     */
    NCube getCube(ApplicationID appId, String cubeName)
    {
        if (appId == null)
        {
            throw new IllegalArgumentException('ApplicationID cannot be null')
        }
        if (isEmpty(cubeName))
        {
            throw new IllegalArgumentException('cubeName cannot be null')
        }
        return getCubeInternal(appId, cubeName)
    }

    Map getAppTests(ApplicationID appId)
    {
        Map result = bean.call(beanName, 'getAppTests', [appId]) as Map
        return result
    }

    Object[] getTests(ApplicationID appId, String cubeName)
    {
        Object[] result = bean.call(beanName, 'getTests', [appId, cubeName]) as Object[]
        return result
    }

    String getUserId()
    {
        String userId = bean.call(beanName, 'getUserId', []) as String
        return userId
    }

    Boolean updateCube(NCube ncube)
    {
        if (ncube == null)
        {
            throw new IllegalArgumentException('Cannot pass null to updateCube.')
        }
        verifyAllowMutable('updateCube')
        Boolean result = bean.call(beanName, 'updateCube', [ncube.applicationID, ncube.name, ncube.cubeAsGzipJsonBytes]) as Boolean
        if (SYS_CLASSPATH.equalsIgnoreCase(ncube.name))
        {   // If the sys.classpath cube is changed, then the entire class loader must be dropped.  It will be lazily rebuilt.
            clearCache(ncube.applicationID)
        }
        clearCache(ncube.applicationID, [ncube.name])
        ncube.removeMetaProperty(NCube.METAPROPERTY_TEST_DATA)
        return result
    }

    Boolean updateCube(ApplicationID appId, String cubeName, byte[] cubeBytes)
    {
        verifyAllowMutable('updateCube')
        Boolean result = bean.call(beanName, 'updateCube', [appId, cubeName, cubeBytes]) as Boolean
        if (SYS_CLASSPATH.equalsIgnoreCase(cubeName))
        {   // If the sys.classpath cube is changed, then the entire class loader must be dropped.  It will be lazily rebuilt.
            clearCache(appId)
        }
        clearCache(appId, [cubeName])
        return result
    }

    NCube loadCubeById(long id, Map options = null)
    {
        NCubeInfoDto record = loadCubeRecordById(id, options)
        NCube ncube = NCube.createCubeFromRecord(record)
        applyAdvices(ncube)
        return ncube
    }

    NCube loadCube(ApplicationID appId, String cubeName, Map options = null)
    {
        NCubeInfoDto record = loadCubeRecord(appId, cubeName, options)
        NCube ncube = NCube.createCubeFromRecord(record)
        applyAdvices(ncube)
        return ncube
    }

    /**
     * Called from Tests to create NCubes from resource (.json) files.  Also could be called from a Java client
     * that is running with 'ncube.allow.mutable' true.
     * @param ncube NCube to create.
     */
    void createCube(NCube ncube)
    {
        verifyAllowMutable('createCube')
        bean.call(beanName, 'createCube', [ncube.applicationID, ncube.name, ncube.cubeAsGzipJsonBytes])
        ncube.removeMetaProperty(NCube.METAPROPERTY_TEST_DATA)
        prepareCube(ncube)
    }

    NCube createCube(ApplicationID appId, String cubeName, byte[] cubeBytes)
    {
        verifyAllowMutable('createCube')
        NCube ncube = (NCube)bean.call(beanName, 'createCube', [appId, cubeName, cubeBytes])
        ncube.removeMetaProperty(NCube.METAPROPERTY_TEST_DATA)
        prepareCube(ncube)
        return ncube
    }

    Boolean duplicate(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName)
    {
        verifyAllowMutable('duplicate')
        Boolean result = bean.call(beanName, 'duplicate', [oldAppId, newAppId, oldName, newName]) as Boolean
        clearCache(newAppId, [newName])
        return result
    }

    Boolean assertPermissions(ApplicationID appId, String resource, Action action)
    {
        Boolean result = bean.call(beanName, 'assertPermissions', [appId, resource, action]) as Boolean
        return result
    }

    Map checkMultiplePermissions(ApplicationID appId, String resource, Object[] actions)
    {
        Map result = bean.call(beanName, 'checkMultiplePermissions', [appId, resource, actions]) as Map
        return result
    }

    Boolean checkPermissions(ApplicationID appId, String resource, Action action)
    {
        Boolean result = bean.call(beanName, 'checkPermissions', [appId, resource, action]) as Boolean
        return result
    }

    Boolean isSysAdmin()
    {
        Boolean result = bean.call(beanName, 'isSysAdmin', []) as Boolean
        return result
    }

    Boolean isAppAdmin(ApplicationID appId)
    {
        Boolean result = bean.call(beanName, 'isAppAdmin', [appId]) as Boolean
        return result
    }

    String getAppLockedBy(ApplicationID appId)
    {
        String result = bean.call(beanName, 'getAppLockedBy', [appId]) as String
        return result
    }

    Boolean lockApp(ApplicationID appId, boolean shouldLock)
    {
        verifyAllowMutable('lockApp')
        Boolean result = bean.call(beanName, 'lockApp', [appId, shouldLock]) as Boolean
        return result
    }

    Integer moveBranch(ApplicationID appId, String newSnapVer)
    {
        verifyAllowMutable('moveBranch')
        Integer result = bean.call(beanName, 'moveBranch', [appId, newSnapVer]) as Integer
        clearCache(appId)
        return result
    }

    Integer releaseVersion(ApplicationID appId, String newSnapVer = null)
    {
        verifyAllowMutable('releaseVersion')
        Integer result = bean.call(beanName, 'releaseVersion', [appId, newSnapVer]) as Integer
        clearCache()
        return result
    }

    Integer releaseCubes(ApplicationID appId, String newSnapVer = null)
    {
        verifyAllowMutable('releaseCubes')
        Integer result = bean.call(beanName, 'releaseCubes', [appId, newSnapVer]) as Integer
        clearCache()
        return result
    }

    Boolean restoreCubes(ApplicationID appId, Object[] cubeNames)
    {
        verifyAllowMutable('restoreCubes')
        Boolean result = bean.call(beanName, 'restoreCubes', [appId, cubeNames]) as Boolean
        return result
    }

    List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion = false)
    {
        List<NCubeInfoDto> result = bean.call(beanName, 'getRevisionHistory', [appId, cubeName, ignoreVersion]) as List
        return result
    }

    NCubeInfoDto promoteRevision(long cubeId)
    {
        verifyAllowMutable('promoteRevision')
        NCubeInfoDto record = bean.call(beanName, 'promoteRevision', [cubeId]) as NCubeInfoDto
        if (SYS_CLASSPATH.equalsIgnoreCase(record.name))
        {   // If the sys.classpath cube is changed, then the entire class loader must be dropped.  It will be lazily rebuilt.
            clearCache(record.applicationID)
        }
        clearCache(record.applicationID, [record.name])
        return record
    }

    List<Delta> fetchJsonRevDiffs(long newCubeId, long oldCubeId)
    {
        List<Delta> deltas = bean.call(beanName, 'fetchJsonRevDiffs', [newCubeId, oldCubeId]) as List
        return deltas
    }

    List<Delta> fetchJsonBranchDiffs(NCubeInfoDto newInfoDto, NCubeInfoDto oldInfoDto)
    {
        List<Delta> deltas = bean.call(beanName, 'fetchJsonBranchDiffs', [newInfoDto, oldInfoDto]) as List
        return deltas
    }

    List<NCubeInfoDto> getCellAnnotation(ApplicationID appId, String cubeName, Set<Long> ids, boolean ignoreVersion = false)
    {
        List<NCubeInfoDto> result = bean.call(beanName, 'getCellAnnotation', [appId, cubeName, ids, ignoreVersion]) as List
        return result
    }

    Object[] getAppNames()
    {
        Object[] result = bean.call(beanName, 'getAppNames', []) as Object[]
        return result
    }

    Object[] getVersions(String app)
    {
        Object[] result = bean.call(beanName, 'getVersions', [app]) as Object[]
        return result
    }

    Integer copyBranch(ApplicationID srcAppId, ApplicationID targetAppId, boolean copyWithHistory = false)
    {
        verifyAllowMutable('copyBranch')
        Integer result = bean.call(beanName, 'copyBranch', [srcAppId, targetAppId, copyWithHistory]) as Integer
        clearCache(targetAppId)
        return result
    }

    Object[] getBranches(ApplicationID appId)
    {
        Object[] result = bean.call(beanName, 'getBranches', [appId]) as Object[]
        return result
    }

    Integer getBranchCount(ApplicationID appId)
    {
        Integer result = bean.call(beanName, 'getBranchCount', [appId]) as Integer
        return result
    }

    NCubeInfoDto loadCubeRecord(ApplicationID appId, String cubeName, Map options)
    {
        NCubeInfoDto record = bean.call(beanName, 'loadCubeRecord', [appId, cubeName, options]) as NCubeInfoDto
        return record
    }

    NCubeInfoDto loadCubeRecordById(long id, Map options = null)
    {
        NCubeInfoDto record = bean.call(beanName, 'loadCubeRecordById', [id, options]) as NCubeInfoDto
        return record
    }

    String getJson(ApplicationID appId, String cubeName, Map options)
    {
        try
        {
            NCube ncube = getCube(appId, cubeName)
            if (ncube == null)
            {
                throw new IllegalArgumentException("Cannot get JSON.  Unable to load cube: ${cubeName} for app: ${appId}")
            }
            return NCube.formatCube(ncube, options)
        }
        catch (IllegalStateException e)
        {
            if (['json','json-pretty'].contains(options.mode))
            {
                log.error(e.message, e)
                NCubeInfoDto record = loadCubeRecord(appId, cubeName, options)
                String json = new String(uncompressBytes(record.bytes), 'UTF-8')
                if ('json-pretty' == options.mode)
                {
                    return JsonWriter.formatJson(json)
                }
                return json
            }
            else
            {
                throw e
            }
        }
    }

    Boolean deleteBranch(ApplicationID appId)
    {
        verifyAllowMutable('deleteBranch')
        Boolean result = bean.call(beanName, 'deleteBranch', [appId]) as Boolean
        clearCache(appId)
        return result
    }

    Boolean deleteApp(ApplicationID appId)
    {
        verifyAllowMutable('deleteApp')
        Boolean result = bean.call(beanName, 'deleteApp', [appId]) as Boolean
        clearCache(appId)
        return result
    }

    Boolean mergeDeltas(ApplicationID appId, String cubeName, List<Delta> deltas)
    {
        verifyAllowMutable('mergeDeltas')
        Boolean result = bean.call(beanName, 'mergeDeltas', [appId, cubeName, deltas])
        clearCache(appId, [cubeName])
        return result
    }

    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames)
    {
        verifyAllowMutable('deleteCubes')
        Boolean result = bean.call(beanName, 'deleteCubes', [appId, cubeNames]) as Boolean
        if (result)
        {
            for (Object cubeName : cubeNames)
            {
                clearCache(appId, [cubeName as String])
            }
        }
        return result
    }

    void changeVersionValue(ApplicationID appId, String newVersion)
    {
        verifyAllowMutable('changeVersionValue')
        bean.call(beanName, 'changeVersionValue', [appId, newVersion])
        clearCache(appId)
        clearCache(appId.asVersion(newVersion))
    }

    Boolean renameCube(ApplicationID appId, String oldName, String newName)
    {
        verifyAllowMutable('renameCube')
        Boolean result = bean.call(beanName, 'renameCube', [appId, oldName, newName]) as Boolean
        clearCache(appId, [oldName, newName])
        return result
    }

    Set<String> getReferencesFrom(ApplicationID appId, String cubeName)
    {
        Object[] results = bean.call(beanName, 'getReferencesFrom', [appId, cubeName]) as Object[]
        Set<String> refs = new CaseInsensitiveSet()
        results.each { result -> refs.add((String)result) }
        return refs
    }

    List<AxisRef> getReferenceAxes(ApplicationID appId)
    {
        List<AxisRef> result = bean.call(beanName, 'getReferenceAxes', [appId]) as List
        return result
    }

    void updateReferenceAxes(Object[] axisRefs)
    {
        verifyAllowMutable('updateReferenceAxes')
        bean.call(beanName, 'updateReferenceAxes', [axisRefs])
        clearCache()
    }

    ApplicationID getApplicationID(String tenant, String app, Map<String, Object> coord)
    {
        ApplicationID.validateTenant(tenant)
        ApplicationID.validateApp(tenant)

        if (coord == null)
        {
            coord = [:]
        }

        NCube bootCube = getCube(getBootVersion(tenant, app), SYS_BOOTSTRAP)

        if (bootCube == null)
        {
            throw new IllegalStateException("Missing ${SYS_BOOTSTRAP} cube in the 0.0.0 version for the app: ${app}")
        }

        Map copy = new CaseInsensitiveMap(coord, new LinkedHashMap<>(coord.size()))
        ApplicationID bootAppId = (ApplicationID) bootCube.getCell(copy, [:])
        String version = bootAppId.version
        String status = bootAppId.status
        String branch = bootAppId.branch

        if (!tenant.equalsIgnoreCase(bootAppId.tenant))
        {
            log.warn("sys.bootstrap cube for tenant: ${tenant}, app: ${app} is returning a different tenant: ${bootAppId.tenant} than requested. Using ${tenant} instead.")
        }

        if (!app.equalsIgnoreCase(bootAppId.app))
        {
            log.warn("sys.bootstrap cube for tenant: ${tenant}, app: ${app} is returning a different app: ${bootAppId.app} than requested. Using ${app} instead.")
        }

        return new ApplicationID(tenant, app, version, status, branch)
    }

    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)
    {
        verifyAllowMutable('updateAxisMetaProperties')
        bean.call(beanName, 'updateAxisMetaProperties', [appId, cubeName, axisName, newMetaProperties])
        clearCache(appId, [cubeName])
    }

    Boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    {
        verifyAllowMutable('updateNotes')
        Boolean result = bean.call(beanName, 'updateNotes', [appId, cubeName, notes]) as Boolean
        return result
    }

    String getNotes(ApplicationID appId, String cubeName)
    {
        String result = bean.call(beanName, 'getNotes', [appId, cubeName]) as String
        return result
    }

    void clearTestDatabase()
    {
        verifyAllowMutable('clearTestDatabase')
        bean.call(beanName, 'clearTestDatabase', [])
    }

    void clearPermCache()
    {
        verifyAllowMutable('clearPermCache')
        bean.call(beanName, 'clearPermCache', [])
    }

    List<NCubeInfoDto> getHeadChangesForBranch(ApplicationID appId)
    {
        List<NCubeInfoDto> changes = bean.call(beanName, 'getHeadChangesForBranch', [appId]) as List<NCubeInfoDto>
        return changes
    }

    List<NCubeInfoDto> getBranchChangesForHead(ApplicationID appId)
    {
        List<NCubeInfoDto> changes = bean.call(beanName, 'getBranchChangesForHead', [appId]) as List<NCubeInfoDto>
        return changes
    }

    List<NCubeInfoDto> getBranchChangesForMyBranch(ApplicationID appId, String branch)
    {
        List<NCubeInfoDto> changes = bean.call(beanName, 'getBranchChangesForMyBranch', [appId, branch]) as List<NCubeInfoDto>
        return changes
    }

    Map<String, Object> updateBranch(ApplicationID appId, Object[] cubeDtos = null)
    {
        verifyAllowMutable('updateBranch')
        Map<String, Object> map = bean.call(beanName, 'updateBranch', [appId, cubeDtos]) as Map<String, Object>
        clearCache(appId)
        return map
    }

    String generatePullRequestHash(ApplicationID appId, Object[] infoDtos, String notes = '')
    {
        verifyAllowMutable('generatePullRequestHash')
        String link = bean.call(beanName, 'generatePullRequestHash', [appId, infoDtos, notes]) as String
        return link
    }

    Map<String, Object> mergePullRequest(String prId)
    {
        verifyAllowMutable('mergePullRequest')
        Map result = bean.call(beanName, 'mergePullRequest', [prId]) as Map

        ApplicationID prApp = result[PR_APP] as ApplicationID
        ApplicationID headAppId = prApp.asHead()
        clearCache(headAppId)
        String prCube = result[PR_CUBE] as String
        clearCache(prApp, [prCube])
        return result
    }

    void obsoletePullRequest(String prId)
    {
        verifyAllowMutable('obsoletePullRequest')
        bean.call(beanName, 'obsoletePullRequest', [prId]) as NCube
    }

    void cancelPullRequest(String prId)
    {
        verifyAllowMutable('cancelPullRequest')
        bean.call(beanName, 'cancelPullRequest', [prId]) as NCube
    }

    void reopenPullRequest(String prId)
    {
        verifyAllowMutable('reopenPullRequest')
        bean.call(beanName, 'reopenPullRequest', [prId]) as NCube
    }

    Object[] getPullRequests(Date startDate = null, Date endDate = null, String prId = null)
    {
        verifyAllowMutable('getPullRequests')
        Object[] result = bean.call(beanName, 'getPullRequests', [startDate, endDate, prId]) as Object[]
        return result
    }

    Map<String, Object> commitBranch(ApplicationID appId, Object[] inputCubes = null, String notes = null)
    {
        verifyAllowMutable('commitBranch')
        Map<String, Object> map = bean.call(beanName, 'commitBranch', [appId, inputCubes, notes]) as Map<String, Object>
        clearCache(appId)
        clearCache(appId.asHead())
        return map
    }

    Integer rollbackBranch(ApplicationID appId, Object[] names)
    {
        verifyAllowMutable('rollbackBranch')
        Integer result = bean.call(beanName, 'rollbackBranch', [appId, names]) as Integer
        clearCache(appId)
        return result
    }

    Integer acceptMine(ApplicationID appId, Object[] cubeNames)
    {
        verifyAllowMutable('acceptMine')
        Integer result = bean.call(beanName, 'acceptMine', [appId, cubeNames]) as Integer
        return result
    }

    Integer acceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch = ApplicationID.HEAD)
    {
        verifyAllowMutable('acceptTheirs')
        Integer result = bean.call(beanName, 'acceptTheirs', [appId, cubeNames, sourceBranch]) as Integer
        clearCache(appId)
        return result
    }

    Boolean isCubeUpToDate(ApplicationID appId, String cubeName)
    {
        Boolean result = bean.call(beanName, 'isCubeUpToDate', [appId, cubeName]) as Boolean
        return result
    }

    ApplicationID getBootVersion(String tenant, String app)
    {
        String branch = getSystemParams()[NCUBE_PARAMS_BRANCH]
        return new ApplicationID(tenant, app, "0.0.0", ReleaseStatus.SNAPSHOT.name(), isEmpty(branch) ? ApplicationID.HEAD : branch)
    }

    private void verifyAllowMutable(String methodName)
    {
        if (!allowMutableOn)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} ${methodName}()")
        }
    }

    void createRefAxis(ApplicationID appId, String cubeName, String axisName, ApplicationID refAppId, String refCubeName, String refAxisName)
    {
        verifyAllowMutable('createRefAxis')
        bean.call(beanName, 'createRefAxis', [appId, cubeName, axisName, refAppId, refCubeName, refAxisName])
        clearCache(appId, [cubeName])
    }

    //-- Run Tests -------------------------------------------------------------------------------------------------

    Map runTests(ApplicationID appId)
    {
        Map ret = [:]
        Map appTests = getAppTests(appId)
        for (Map.Entry cubeData : appTests)
        {
            String cubeName = cubeData.key
            ret[cubeName] = runTests(appId, cubeName, cubeData.value as Object[])
        }
        return ret
    }

    Map runTests(ApplicationID appId, String cubeName, Object[] tests)
    {
        Map ret = [:]
        for (Object t : tests)
        {
            NCubeTest test = t as NCubeTest
            try
            {
                ret[test.name] = runTest(appId, cubeName, test)
            }
            catch (Exception e)
            {
                ret[test.name] = ['_message':e.message, '_result':e.stackTrace] as Map
            }
        }
        return ret
    }

    Map runTest(ApplicationID appId, String cubeName, NCubeTest test)
    {
        try
        {
            // Do not remove try-catch handler here - this API must handle it's own exceptions, instead
            // of allowing the Around Advice to handle them.
            Properties props = System.properties
            String server = props.getProperty("http.proxyHost")
            String port = props.getProperty("http.proxyPort")
            log.info("proxy server: ${server}, proxy port: ${port}".toString())

            NCube ncube = getCube(appId, cubeName)
            if (ncube == null)
            {
                throw new IllegalArgumentException("Failed to run test. Unable to load cube: ${cubeName} for app: ${appId}")
            }
            Map<String, Object> coord = test.coordWithValues
            boolean success = true
            Map output = new LinkedHashMap()
            Map args = [input:coord, output:output, ncube:ncube]
            Map<String, Object> copy = new CompactCILinkedMap<>(coord)

            // If any of the input values are a CommandCell, execute them.  Use the fellow (same) input as input.
            // In other words, other key/value pairs on the input map can be referenced in a CommandCell.
            copy.each { k, value ->
                String key = k
                if (value instanceof CommandCell)
                {
                    CommandCell cmd = (CommandCell) value
                    redirectOutput(true)
                    coord.put(key, cmd.execute(args))
                    redirectOutput(false)
                }
            }

            Set<String> errors = new LinkedHashSet<>()
            redirectOutput(true)
            ncube.getCell(coord, output)               // Execute test case
            redirectOutput(false)

            List<GroovyExpression> assertions = test.createAssertions()
            int i = 0

            for (GroovyExpression exp : assertions)
            {
                i++

                try
                {
                    Map assertionOutput = new LinkedHashMap<>(output)
                    RuleInfo ruleInfo = new RuleInfo()
                    assertionOutput.put((NCube.RULE_EXEC_INFO), ruleInfo)
                    args.output = assertionOutput
                    redirectOutput(true)
                    if (!exp.execute(args))
                    {
                        errors.add("[assertion ${i} failed]: ${exp.cmd}".toString())
                        success = false
                    }
                    redirectOutput(false)
                }
                catch (ThreadDeath t)
                {
                    throw t
                }
                catch (Throwable e)
                {
                    errors.add('[exception]')
                    errors.add('\n')
                    errors.add(getTestCauses(e))
                    success = false
                }
            }

            RuleInfo ruleInfoMain = (RuleInfo) output.get((NCube.RULE_EXEC_INFO))
            ruleInfoMain.setSystemOut(fetchRedirectedOutput())
            ruleInfoMain.setSystemErr(fetchRedirectedErr())
            ruleInfoMain.setAssertionFailures(errors)
            return ['_message': new TestResultsFormatter(output).format(), '_result': success]
        }
        catch(Exception e)
        {
            fetchRedirectedOutput()
            fetchRedirectedErr()
            throw new IllegalStateException(getTestCauses(e), e)
        }
        finally
        {
            redirectOutput(false)
        }
    }

    private static String fetchRedirectedOutput()
    {
        OutputStream outputStream = System.out
        if (outputStream instanceof ThreadAwarePrintStream)
        {
            return ((ThreadAwarePrintStream) outputStream).content
        }
        return ''
    }

    private static String fetchRedirectedErr()
    {
        OutputStream outputStream = System.err
        if (outputStream instanceof ThreadAwarePrintStreamErr)
        {
            return ((ThreadAwarePrintStreamErr) outputStream).content
        }
        return ''
    }

    private static void redirectOutput(boolean redirect)
    {
        OutputStream outputStream = System.out
        if (outputStream instanceof ThreadAwarePrintStream)
        {
            ((ThreadAwarePrintStream) outputStream).redirect = redirect
        }
        outputStream = System.err
        if (outputStream instanceof ThreadAwarePrintStreamErr)
        {
            ((ThreadAwarePrintStreamErr) outputStream).redirect = redirect
        }
    }

    /**
     * Given an exception, get an HTML version of it.  This version is reversed in order,
     * so that the root cause is first, and then the caller, and so on.
     * @param t Throwable exception for which to obtain the HTML
     * @return String version of the Throwable in HTML format.  Surrounded with pre-tag.
     */
    String getTestCauses(Throwable t)
    {
        LinkedList<?> stackTraces = new LinkedList<>()

        while (true)
        {
            stackTraces.push([msg: t.localizedMessage, trace: t.stackTrace])
            t = t.cause
            if (t == null)
            {
                break
            }
        }

        // Convert from LinkedList to direct access list
        List<?> stacks = new ArrayList<>(stackTraces)
        StringBuilder s = new StringBuilder()
        final int len = stacks.size()

        for (int i=0; i < len; i++)
        {
            Map map = (Map)stacks[i]
            s.append("""<b style="color:darkred">${map.msg}</b><br>""")

            if (i != len - 1i)
            {
                Map nextStack = (Map)stacks[i + 1i]
                StackTraceElement[] nextStackElementArray = (StackTraceElement[]) nextStack.trace
                s.append(trace(map.trace as StackTraceElement[], nextStackElementArray))
                s.append('<hr style="border-top: 1px solid #aaa;margin:8px"><b>Called by:</b><br>')
            }
            else
            {
                s.append(trace(map.trace as StackTraceElement[], null))
            }
        }

        return "<pre>${s}</pre>"
    }

    private static String trace(StackTraceElement[] stackTrace, StackTraceElement[] nextStrackTrace)
    {
        StringBuilder s = new StringBuilder()
        final int len = stackTrace.length
        for (int i=0; i < len; i++)
        {
            s.append('&nbsp;&nbsp;')
            StackTraceElement element = stackTrace[i]
            if (alreadyExists(element, nextStrackTrace))
            {
                s.append('...continues below<br>')
                return s.toString()
            }
            else
            {
                s.append("""${element.className}.${element.methodName}()&nbsp;<small><b class="pull-right">""")

                if (element.nativeMethod)
                {
                    s.append('Native Method')
                }
                else
                {
                    if (element.fileName)
                    {
                        s.append("${element.fileName}:${element.lineNumber}")
                    }
                    else
                    {
                        s.append('source n/a')
                    }
                }
                s.append('</b></small><br>')
            }
        }

        return s.toString()
    }

    private static boolean alreadyExists(StackTraceElement element, StackTraceElement[] stackTrace)
    {
        if (ArrayUtilities.isEmpty(stackTrace))
        {
            return false
        }

        for (StackTraceElement traceElement : stackTrace)
        {
            if (element == traceElement)
            {
                return true
            }
        }
        return false
    }

    //-- NCube Caching -------------------------------------------------------------------------------------------------

    /**
     * Clear the cube (and other internal caches) for a given ApplicationID.
     * This will remove all the n-cubes from memory, compiled Groovy code,
     * caches related to expressions, caches related to method support,
     * advice caches, and local classes loaders (used when no sys.classpath is
     * present). If NCube names are passed as the second argument, only those
     * NCubes will be evicted from the cache.
     *
     * @param appId ApplicationID for which the cache is to be cleared.
     * @param cubeNames (optional, defaults to null) Collection<String> for specific NCubes to clear from cache.
     */
    void clearCache(ApplicationID appId, Collection<String> cubeNames = null)
    {
        ApplicationID.validateAppId(appId)
        if (cubeNames != null)
        {
            Cache cubeCache = ncubeCacheManager.getCache(appId.cacheKey())
            for (String cubeName : cubeNames)
            {
                cubeCache.evict(cubeName.toLowerCase())
            }
        }
        else
        {
            synchronized (ncubeCacheManager) {
                String cacheKey = appId.cacheKey()
                // Clear NCube cache
                Cache cubeCache = ncubeCacheManager.getCache(cacheKey)
                cubeCache.clear()   // eviction will trigger removalListener, which clears other NCube internal caches

                GroovyBase.clearCache(appId)
                NCubeGroovyController.clearCache(appId)

                // Clear Advice cache
                Cache adviceCache = adviceCacheManager.getCache(cacheKey)
                adviceCache.clear()

                // Clear ClassLoader cache
                GroovyClassLoader classLoader = localClassLoaders[appId]
                if (classLoader != null)
                {
                    classLoader.clearCache()
                    localClassLoaders.remove(appId)
                }
            }
        }
    }

    void clearCache()
    {
        ncubeCacheManager.cacheNames.each { String cacheKey ->
            ApplicationID appId = ApplicationID.convert(cacheKey)
            clearCache(appId)
        }
    }

    Cache getCacheForApp(ApplicationID appId)
    {
        Cache cache = ncubeCacheManager.getCache(appId.cacheKey())
        return cache
    }
    
    /**
     * Add a cube to the internal cache of available cubes.
     * @param ncube NCube to add to the list.
     */
    void addCube(NCube ncube, boolean force = true)
    {
        ApplicationID appId = ncube.applicationID
        ApplicationID.validateAppId(appId)
        validateCube(ncube)
        prepareCube(ncube, force)
    }

    /**
     * @return boolean true if the named cube is cached in the Runtime cache, false otherwise.  If the name is
     * of a non-existent cube, the return value will be false.
     */
    boolean isCached(ApplicationID appId, String cubeName)
    {
        Cache cubeCache = ncubeCacheManager.getCache(appId.cacheKey())
        String loName = cubeName.toLowerCase()
        Cache.ValueWrapper wrapper = cubeCache.get(loName)
        if (wrapper != null)
        {
            def value = wrapper.get()
            return value instanceof NCube
        }
        return false
    }
    
    private NCube cacheCube(NCube ncube, boolean force = false)
    {
        ApplicationID appId = ncube.applicationID
        if (appId.release || !ncube.metaProperties.containsKey(PROPERTY_CACHE) || Boolean.TRUE == ncube.getMetaProperty(PROPERTY_CACHE))
        {
            Cache cubeCache = ncubeCacheManager.getCache(appId.cacheKey())
            String loName = ncube.name.toLowerCase()

            if (allowMutableOn || force)
            {
                cubeCache.put(loName, ncube)
            }
            else
            {
                Cache.ValueWrapper wrapper = cubeCache.putIfAbsent(loName, ncube)
                if (wrapper != null)
                {
                    def value = wrapper.get()
                    if (value instanceof NCube)
                    {
                        ncube = (NCube)value
                    }
                    else
                    {
                        log.info('Updating n-cube cache entry on a non-existing cube (cached = false) to an n-cube.')
                        cubeCache.put(loName, ncube)
                    }
                }
            }
        }
        return ncube
    }

    private NCube getCubeInternal(ApplicationID appId, String cubeName)
    {
        Cache cubeCache = ncubeCacheManager.getCache(appId.cacheKey())
        boolean localCacheEnabled = localFileCache?.enabled
        final String lowerCubeName = cubeName.toLowerCase()

        Cache.ValueWrapper item = cubeCache.get(lowerCubeName)
        if (item != null)
        {   // pull from cache
            Object value = item.get()
            return Boolean.FALSE == value ? null : value as NCube
        }

        NCube ncube = null
        if (localCacheEnabled) {
            item = localFileCache.get(appId,cubeName)
            if (item?.get() instanceof NCube) {
                ncube = (NCube) item.get()
            }
        }

        if (!item || (localCacheEnabled && appId.snapshot && localFileCache.snapshotPolicy==FORCE)) {
            // now even items with metaProperties(cache = 'false') can be retrieved
            // and normal app processing doesn't do two queries anymore.
            // used to do getCubeInfoRecords() -> dto
            // and then dto -> loadCube(id)

            Map options = null
            if (localCacheEnabled && ncube!=null) {
                // data will only be returned if sha1 is different than supplied value
                options = [(SEARCH_INCLUDE_CUBE_DATA):true, (SEARCH_CHECK_SHA1):ncube.sha1()]
            }

            NCubeInfoDto record = loadCubeRecord(appId, cubeName, options)
            if (record == null ) {
                ncube = null    // reset in case cube had existed in cache
            }
            else if (record.hasCubeData()) {
                ncube = NCube.createCubeFromRecord(record)
            }

            // update cache if item is new (didn't exist before) or changed
            if (localCacheEnabled && (!item || item.get() != ncube)) {
                localFileCache.put(appId, cubeName, ncube)
            }
        }

        if (ncube==null)
        {
            cubeCache.put(lowerCubeName, false)
            return null
        }
        else {
            return prepareCube(ncube)
        }
    }

    private NCube prepareCube(NCube ncube, boolean force = false)
    {
        applyAdvices(ncube)
        NCube cachedCube = cacheCube(ncube, force)
        return cachedCube
    }

    //-- Advice --------------------------------------------------------------------------------------------------------

    /**
     * Associate Advice to all n-cubes that match the passed in regular expression.
     */
    void addAdvice(ApplicationID appId, String wildcard, Advice advice)
    {
        ApplicationID.validateAppId(appId)
        Cache current = adviceCacheManager.getCache(appId.cacheKey())
        current.put("${advice.name}/${wildcard}".toString(), advice)

        // Apply newly added advice to any fully loaded (hydrated) cubes.
        String regex = wildcardToRegexString(wildcard)
        Pattern pattern = Pattern.compile(regex)

        if (ncubeCacheManager instanceof GCacheManager)
        {
            ((GCacheManager)ncubeCacheManager).applyToEntries(appId.cacheKey(), {String key, Object value ->
                if (value instanceof NCube)
                {   // apply advice to hydrated cubes
                    NCube ncube = value as NCube
                    Axis axis = ncube.getAxis('method')
                    addAdviceToMatchedCube(advice, pattern, ncube, axis)
                }
            })
        }
    }

    private static void addAdviceToMatchedCube(Advice advice, Pattern pattern, NCube ncube, Axis axis)
    {
        if (axis != null)
        {   // Controller methods
            for (Column column : axis.columnsWithoutDefault)
            {
                String method = column.value.toString()
                String classMethod = "${ncube.name}.${method}()"
                if (pattern.matcher(classMethod).matches())
                {
                    ncube.addAdvice(advice, method)
                }
            }
        }

        // Add support for run() method (inline GroovyExpressions)
        String classMethod = "${ncube.name}.run()"
        if (pattern.matcher(classMethod).matches())
        {
            ncube.addAdvice(advice, 'run')
        }
    }

    /**
     * Apply existing advices loaded into the NCubeRuntime, to the passed in
     * n-cube.  This allows advices to be added first, and then let them be
     * applied 'on demand' as an n-cube is loaded later.
     * @param appId ApplicationID
     * @param ncube NCube to which all matching advices will be applied.
     */
    private void applyAdvices(NCube ncube)
    {
        if (ncube && adviceCacheManager instanceof GCacheManager)
        {
            ApplicationID appId = ncube.applicationID
            String cacheKey = appId.cacheKey()
            GCacheManager acm = (GCacheManager)adviceCacheManager
            if (ncube.name != SYS_ADVICE)
            {
                addSysAdviceAdvices(acm.getCache(cacheKey), appId)
            }
            acm.applyToEntries(cacheKey, {String key, Object value ->
                final Advice advice = value as Advice
                final String wildcard = key.replace("${advice.name}/", "")
                final String regex = wildcardToRegexString(wildcard)
                final Axis axis = ncube.getAxis('method')
                addAdviceToMatchedCube(advice, Pattern.compile(regex), ncube, axis)
            })
        }
    }

    private void addSysAdviceAdvices(Cache cache, ApplicationID appId)
    {
        com.google.common.cache.Cache gCache = ((GuavaCache)cache).nativeCache
        Iterator i = gCache.asMap().entrySet().iterator()
        if (i.hasNext())
        {
            return
        }
        NCube sysAdviceCube = getCube(appId, SYS_ADVICE)
        if (!sysAdviceCube)
        {
            return
        }
        Axis adviceAxis = sysAdviceCube.getAxis('advice')
        if (!adviceAxis)
        {
            throw new IllegalStateException("sys.advice is malformed for app: ${appId}")
        }
        for (Column column : adviceAxis.columns)
        {
            Map map = sysAdviceCube.getMap([advice: column.value, attribute: new LinkedHashSet()])
            addAdvice(appId, (String)map.pattern, (Advice)map.expression)
        }
    }

    //-- Classloader / Classpath ---------------------------------------------------------------------------------------

    /**
     * Resolve the passed in String URL to a fully qualified URL object.  If the passed in String URL is relative
     * to a path in the sys.classpath, this method will perform (indirectly) the necessary HTTP HEAD requests to
     * determine which path it connects to.
     * @param url String url (relative or absolute)
     * @param input Map coordinate that the reuqested the URL (may include environment level settings that
     *              help sys.classpath select the correct ClassLoader.
     * @return URL fully qualified URL based on the passed in relative or absolute URL String.
     */
    URL getActualUrl(ApplicationID appId, String url, Map input)
    {
        ApplicationID.validateAppId(appId)
        if (isEmpty(url))
        {
            throw new IllegalArgumentException("URL cannot be null or empty, attempting to resolve relative to absolute url for app: ${appId}")
        }
        String localUrl = url.toLowerCase()

        if (localUrl.startsWith('http:') || localUrl.startsWith('https:') || localUrl.startsWith('file:'))
        {   // Absolute URL
            try
            {
                return new URL(url)
            }
            catch (MalformedURLException e)
            {
                throw new IllegalArgumentException("URL is malformed: ${url}", e)
            }
        }
        else
        {
            URL actualUrl
            synchronized (url.intern()) {
                URLClassLoader loader = getUrlClassLoader(appId, input)

                // Make URL absolute (uses URL roots added to NCubeRuntime)
                actualUrl = loader.getResource(url)
            }

            if (actualUrl == null)
            {
                String err = "Unable to resolve URL, make sure appropriate resource URLs are added to the sys.classpath cube, URL: ${url}, app: ${appId}"
                throw new IllegalArgumentException(err)
            }
            return actualUrl
        }
    }

    String getUrlContent(ApplicationID appId, String url, Map input)
    {
        URL actualUrl = getActualUrl(appId, url, input)
        return actualUrl?.text
    }

    /**
     * Fetch the classloader for the given ApplicationID.
     */
    URLClassLoader getUrlClassLoader(ApplicationID appId, Map input)
    {
        NCube cpCube = getCube(appId, SYS_CLASSPATH)

        if (cpCube == null)
        {   // No sys.classpath cube exists, just create regular GroovyClassLoader with no URLs set into it.
            // Scope the GroovyClassLoader per ApplicationID
            return getLocalClassloader(appId)
        }

        // duplicate input coordinate - no need to validate, that will be done inside cpCube.getCell() later.
        Map copy = new CompactCILinkedMap(input)

        final String envLevel = getExternalVariable('ENV_LEVEL')
        if (hasContent(envLevel) && !doesMapContainKey(copy, 'env'))
        {   // Add in the 'ENV_LEVEL" environment variable when looking up sys.* cubes,
            // if there was not already an entry for it.
            copy.env = envLevel
        }
        if (!doesMapContainKey(copy, 'username'))
        {   // same as ENV_LEVEL, add if not already there.
            copy.username = System.getProperty('user.name')
        }

        Object urlCpLoader = cpCube.getCellById(cpCube.getCoordinateKey(copy), copy, [:])

        if (urlCpLoader instanceof URLClassLoader)
        {
            return (URLClassLoader)urlCpLoader
        }

        throw new IllegalStateException('If the sys.classpath cube exists, it must return a URLClassLoader.')
    }

    URLClassLoader getLocalClassloader(ApplicationID appId)
    {
        GroovyClassLoader gcl = localClassLoaders[appId]
        if (gcl == null)
        {
            gcl = new CdnClassLoader(NCubeRuntime.class.classLoader)
            GroovyClassLoader classLoaderRef = localClassLoaders.putIfAbsent(appId, gcl)
            if (classLoaderRef != null)
            {
                gcl = classLoaderRef
            }
        }
        return gcl
    }

    private static boolean doesMapContainKey(Map map, String key)
    {
        if (map instanceof TrackingMap)
        {   // Bypass marking the tracking key as accessed
            Map wrappedMap = ((TrackingMap)map).getWrappedMap()
            return wrappedMap.containsKey(key)
        }
        return map.containsKey(key)
    }

    //-- Environment Variables -----------------------------------------------------------------------------------------

    Map<String, Object> getSystemParams()
    {
        final ConcurrentMap<String, Object> params = systemParams

        if (params != null)
        {
            return params
        }

        synchronized (NCubeRuntime.class)
        {
            if (systemParams == null)
            {
                String jsonParams = getExternalVariable(NCUBE_PARAMS)
                ConcurrentMap<String, Object> sysParamMap = new ConcurrentHashMap<>()

                if (hasContent(jsonParams))
                {
                    try
                    {
                        sysParamMap = new ConcurrentHashMap<>((Map) JsonReader.jsonToJava(jsonParams, [(JsonReader.USE_MAPS): true] as Map))
                    }
                    catch (Exception ignored)
                    {
                        log.warn("Parsing of NCUBE_PARAMS failed: ${jsonParams}")
                    }
                }
                systemParams = sysParamMap
            }
        }
        return systemParams
    }

    void clearSysParams()
    {
        systemParams = null
    }

    //-- Resource APIs -------------------------------------------------------------------------------------------------

    static String getResourceAsString(String name) throws Exception
    {
        URL url = NCubeRuntime.class.getResource("/${name}")
        BufferedReader reader = new BufferedReader(new InputStreamReader(url.newInputStream()))
        return reader.text
    }
    
    NCube getNCubeFromResource(ApplicationID appId, String name)
    {
        try
        {
            String json = getResourceAsString(name)
            NCube ncube = NCube.fromSimpleJson(json)
            ncube.applicationID = appId
            ncube.sha1()
            prepareCube(ncube, true)
            return ncube
        }
        catch (NullPointerException e)
        {
            throw new IllegalArgumentException("Could not find the file [n-cube]: ${name}, app: ${appId}", e)
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException)e
            }
            throw new RuntimeException("Failed to load cube from resource: ${name}", e)
        }
    }

    /**
     * Still used in getNCubesFromResource
     */
    private static Object[] getJsonObjectFromResource(String name) throws IOException
    {
        JsonReader reader = null
        try
        {
            URL url = NCubeRuntime.class.getResource('/' + name)
            File jsonFile = new File(url.file)
            InputStream input = new BufferedInputStream(new FileInputStream(jsonFile))
            reader = new JsonReader(input, true)
            return (Object[]) reader.readObject()
        }
        finally
        {
            close(reader)
        }
    }

    List<NCube> getNCubesFromResource(ApplicationID appId, String name)
    {
        String lastSuccessful = ''
        try
        {
            Object[] cubes = getJsonObjectFromResource(name)
            List<NCube> cubeList = new ArrayList<>(cubes.length)

            for (Object cube : cubes)
            {
                JsonObject ncube = (JsonObject) cube
                String json = JsonWriter.objectToJson(ncube)
                NCube nCube = NCube.fromSimpleJson(json)
                nCube.applicationID = appId
                nCube.sha1()
                prepareCube(nCube, true)
                lastSuccessful = nCube.name
                cubeList.add(nCube)
            }

            return cubeList
        }
        catch (Exception e)
        {
            String s = "Failed to load cubes from resource: ${name}, last successful cube: ${lastSuccessful}"
            log.warn(s)
            throw new RuntimeException(s, e)
        }
    }

    //-- Validation ----------------------------------------------------------------------------------------------------

    protected static void validateCube(NCube cube)
    {
        if (cube == null)
        {
            throw new IllegalArgumentException('NCube cannot be null')
        }
        NCube.validateCubeName(cube.name)
    }
}
