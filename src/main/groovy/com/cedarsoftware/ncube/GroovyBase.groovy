package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.CdnClassLoader
import com.cedarsoftware.util.TimedSynchronize
import com.google.common.base.Joiner
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import ncube.grv.exp.NCubeGroovyExpression
import org.codehaus.groovy.control.CompilationUnit
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.Phases
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.tools.GroovyClass

import java.lang.reflect.Constructor
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Matcher

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.ncube.NCubeConstants.NCUBE_PARAMS_BYTE_CODE_DEBUG
import static com.cedarsoftware.ncube.NCubeConstants.NCUBE_PARAMS_BYTE_CODE_VERSION
import static com.cedarsoftware.util.EncryptionUtilities.calculateSHA1Hash
import static com.cedarsoftware.util.ReflectionUtils.getClassNameFromByteCode
import static com.cedarsoftware.util.StringUtilities.createUTF8String
import static com.cedarsoftware.util.StringUtilities.getUTF8Bytes
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.StringUtilities.isEmpty
import static com.cedarsoftware.util.UrlUtilities.getContentFromUrl

/**
 * Base class for Groovy CommandCells.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         Greg Morefield (morefigs@hotmail.com)
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
abstract class GroovyBase extends UrlCommandCell
{
    public static final String CLASS_NAME_FOR_L2_CALC = 'N_null'
    protected transient String L2CacheKey  // in-memory cache of (SHA-1(source) || SHA-1(URL + classpath.urls)) to compiled class
    protected transient String fullClassName  // full name of compiled class
    private volatile transient Class<NCubeGroovyExpression> runnableCode = null
    private Lock compileLock = new ReentrantLock()
    /**
     * This cache is 'per ApplicationID'.  This allows different applications to define the same
     * class (URL to groovy), yet have different source code for that class.
     */
    private static final ConcurrentMap<ApplicationID, ConcurrentMap<String, Class>> L2_CACHE = new ConcurrentHashMap<>()
    private static String generatedSourcesDir = ''

    //  Private constructor only for serialization.
    protected GroovyBase() {}

    GroovyBase(String cmd, String url = null, boolean cache = false)
    {
        super(cmd, url, cache)
    }

    void clearClassLoaderCache(ApplicationID appId)
    {
        super.clearClassLoaderCache(appId)
        runnableCode = null
    }

    /**
     * @return Class the compiled Class the code within this cell was compiled to (it could have been found
     * and re-used if the code is the same as another cell).  If the cell is marked cache=true, then this
     * method will return null. This is because the cell retains the cached value of the executed code and
     * therefore the class reference is no longer needed.
     */
    protected Class getRunnableCode()
    {
        return runnableCode
    }

    protected void setRunnableCode(Class runnableCode)
    {
        this.runnableCode = runnableCode
    }

    protected Object fetchResult(final Map<String, Object> ctx)
    {
        Class<NCubeGroovyExpression> runner = runnableCode;
        if (runner == null)
        {
            runner = prepare(cmd ?: url, ctx)
        }
        return executeInternal(ctx, runner)
    }

    protected abstract String buildGroovy(Map<String, Object> ctx, String className, String theirGroovy)

    protected static void clearCache(ApplicationID appId)
    {
        Map<String, Class> L2Cache = getAppL2Cache(appId)
        L2Cache.clear()
    }

    protected static Map<String, Class> getAppL2Cache(ApplicationID appId)
    {
        ConcurrentMap<String, Class> map = L2_CACHE.get(appId)

        if (map == null)
        {
            map = new ConcurrentHashMap<>()
            ConcurrentMap mapRef = L2_CACHE.putIfAbsent(appId, map)
            if (mapRef != null)
            {
                map = mapRef
            }
        }
        return map
    }

    Object executeInternal(final Map<String, Object> ctx, Class<NCubeGroovyExpression> code)
    {
        NCube ncube = getNCube(ctx)
        if (code == null)
        {
            throw new IllegalStateException("Code cleared while cell was executing, n-cube: ${ncube.name}, app: ${ncube.applicationID}, input: ${getInput(ctx).toString()}")
        }
        Constructor constructor = code.getDeclaredConstructor()
        NCubeGroovyExpression exp = constructor.newInstance()
        exp.input = getInput(ctx)
        exp.output = getOutput(ctx)
        exp.ncube = ncube
        return invokeRunMethod(exp)
    }

    /**
     * Fetch constructor (from cache, if cached) and instantiate GroovyExpression
     */
    protected abstract Object invokeRunMethod(NCubeGroovyExpression instance) throws Throwable

    /**
     * Conditionally compile the passed in command.  If it is already compiled, this method
     * immediately returns.  Insta-check because it is just a reference check.
     */
    Class<NCubeGroovyExpression> prepare(Object data, Map<String, Object> ctx)
    {
        TimedSynchronize.synchronize(compileLock, 250, TimeUnit.MILLISECONDS, 'Dead lock detected attempting to compile cell', 240)
        ClassLoader originalClassLoader = null

        try
        {
            // Double-check after lock obtained
            Class<NCubeGroovyExpression> runner = runnableCode
            if (runner != null)
            {   // Found in L1 (the member variable 'runnable')
                return runner
            }

            if (!L2CacheKey)
            {
                computeL2CacheKey(data, ctx)
            }
            Map<String, Class> L2Cache = getAppL2Cache(getNCube(ctx).applicationID)

            synchronized (L2CacheKey.intern())
            {
                // check L2 cache
                if (L2Cache.containsKey(L2CacheKey))
                {
                    // Already been compiled, re-use class (different cell, but has identical source or URL as other expression).
                    runner = L2Cache.get(L2CacheKey)
                    runnableCode = runner
                    return runner
                }

                // Pre-compiled check (e.g. source code was pre-compiled and instrumented for coverage)
                Map ret = getClassLoaderAndSource(ctx)
                if (ret.gclass instanceof Class)
                {   // Found class matching URL fileName.groovy already in JVM
                    runner = (Class)ret.gclass
                    L2Cache.put(L2CacheKey, runner)
                    runnableCode = runner
                    return runner
                }
                
                GroovyClassLoader gcLoader = ret.loader as GroovyClassLoader
                String groovySource = (String)ret.source

                // Internally, Groovy sometimes uses the Thread.currentThread().contextClassLoader, which is not the
                // correct class loader to use when inside a container.
                originalClassLoader = Thread.currentThread().contextClassLoader
                Thread.currentThread().contextClassLoader = gcLoader
                Class root = compile(gcLoader, groovySource, ctx)
                runnableCode = root
                L2Cache.put(L2CacheKey, root)
                return root
            }
        }
        finally
        {
            if (originalClassLoader != null)
            {
                Thread.currentThread().contextClassLoader = originalClassLoader
            }
            compileLock.unlock()
        }
    }
    
    /**
     * Ensure that the sys.classpath CdnClassLoader is used during compilation.  It has additional
     * classpath entries that the application developers likely have added.
     * @return Class the compile Class associated to the main class (root of source passed in)
     */
    protected Class compile(GroovyClassLoader gcLoader, String groovySource, Map<String, Object> ctx)
    {
        CompilerConfiguration compilerConfiguration = new CompilerConfiguration()
        compilerConfiguration.targetBytecode = targetByteCodeVersion
        compilerConfiguration.debug = NCubeCodeGenDebug
        compilerConfiguration.defaultScriptExtension = '.groovy'
        
        // TODO: Research when this can be safely turned on vs having to be turned off
//        compilerConfiguration.optimizationOptions = [(CompilerConfiguration.INVOKEDYNAMIC): Boolean.TRUE]

        // The source unit 'name' below must match what is present in GroovyExpression's 'bun' generator code.
        SourceUnit sourceUnit = new SourceUnit("ncube.N_${L2CacheKey}", groovySource, compilerConfiguration, gcLoader, null)

        CompilationUnit compilationUnit = new CompilationUnit(gcLoader)
        compilationUnit.addSource(sourceUnit)
        compilationUnit.configure(compilerConfiguration)
        if (gcLoader instanceof CdnClassLoader)
        {
            compilationUnit.classNodeResolver = (gcLoader as CdnClassLoader).classNodeResolver
        }
        compilationUnit.compile(Phases.CLASS_GENERATION)
        Map<String, Class> L2Cache = getAppL2Cache(getNCube(ctx).applicationID)
        Class generatedClass = defineClasses(gcLoader, compilationUnit.classes, L2Cache, groovySource)
        return generatedClass
    }

    protected Class defineClasses(GroovyClassLoader gcLoader, List classes, Map<String, Class> L2Cache, String groovySource)
    {
        int numClasses = classes.size()
        Class root = null
        byte[] mainClassBytes = null

        for (int i = 0; i < numClasses; i++)
        {
            GroovyClass gclass = (GroovyClass) classes.get(i)
            String className = gclass.name
            boolean isRoot = className.indexOf('$') == -1

            // Add compiled class to classLoader
            Class clazz = defineClass(gcLoader, url == null ? gclass.name : null, gclass.bytes)
            if (clazz == null)
            {   // error defining class - may have already been defined thru another route
                continue
            }

            // Persist class bytes
            if (className == fullClassName || (isRoot && root == null && NCubeGroovyExpression.isAssignableFrom(clazz)))
            {
                // return reference to main class
                root = clazz
                mainClassBytes = gclass.bytes

                if (url==null)
                {
                    dumpGeneratedSource(className,groovySource)
                }
            }
        }

        if (root == null)
        {
            if (hasContent(url))
            {
                throw new IllegalStateException("Unable to locate main compiled class: ${fullClassName} at url: ${url}.  Does it not extend NCubeGroovyExpression?")
            }
            else
            {
                throw new IllegalStateException("Unable to locate main compiled class. Does it not extend NCubeGroovyExpression? Source:\n${groovySource}")
            }
        }

        // Load root (main class)
        gcLoader.loadClass(getClassNameFromByteCode(mainClassBytes), false, true, true)
        return root
    }

    /**
     * Writes generated Groovy source to the directory identified by the NCUBE_PARAM:genSrcDir
     */
    private static void dumpGeneratedSource(String className, String groovySource) {
        String sourcesDir = generatedSourcesDirectory
        if (!sourcesDir) {
            return
        }

        File sourceFile = null
        try {
            sourceFile = new File("${sourcesDir}/${className.replace('.',File.separator)}.groovy")
            if (ensureDirectoryExists(sourceFile.parent)) {
                sourceFile.bytes = getUTF8Bytes(groovySource)
            }
        }
        catch (Exception e) {
            log.warn("Failed to write source file with path=${sourceFile.path}",e)
        }
    }

    /**
     * Returns directory to use for writing source files, if configured and valid
     * @return String specifying valid directory or empty string, if not configured or specified directory was not valid
     */
    static String getGeneratedSourcesDirectory()
    {
        return generatedSourcesDir
    }

    /**
     * Controls directory used to store generated sources
     *
     * @param sourcesDir String containing directory to log sources to:
     *      null - will attempt to use value configured in SystemParams
     *      empty - will disable logging
     *      valid directory - directory to use for generated sources
     *   NOTE: if directory cannot be validated, generated sources will be disabled
     */
    static void setGeneratedSourcesDirectory(String sourcesDir)
    {
        try
        {
            if (sourcesDir)
            {
                generatedSourcesDir = ensureDirectoryExists(sourcesDir) ? sourcesDir : ''
            }
            else
            {
                generatedSourcesDir = ''
            }

            if (generatedSourcesDir)
            {
                log.info("Generated sources configured to use path: ${generatedSourcesDir}")
            }
        }
        catch (Exception e)
        {
            log.warn("Unable to set sources directory to: ${sourcesDir}", e)
            generatedSourcesDir = ''
        }
    }

    /**
     * Validates directory existence
     * @param dirPath String path of directory to validate
     * @return true if directory exists; false, otherwise
     * @throws SecurityException if call to mkdirs encounters an issue
     */
    private static boolean ensureDirectoryExists(String dirPath) {
        if (!dirPath) {
            return false
        }

        File dir = new File(dirPath)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        boolean valid = dir.directory
        if (!valid)
        {
            log.warn("Failed to locate or create generated sources directory with path: ${dir.path}")
        }
        return valid
    }

    private static Class defineClass(GroovyClassLoader loader, String className, byte [] byteCode)
    {
        // Add compiled class to classLoader
        try
        {
            Class clazz = loader.defineClass(className, byteCode)
            return clazz
        }
        catch (ThreadDeath t)
        {
            throw t
        }
        catch (ClassCircularityError e)
        {
            log.warn("Attempting to defineClass() in GroovyBase", e)
            return null
        }
        catch (LinkageError ignored)
        {
            Class clazz = Class.forName(getClassNameFromByteCode(byteCode), false, loader)
            return clazz
        }
        catch (Throwable t)
        {
            if (byteCode != null)
            {
                log.warn("Unable to defineClass: ${getClassNameFromByteCode(byteCode)}", t)
            }
            else
            {
                log.warn("Unable to defineClass, null byte code", t)
            }
            return null
        }
    }

    protected Map getClassLoaderAndSource(Map<String, Object> ctx)
    {
        NCube cube = getNCube(ctx)
        boolean isUrlUsed = hasContent(url)
        Map ret = [:]

        if (cube.name.toLowerCase().startsWith("sys."))
        {   // No URLs allowed, nor code from sys.classpath when executing these cubes
            ret.loader = (GroovyClassLoader)ncubeRuntime.getLocalClassloader(cube.applicationID)
            ret.source = cmd
        }
        else if (isUrlUsed)
        {
            GroovyClassLoader gcLoader = getAppIdClassLoader(ctx)
            ret.loader = gcLoader

            if (url.endsWith('.groovy'))
            {
                // If a class exists already with the same name as the groovy file (substituting slashes for dots),
                // then attempt to find and return that class without going through the resource location and parsing
                // code. This can be useful, for example, if a build process pre-builds and load coverage enhanced
                // versions of the classes.
                String className = url - '.groovy'
                className = className.replace('/', '.')
                Class groovyClass = loadClass(className, gcLoader)
                if (groovyClass)
                {
                    ret.gclass = groovyClass
                    return ret
                }
            }

            URL groovySourceUrl = getActualUrl(ctx)
            ret.source = createUTF8String(getContentFromUrl(groovySourceUrl, true))
            if (isEmpty((String)ret.source))
            {
                throw new RuntimeException("Unable to fetch groovy source from url: ${groovySourceUrl}, app: ${cube.applicationID}, n-cube: ${cube.name}")
            }
        }
        else
        {   // inline code
            GroovyClassLoader gcLoader = getAppIdClassLoader(ctx)
            ret.loader = gcLoader

            if (Regexes.hasClassDefPattern.matcher(cmd).find())
            {
                ret.source = expandNCubeShortCuts(getCmd())  // TODO: Groovy error in IDE if accessor not used.
                return ret
            }
            else if (Regexes.grabPattern.matcher(cmd).find() || Regexes.grapePattern.matcher(cmd).find())
            {
                // force recompile
            }
            else
            {
                Class groovyClass = loadClass(fullClassName, gcLoader)
                if (groovyClass)
                {
                    ret.gclass = groovyClass
                    return ret
                }
            }

            ret.source = cmd
        }

        String className ="N_${L2CacheKey}"
        String source = getSourceFromCache(ctx, L2CacheKey)
        if (source)
        {
            ret.source = source.replace(CLASS_NAME_FOR_L2_CALC, className)
        }
        else
        {
            ret.source = expandNCubeShortCuts(buildGroovy(ctx, className, ret.source as String))
        }

        return ret
    }

    /**
     * Attempts to load Class and add it to output
     * @param className String containing fully qualified name of Class
     * @param output Map which provides 'loader' and will have 'gclass' added, if the Class is found
     * @return true, if the Class was added to output; otherwise, false
     */
    private static Class loadClass(String className, GroovyClassLoader loader)
    {
        try
        {
            Class loadedClass = loader.loadClass(className,false,true,true)
            if (NCubeGroovyExpression.class.isAssignableFrom(loadedClass))
            {
                log.trace("Loaded class: ${className}")
                return loadedClass
            }
        }
        catch (LinkageError error)
        {
            log.warn("Failed to load class :${className}. Will attempt to compile.",error)
        }
        catch (Exception ignored)
        { }

        return null
    }

    /**
     * Compute SHA1 hash for this CommandCell.  The tricky bit here is that the command can be either
     * defined inline or via a URL.  If defined inline, then the command hash is SHA1(command text).  If
     * defined through a URL, then the command hash is SHA1(command URL + GroovyClassLoader URLs.toString).
     * This is done this way so that when the URL is encountered, 1) the source does not have to be fetched, and 2)
     * to support the same URL (http://foo.com/code.groovy) actually having different source if the classpath
     * is different (sys.classpath allows different classpaths per scope). Adding the URLs from the class loader
     * to the URL in terms of SHA-1, makes the same URL, with a different sys.classpath, unique.
     */
    private void computeL2CacheKey(Object data, Map<String, Object> ctx)
    {
        if (url == null)
        {   // inline statement block (GroovyExpression)
            String content = expandNCubeShortCuts(buildGroovy(ctx, CLASS_NAME_FOR_L2_CALC, (data != null ? data.toString() : "")))
            L2CacheKey = calculateSHA1Hash(getUTF8Bytes(content))
            String packageName = null
            String className = null

            Matcher m = Regexes.hasClassDefPattern.matcher(content)
            if (m.find())
            {
                packageName = m.group('packageName')
                className = m.group('className')
            }

            if (className == CLASS_NAME_FOR_L2_CALC || className == null)
            {
                className = "N_${L2CacheKey}"
            }
            fullClassName = packageName==null ? className : "${packageName}.${className}"
            addSourceToCache(ctx, L2CacheKey, content)
        }
        else
        {   // specified via URL, add classLoader URL strings to URL for SHA-1 source.
            GroovyClassLoader gcLoader = getAppIdClassLoader(ctx)
            URL[] urls = gcLoader.URLs
            String content = "${Joiner.on('|').join(urls)}.${url}"
            fullClassName = url - '.groovy'
            fullClassName = fullClassName.replace('/', '.')
            L2CacheKey = calculateSHA1Hash(getUTF8Bytes(content))
        }
    }

    private static GroovyClassLoader getAppIdClassLoader(Map<String, Object> ctx)
    {
        NCube cube = getNCube(ctx)
        ApplicationID appId = cube.applicationID
        GroovyClassLoader gcLoader = (GroovyClassLoader) ncubeRuntime.getUrlClassLoader(appId, getInput(ctx))
        return gcLoader
    }

    private static String getTargetByteCodeVersion()
    {
        return ncubeRuntime.systemParams.get(NCUBE_PARAMS_BYTE_CODE_VERSION) ?: '1.8'
    }

    private static boolean isNCubeCodeGenDebug()
    {
        return 'true'.equalsIgnoreCase((String) ncubeRuntime.systemParams.get(NCUBE_PARAMS_BYTE_CODE_DEBUG))
    }

    protected static String expandNCubeShortCuts(String groovy)
    {
        Matcher m = Regexes.groovyAbsRefCubeCellPattern.matcher(groovy)
        String exp = m.replaceAll('$1go(${input}, \'${cubeName}\')')

        m = Regexes.groovyAbsRefCubeCellPatternA.matcher(exp)
        exp = m.replaceAll('$1go(${input}, \'${cubeName}\')')

        m = Regexes.groovyAbsRefCellPattern.matcher(exp)
        exp = m.replaceAll('$1go(${input})')

        m = Regexes.groovyAbsRefCellPatternA.matcher(exp)
        exp = m.replaceAll('$1go(${input})')

        m = Regexes.groovyRelRefCubeCellPattern.matcher(exp)
        exp = m.replaceAll('$1at(${input},\'${cubeName}\')')

        m = Regexes.groovyRelRefCubeCellPatternA.matcher(exp)
        exp = m.replaceAll('$1at(${input}, \'${cubeName}\')')

        m = Regexes.groovyRelRefCellPattern.matcher(exp)
        exp = m.replaceAll('$1at(${input})')

        m = Regexes.groovyRelRefCellPatternA.matcher(exp)
        exp = m.replaceAll('$1at(${input})')
        return exp
    }

    void getCubeNamesFromCommandText(final Set<String> cubeNames)
    {
        getCubeNamesFromText(cubeNames, cmd)
    }

    protected static String getSourceFromCache(Map<String, Object> ctx, String cacheKey)
    {
        return (String)ctx.get(cacheKey)
    }

    protected static void addSourceToCache(Map<String, Object> ctx, String cacheKey, String source)
    {
        ctx.put(cacheKey, source)
    }

    protected static void getCubeNamesFromText(final Set<String> cubeNames, final String text)
    {
        if (isEmpty(text))
        {
            return
        }

        Matcher m = Regexes.groovyAbsRefCubeCellPattern.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyAbsRefCubeCellPatternA.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyRelRefCubeCellPattern.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyRelRefCubeCellPatternA.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyExplicitCubeRefPattern.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyExplicitJumpPattern.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyExplicitAtPattern.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyExplicitGoPattern.matcher(text)
        getCubeNames(m, cubeNames)

        m = Regexes.groovyExplicitUsePattern.matcher(text)
        getCubeNames(m, cubeNames)
    }

    private static void getCubeNames(Matcher m, Set<String> cubeNames)
    {
        while (m.find())
        {
            cubeNames.add(m.group('cubeName'))  // based on Regex pattern - if pattern changes, this could change
        }
    }

    /**
     * Find all occurrences of 'input.variableName' in the Groovy code
     * and add the variableName as a scope (key).
     * @param scopeKeys Set to add required scope keys to.
     */
    void getScopeKeys(Set<String> scopeKeys)
    {
        Matcher m = Regexes.inputVar.matcher(cmd)
        while (m.find())
        {
            scopeKeys.add(m.group(2))
        }
    }

    static String extractImportsAndAnnotations(String text, Set<String> extractedLines)
    {
        String adjusted = extract(Regexes.importPattern.matcher(text), extractedLines)
        adjusted = extract(Regexes.grapePattern.matcher(adjusted), extractedLines)
        adjusted = extract(Regexes.grabPattern.matcher(adjusted), extractedLines)
        adjusted = extract(Regexes.compileStaticPattern.matcher(adjusted), extractedLines)
        adjusted = extract(Regexes.typeCheckPattern.matcher(adjusted), extractedLines)
        return adjusted
    }

    static String extract(Matcher matcher, Set<String> extractedLines)
    {
        while (matcher.find())
        {
            extractedLines.add(matcher.group(0))  // based on Regex pattern - if pattern changes, this could change
        }
        matcher.reset()
        return matcher.replaceAll('')
    }
}
