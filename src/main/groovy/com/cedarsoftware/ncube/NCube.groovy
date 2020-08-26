package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CommandCellException
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.exception.InvalidCoordinateException
import com.cedarsoftware.ncube.exception.RuleJump
import com.cedarsoftware.ncube.exception.RuleStop
import com.cedarsoftware.ncube.formatters.HtmlFormatter
import com.cedarsoftware.ncube.formatters.JsonFormatter
import com.cedarsoftware.ncube.formatters.NCubeTestReader
import com.cedarsoftware.ncube.formatters.NCubeTestWriter
import com.cedarsoftware.ncube.util.CellMap
import com.cedarsoftware.util.AdjustableGZIPOutputStream
import com.cedarsoftware.util.ByteUtilities
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCIHashMap
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.CompactLinkedMap
import com.cedarsoftware.util.LongHashSet
import com.cedarsoftware.util.MapUtilities
import com.cedarsoftware.util.MathUtil
import com.cedarsoftware.util.TrackingMap
import com.cedarsoftware.util.io.JsonObject
import com.cedarsoftware.util.io.JsonWriter
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.springframework.util.FastByteArrayOutputStream

import java.lang.reflect.Array
import java.lang.reflect.Field
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.regex.Matcher
import java.util.zip.Deflater
import java.util.zip.GZIPInputStream

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.ncube.NCubeConstants.DECISION_TABLE
import static com.cedarsoftware.util.Converter.convertToInteger
import static com.cedarsoftware.util.Converter.convertToLong
import static com.cedarsoftware.util.EncryptionUtilities.SHA1Digest
import static com.cedarsoftware.util.EncryptionUtilities.calculateSHA1Hash
import static com.cedarsoftware.util.ExceptionUtilities.getDeepestException
import static com.cedarsoftware.util.IOUtilities.close
import static com.cedarsoftware.util.ReflectionUtils.getDeepDeclaredFields
import static com.cedarsoftware.util.StringUtilities.encode
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.StringUtilities.isEmpty
import static com.cedarsoftware.util.io.MetaUtils.isLogicalPrimitive

/**
 * Implements an n-cube.  This is a hyper (n-dimensional) cube
 * of cells, made up of 'n' number of axes.  Each Axis is composed
 * of Columns that denote discrete nodes along an axis.  Use NCubeManager
 * to manage a list of NCubes.  Documentation on Github.
 *
 * Useful for pricing, rating, and configuration modeling.
 *
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
class NCube<T>
{
    public static final String DEFAULT_CELL_VALUE_TYPE = 'defaultCellValueType'
    public static final String DEFAULT_CELL_VALUE = 'defaultCellValue'
    public static final String DEFAULT_CELL_VALUE_URL = 'defaultCellValueUrl'
    public static final String DEFAULT_CELL_VALUE_CACHE = 'defaultCellValueCache'
    public static final String validCubeNameChars = '0-9a-zA-Z._-'
    public static final String RULE_EXEC_INFO = '_rule'
    public static final String METAPROPERTY_TEST_UPDATED = 'testUpdated'
    public static final String METAPROPERTY_TEST_DATA = '_testData'
    public static final String MAP_REDUCE_COLUMNS_TO_SEARCH = 'columnsToSearch'
    public static final String MAP_REDUCE_COLUMNS_TO_RETURN = 'columnsToReturn'
    public static final String MAP_REDUCE_SHOULD_EXECUTE = 'shouldExecute'
    public static final String MAP_REDUCE_DEFAULT_VALUE = 'defaultValue'
    protected static final byte[] TRUE_BYTES = 't'.bytes
    protected static final byte[] FALSE_BYTES = 'f'.bytes
    private static final byte[] A_BYTES = 'a'.bytes
    private static final byte[] C_BYTES = 'c'.bytes
    private static final byte[] O_BYTES = 'o'.bytes
    private static final byte[] NULL_BYTES = 'null'.bytes
    private static final byte[] ARRAY_BYTES = 'array'.bytes
    private static final byte[] MAP_BYTES = 'map'.bytes
    private static final byte[] COL_BYTES = 'col'.bytes

    private String name
    private String sha1
    private final Map<String, Axis> axisList = new CompactCILinkedMap<>()
    private final Map<Long, Axis> idToAxis = new Long2ObjectOpenHashMap<>()
    protected final Map<Set<Long>, T> cells = new CellMap<T>()
    private T defaultCellValue
    private final Map<String, Advice> advices = [:]
    private Map<String, Object> metaProps = new CompactCILinkedMap<>()
    private static ConcurrentMap primitives = new ConcurrentHashMap()
    //  Sets up the defaultApplicationId for cubes loaded in from disk.
    private transient ApplicationID appId = ApplicationID.testAppId
    private transient Object engine = null
    private static final ThreadLocal<Deque<StackEntry>> executionStack = new ThreadLocal<Deque<StackEntry>>() {
        Deque<StackEntry> initialValue()
        {
            return new ArrayDeque<>()
        }
    }
    private static int stackEntryCoordinateValueMaxSize
    
    /**
     * Creata a new NCube instance with the passed in name
     * @param name String name to use for the NCube.
     */
    NCube(String name)
    {
        if (name != null)
        {   // If name is null, likely being instantiated via serialization
            validateCubeName(name)
        }
        this.name = name
    }

    /**
     * @return reference to associated business engine (RulesEngine, DecisionTable, TruthTable, ...)
     */
    Object getBusinessEngine()
    {
        return engine
    }

    /**
     * Associate business engine (RulesEngine, DecisionTable, TruthTable, ...) to this NCube.
     * @param businessEngine Object engine to associate.
     */
    void setBusinessEngine(Object businessEngine)
    {
        engine = businessEngine
    }

    /**
     * Fetch n-cube meta properties (SHA1, HEAD_SHA1, and CHANGE_TYPE are not meta-properties.
     * Use their respective accessor functions to obtain those).
     * @return Map (case insensitive keys) containing meta (additional) properties for the n-cube.
     * Modifications to this Map do not modify the actual meta properties of n-cube.  To do that,
     * you need to use setMetaProperty(), addMetaProperty(), or remoteMetaProperty()
     */
    Map<String, Object> getMetaProperties()
    {
        return Collections.unmodifiableMap(metaProps)
    }

    /**
     * Fetch the value associated to the passed in Key from the MetaProperties (if any exist).  If
     * none exist, null is returned.
     */
    Object getMetaProperty(String key)
    {
        return metaProps.get(key)
    }

    /**
     * Test for existence of a given meta-property key.
     * @param key String name of key
     * @return boolean true if the passed in meta-property key exists, false otherwise.
     */
    boolean containsMetaProperty(String key)
    {
        return metaProps.containsKey(key)
    }
    
    /**
     * If a meta property value is fetched from an Axis or a Column, the value should be extracted
     * using this API, so as to allow executable values to be retrieved.
     * @param value Object value to be extracted.
     */
    Object extractMetaPropertyValue(Object value, Map input = [:], Map output = [:])
    {
        if (value instanceof CommandCell)
        {
            if (!(input instanceof TrackingMap))
            {
                input = new TrackingMap(input)
            }
            CommandCell cmd = (CommandCell) value
            value = executeExpression(prepareExecutionContext(input, output), cmd)
        }
        return value
    }

    /**
     * Set (add / overwrite) a Meta Property associated to this n-cube.
     * @param key String key name of meta property
     * @param value Object value to associate to key
     * @return prior value associated to key or null if none was associated prior
     */
    Object setMetaProperty(String key, Object value)
    {
        clearSha1()
        return metaProps.put(key, value)
    }

    /**
     * Remove a meta-property entry
     */
    Object removeMetaProperty(String key)
    {
        Object prop =  metaProps.remove(key)
        clearSha1()
        return prop
    }

    /**
     * Add a Map of meta properties all at once.
     * @param allAtOnce Map of meta properties to add
     */
    void addMetaProperties(Map<String, Object> allAtOnce)
    {
        for (entry in allAtOnce.entrySet())
        {
            final String key = entry.key
            metaProps.put(key, entry.value)
        }
        clearSha1()
    }

    /**
     * Remove all meta properties associated to this n-cube.
     */
    void clearMetaProperties()
    {
        metaProps.clear()
        clearSha1()
    }

    /**
     * Walk cell map and ensure all coordinates are fully resolvable
     */
    protected void dropOrphans(Set<Long> columnIds, long axisId)
    {
        Iterator<Set<Long>> i = cells.keySet().iterator()
        while (i.hasNext())
        {
            Set<Long> cols = i.next()
            for (id in cols)
            {
                Axis axis = getAxisFromColumnId(id, false)
                if (axis && axis.id == axisId)
                {
                    if (!columnIds.contains(id))
                    {
                        i.remove()
                        break
                    }
                }
            }
        }
    }

    /**
     * This is a "Pointer" (or Key) to a cell in an NCube.
     * It consists of a String cube Name and a Set of
     * Column references (one Column per axis).
     */
    private static class StackEntry
    {
        final String cubeName
        final Map coord

        StackEntry(String name, Map coordinate)
        {
            cubeName = name
            coord = coordinate
        }

        String toString()
        {
            StringBuilder s = new StringBuilder()
            s.append("${cubeName}:[")
            Iterator<Map.Entry> i = coord.entrySet().iterator()
            
            while (i.hasNext())
            {
                Map.Entry<String, Object> coordinate = i.next()
                String value = coordinate.value.toString()
                if (value.size() > stackEntryCoordinateValueMaxSize)
                {
                    value = "${value[0..(stackEntryCoordinateValueMaxSize - 1)]}..."
                }
                s.append("${coordinate.key}:${value}")
                if (i.hasNext())
                {
                    s.append(',')
                }
            }
            s.append(']')
            return s.toString()
        }
    }

    /**
     * Add advice to this n-cube that will be called before / after any Controller Method or
     * URL-based Expression, for the given method
     */
    protected void addAdvice(Advice advice, String method)
    {
        advices.put("${advice.name}/${method}".toString(), advice)
    }

    /**
     * @return List<Advice> advices added to this n-cube.
     */
    List<Advice> getAdvices(String method)
    {
        List<Advice> result = []
        if (advices.isEmpty())
        {
            return result
        }
        method = "/${method}"
        for (entry in advices.entrySet())
        {
            // Entry key = "AdviceName/MethodName"
            if (entry.key.endsWith(method))
            {   // Entry.Value = Advice instance
                result.add(entry.value)
            }
        }

        if (!result.empty)
        {
            Collections.sort(result, new Comparator<Advice>() {
                int compare(Advice a1, Advice a2)
                {
                    return a1.name.compareToIgnoreCase(a2.name)
                }
            })
        }

        return result
    }

    /**
     * For testing, advices need to be removed after test completes.
     */
    protected void clearAdvices()
    {
        advices.clear()
    }

    /**
     * @return ReleaseStatus of this n-cube as it was loaded.
     */
    String getStatus()
    {
        return appId.status
    }

    /**
     * @return String version of this n-cube as it was loaded.
     */
    String getVersion()
    {
        return appId.version
    }

    /**
     * @return ApplicationID for this n-cube.  This contains the app name, version, etc. that this
     * n-cube is part of.
     */
    ApplicationID getApplicationID()
    {
        return appId
    }

    void setApplicationID(ApplicationID appId)
    {
        this.appId = appId
    }

    /**
     * This should only be called from NCubeManager when loading the cube from a database
     * It is mainly to prevent an unnecessary sha1 calculation after being loaded from a
     * db that already knows the sha1.
     * @param sha1 String SHA-1 value to set into this n-cube.  Should only be called internally
     * from code constructing this n-cube from a persistent store.
     */
    protected void setSha1(String sha1)
    {
        this.sha1 = sha1
    }

    /**
     * @return String name of the NCube
     */
    String getName()
    {
        return name
    }

    /**
     * Clear (remove) the cell at the given coordinate.  The cell is dropped
     * from the internal sparse storage. After this call, containsCell(coord) for the
     * same cell will return false.
     * @param coordinate Map coordinate of Cell to remove.
     * @return value of cell that was removed.
     * For RULE axes, the name of the Rule Axis must be bound to a rule name (e.g. the 'name'
     * attribute on the Column expression).  If you need to distinguish between a null
     * stored in a null, versus nothing being stored there, use containsCell() first.
     */
    T removeCell(final Map coordinate)
    {
        clearSha1()
        return cells.remove(getCoordinateKey(coordinate))
    }

    /**
     * Clear a cell directly from the cell sparse-matrix specified by the passed in Column
     * IDs. After this call, containsCell(coord) for the same coordinate would return false.
     */
    T removeCellById(final Set<Long> coordinate)
    {
        clearSha1()
        Set<Long> ids = ensureFullCoordinate(coordinate)
        if (ids == null)
        {
            return null
        }
        return cells.remove(ids)
    }

    /**
     * Test to see if a value is mapped at the given coordinate.  The 2nd argument allows
     * you to take into account the n-cube level Default Cell value or not. Set to true
     * to have the default value considered, false otherwise.
     * @param coordinate Map (coordinate) of a cell
     * @param useDefault (optional, defaults to false. Set to true, then if a non-default
     * value for the n-cube is set, this method will return true).
     * @return 1. boolean true if a defaultValue is set (non-null) and useDefault is true
     * 2. boolean true if a cell is located at the specified coordinate in the
     * sparse cell map.
     * For RULE axes, the name of the Rule Axis must be bound to a rule name
     * (e.g. the 'name' attribute on the Column expression).
     */
    boolean containsCell(final Map coordinate, boolean useDefault = false)
    {
        Set<Long> cols
        if (useDefault)
        {
            if (defaultCellValue != null)
            {   // n-cube default not-null, so yes it 'contains cell' (when useDefault true)
                return true
            }
            cols = getCoordinateKey(coordinate)
            if (getColumnDefault(cols) != null)
            {   // n-cube column default not-null, so yes it 'contains cell' (when useDefault true)
                return true
            }
        }
        else
        {
            cols = getCoordinateKey(coordinate)
        }

        return cells.containsKey(cols)
    }


    /**
     * @return true if and only if there is a cell stored at the location
     * specified by the Set<Long> coordinate.  If the IDs don't locate a coordinate,
     * no exception is thrown - simply false is returned.
     * If no coordinate is supplied for an axis (or axes) that has a default column, then the default
     * column will be bound to for that axis (or axes).
     */
    boolean containsCellById(final Set<Long> coordinate)
    {
        Set<Long> ids = ensureFullCoordinate(coordinate)
        return cells.containsKey(ids)
    }

    /**
     * Store a value in the cell at the passed in coordinate.
     * @param value A value to store in the NCube cell.
     * @param coordinate Map coordinate used to identify what cell to update.
     * The Map contains keys that are axis names, and values that will
     * locate to the nearest column on the axis.
     * @return the prior cells value.
     */
    T setCell(final T value, final Map coordinate)
    {
        if (!(value instanceof byte[]) && value != null && value.class.array)
        {
            throw new IllegalArgumentException("Cannot set a cell to be an array type directly (except byte[]). Instead use GroovyExpression.")
        }
        clearSha1()
        return cells.put(getCoordinateKey(coordinate), (T) internValue(value))

    }

    /**
     * Set a cell directly into the cell sparse-matrix specified by the passed in
     * Column IDs.
     */
    T setCellById(final T value, final Set<Long> coordinate, boolean skipEnsureCheck = false)
    {
        if (!(value instanceof byte[]) && value != null && value.class.array)
        {
            throw new IllegalArgumentException("Cannot set a cell to be an array type directly (except byte[]). Instead use GroovyExpression.")
        }
        clearSha1()
        if (skipEnsureCheck)
        {
            return cells.put(coordinate, (T)internValue(value))
        }
        Set<Long> ids = ensureFullCoordinate(coordinate)
        if (ids == null)
        {
            throw new InvalidCoordinateException("Unable to setCellById() into n-cube: ${name}, appId: ${appId} using coordinate: ${coordinate}. Add column(s) before assigning cells.", name)
        }
        return cells.put(ids, (T)internValue(value))
    }

    /**
     * Mainly useful for displaying an ncube within an editor.  This will
     * get the actual stored cell, not execute it.  The caller will get
     * CommandCell instances for example, as opposed to the return value
     * of the executed CommandCell.
     */
    def getCellByIdNoExecute(final Set<Long> coordinate)
    {
        Set<Long> ids = ensureFullCoordinate(coordinate)
        return cells.get(ids)
    }

    /**
     * Fetch the actual 'formula' at the given cell.  In the case of primitives,
     * the primitive will be returned.  However, in the case of an Expression,
     * the Expression will be returned, not executed.
     * @return value stored at the given location.  Since this method will return
     * null when there is no value in an empty (unset) cell, use containsCell() if
     * you need to distinguish between not present and null.
     * @throws CoordinateNotFoundException if the coordinate does not represent a
     * coordinate with the space of this n-cube.
     */
    def getCellNoExecute(final Map coordinate)
    {
        Set<Long> ids = getCoordinateKey(coordinate)
        return cells.get(ids)
    }

    /**
     * Fetch the contents of the cell at the location specified by the coordinate argument.
     * Be aware that if you have any rule cubes in the execution path, they can execute
     * more than one cell.  The cell value returned is the value of the last cell executed.
     * Typically, in a rule cube, you are writing to specific keys within the rule cube, and
     * the calling code then accesses the 'output' Map to fetch the values at these specific
     * keys.
     * @param coordinate Map of String keys to values meant to bind to each axis of the n-cube.
     * @param output Map that can be written to by the code within the the n-cubes (for example,
     *               GroovyExpressions.
     * @param defaultValue Object placed here will be returned if there is no cell at the location
     *                     pinpointed by the input coordinate.  Normally, the defaulValue of the
     *                     n-cube is returned, but if this parameter is passed a non-null value,
     *                     then it will be returned.
     * @return Cell pinpointed by the input coordinate.  If there is nothing stored at this
     * location, then if there is an axis containing a column with a default value (set as
     * meta-property Column.DEFAULT_VALUE [key: 'default_value']), then that will be returned.
     * If there is no column with a default value, then the n-cube's default value will be
     * returned. If defaultValue is null, then then n-cube defaultValue argument will be returned.
     */
    T at(final Map coordinate, final Map output = [:], Object defaultValue = null)
    {
        return getCell(coordinate, output, defaultValue)
    }

    /**
     * Grab the cell located at altInput, then run it in terms of the input.
     */
    T use(Map altInput, Map input, Map output, def defaultCellValue)
    {
        Map safeCoord = wrapCoordinate(validateCoordinate(altInput, output))
        T value = getCellById(getCoordinateKey(safeCoord, output), input, output, defaultCellValue)
        RuleInfo info = getRuleInfo(output)
        info.setLastExecutedStatement(value)
        output.return = value
        return value
    }

    /**
     * <pre>Fetch the contents of the cell at the location specified by the coordinate argument.
     *
     * Note that if you have any rule axes, they can execute more than one time.  The value
     * returned by this method is the value of the last cell executed. Typically, in a rule cube,
     * you are writing to specific keys within the output Map, and the calling code then accesses
     * the 'output' Map to fetch the values at these specific keys.
     *
     * A rule axis name can have a String, Collection, Map, or nothing associated to it.
     *   - If the value is a String, then it is the name of the rule to begin execution
     *     (skips past rules ahead of it).
     *   - If the value associated to a rule axis name is a Collection, then it is considered a
     *     Collection of rule names to run (orchestration).  In that case, only the named rules
     *     will be executed (their conditions evaluated, and if true, the associated statements).
     *   - If the associated value is a Map, then the keys will be the names of meta-property keys
     *     associated to the rule condition column meta-property keys, and the values must match
     *     the value associated to the meta-property. The special value NCUBE.DONT_CARE can be
     *     associated to the key, in which case only the key name of the meta-property must match
     *     the key name in the passed in map in order for the rule to be selected.  If there
     *     is more than one entry in the passed in Map, then the rules must match on all entries.
     *     This is a conjunction (or AND) - the rules match all keys are selected.
     *   - If nothing is associated to the rule axis name (or null), then all rules are selected.
     *
     * Once the rules are selected, all rule conditions are executed.  If the condition is true,
     * then the associated statement is executed.
     *
     * Note: More than one rule axis can be added to an n-cube.  In this case, each rule axis
     * name can have its own orchestration (rule list) to select the rules on the given axis.
     * </pre>
     * @param coordinate Map of String keys to values meant to bind to each axis of the n-cube.
     * @param output Map that can be written to by the code within the the n-cubes (for example,
     *               GroovyExpressions.
     * @param defaultValue Object placed here will be returned if there is no cell at the location
     *                     pinpointed by the input coordinate.  Normally, the defaulValue of the
     *                     n-cube is returned, but if this parameter is passed a non-null value,
     *                     then it will be returned.
     * @return Cell pinpointed by the input coordinate.  If there is nothing stored at this
     * location, then if there is an axis containing a column with a default value (set as
     * meta-property Column.DEFAULT_VALUE [key: 'default_value']), then that will be returned.
     * If there is no column with a default value, then the n-cube's default value will be
     * returned. If defaultValue is null, then then n-cube defaultValue argument will be returned.
     */
    T getCell(final Map coordinate, final Map output = [:], Object defaultValue = null)
    {
        Map input = wrapCoordinate(validateCoordinate(coordinate, output))

        if (hasRuleAxis())
        {
            return runRules(coordinate, input, output, defaultValue)
        }
        else
        {   // Perform fast bind and execute.
            T lastStatementValue = getCellById(getCoordinateKey(input, output), input, output, defaultValue)
            final RuleInfo ruleInfo = getRuleInfo(output)
            ruleInfo.setLastExecutedStatement(lastStatementValue)
            return output.return = lastStatementValue
        }
    }

    private T runRules(final Map coordinate, Map input, final Map output = [:], Object defaultValue = null)
    {
        final RuleInfo ruleInfo = getRuleInfo(output)
        final boolean trackBindings = ncubeRuntime?.trackBindingsOn
        boolean run = true
        final List<Binding> bindings = ruleInfo.getAxisBindings()
        final int depth = executionStack.get().size()
        final int dimensions = numDimensions
        final String[] axisNames = axisList.keySet().toArray(new String[dimensions])
        Map ctx = prepareExecutionContext(input, output)
        T lastStatementValue = null

        while (run)
        {
            run = false
            final Map<String, List<Column>> selectedColumns = selectColumns(input, output)   // get [potential subset of] rule columns to execute, per Axis
            final Map<String, Integer> counters = getCountersPerAxis(axisNames)
            final Map<Long, Object> cachedConditionValues = [:]
            final Map<String, Integer> conditionsFiredCountPerAxis = [:]

            try
            {
                while (true)
                {
                    final Binding binding = new Binding(name, depth)

                    for (axis in axisList.values())
                    {
                        final String axisName = axis.name
                        final Column boundColumn = selectedColumns.get(axisName).get(counters[axisName] - 1)

                        if (axis.type == AxisType.RULE)
                        {   // Place value bound to rule axis name on input (that was bound to the input[axisName]).
                            // into a Wrapper class that acts like a String for rebinding to the rule axis, for nested
                            // use() calls, but contains a pointer to the original value (and delegates calls to it).
                            input.put(axisName, new GStringWrapper(boundColumn.columnName, input.get(axisName)))
                            Object conditionValue
                            
                            if (!cachedConditionValues.containsKey(boundColumn.id))
                            {   // Has the condition on the Rule axis been run this execution?  If not, run it and cache it.
                                CommandCell cmd = (CommandCell) boundColumn.value

                                // If the cmd == null, then we are looking at a default column on a rule axis.
                                // the conditionValue becomes 'true' for Default column when ruleAxisBindCount = 0
                                final Integer count = conditionsFiredCountPerAxis.get(axisName)
                                conditionValue = cmd == null ? isZero(count) : executeExpression(ctx, cmd)
                                cachedConditionValues.put(boundColumn.id, conditionValue as boolean)

                                if (conditionValue)
                                {   // Rule fired
                                    conditionsFiredCountPerAxis.put(axisName, count == null ? 1 : count + 1)
                                    if (!axis.fireAll)
                                    {   // Only fire one condition on this axis (fireAll is false)
                                        counters.put(axisName, 1)
                                        selectedColumns.put(axisName, [boundColumn])
                                    }
                                    if (cmd == null)
                                    {
                                        trackUnboundAxis(output, name, axisName, coordinate.get(axisName))
                                    }
                                }
                            }
                            else
                            {   // re-use condition on this rule axis (happens when more than one rule axis on an n-cube)
                                conditionValue = cachedConditionValues.get(boundColumn.id)
                            }

                            // A rule column on a given axis can be accessed more than once (example: A, B, C on
                            // one rule axis, X, Y, Z on another).  This generates coordinate combinations
                            // (AX, AY, AZ, BX, BY, BZ, CX, CY, CZ).  The condition columns must be run only once, on
                            // subsequent access, the cached result of the condition is used.
                            if (conditionValue)
                            {
                                binding.bind(axisName, boundColumn)
                            }
                            else
                            {   // Incomplete binding - no need to attempt further bindings on other axes.
                                break
                            }
                        }
                        else
                        {
                            binding.bind(axisName, boundColumn)
                        }
                    }

                    // Step #2 Execute cell and store return value, associating it to the Axes and Columns it bound to
                    if (binding.numBoundAxes == dimensions)
                    {   // Conditions on rule axes that do not evaluate to true, do not generate complete coordinates (intentionally skipped)
                        if (trackBindings)
                        {   // Add bindings to output map when running ncube.track.bindings = true (default)
                            bindings.add(binding)
                        }
                        lastStatementValue = executeAssociatedStatement(input, output, ruleInfo, binding)
                    }
                    else
                    {
                        restoreWrappedValues(input)
                    }

                    // Step #3 increment counters (variable radix increment)
                    if (!incrementVariableRadixCount(counters, selectedColumns, axisNames))
                    {
                        break
                    }
                }

                // Verify all rule axes were bound 1 or more times
                ensureAllRuleAxesBound(coordinate, conditionsFiredCountPerAxis)
            }
            catch (RuleStop ignored)
            {   // ends this execution cycle
                ruleInfo.ruleStopThrown()
            }
            catch (RuleJump e)
            {
                input = e.coord
                run = true
            }
        }

        ruleInfo.setLastExecutedStatement(lastStatementValue)
        output.return = lastStatementValue
        return lastStatementValue
    }

    private Object executeExpression(Map ctx, CommandCell cmd)
    {
        try
        {
            Object ret = cmd.execute(ctx)
            trackInputKeysUsed((Map) ctx.input, (Map) ctx.output)
            return ret
        }
        catch (ThreadDeath | RuleStop | RuleJump e)
        {
            throw e
        }
        catch (CoordinateNotFoundException e)
        {
            String msg = e.message
            if (!msg.contains('-> cell:'))
            {
                throw new CoordinateNotFoundException("${e.message}\nerror occurred in cube: ${name}\n${stackToString()}",
                        e.cubeName, e.coordinate, e.axisName, e.value)
            }
            else
            {
                throw e
            }
        }
        catch (CommandCellException e)
        {   // Throw the inner-most CommandCellException to the top
            throw e
        }
        catch (Throwable t)
        {
            throw new CommandCellException("Error occurred in cube: ${name}\n${stackToString()}", t)
        }
    }

    private T executeAssociatedStatement(Map input, Map output, RuleInfo ruleInfo, Binding binding)
    {
        try
        {
            final Set<Long> colIds = binding.idCoordinate
            T statementValue = getCellById(colIds, input, output)
            binding.value = statementValue
            return statementValue
        }
        catch (RuleStop e)
        {   // Statement threw at RuleStop
            binding.value = '[RuleStop]'
            // Mark that RULE_STOP occurred
            ruleInfo.ruleStopThrown()
            throw e
        }
        catch(RuleJump e)
        {   // Statement threw at RuleJump
            binding.value = '[RuleJump]'
            throw e
        }
        catch (Exception e)
        {
            Throwable t = e
            while (t.cause != null)
            {
                t = t.cause
            }
            String msg = t.message
            if (isEmpty(msg))
            {
                msg = t.class.name
            }
            binding.value = "[${msg}]".toString()
            throw e
        }
        finally
        {
            // Replace value bound to rule axis name on input (that was bound to the input[axisName]). The replaced
            // value acts like a String for rebinding to the rule axis (nested use() calls), but contains a pointer
            // to the original value (and delegates calls to it).
            restoreWrappedValues(input)
        }
    }

    /**
     * Restore temporarily bound rule condition name (that was bound to the input[axisName]).  The rule name
     * is bound right before executing the associated statement, so that a call back into the rule cube will
     * start on the same rule.  After execution, the rule name's original associated value is restored.
     * @param input Map<String, ?> typical coordinate input
     */
    private void restoreWrappedValues(Map<String, Object> input)
    {
        for (Map.Entry<String, Object> entry: input.entrySet())
        {
            if (entry.value instanceof GStringWrapper)
            {   // Replace any value that was wrapped with the original value.
                def value = ((GStringWrapper) entry.value).originalValue
                entry.value = value
            }
        }
    }

    /**
     * @return boolean true if there is at least one rule axis, false if there are no rule axes.
     */
    boolean hasRuleAxis()
    {
        for (axis in axisList.values())
        {
            if (AxisType.RULE == axis.type)
            {
                return true
            }
        }
        return false
    }

    /**
     * Verify that at least one rule on each rule axis fired.  If not, then you have a
     * CoordinateNotFoundException.
     * @param coordinate Input (Map) coordinate for getCell()
     * @param conditionsFiredCountPerAxis Map that tracks AxisName to number of fired-columns bound to axis
     */
    private void ensureAllRuleAxesBound(Map coordinate, Map<String, Integer> conditionsFiredCountPerAxis)
    {
        for (axis in axisList.values())
        {
            if (AxisType.RULE == axis.type)
            {
                String axisName = axis.name
                Integer count = conditionsFiredCountPerAxis[axisName]
                if (count == null || count < 1)
                {
                    throw new CoordinateNotFoundException("No conditions on the rule axis: ${axisName} fired, and there is no default column on this axis, cube: ${name}, input: ${coordinate}",
                            name, coordinate, axisName)
                }
            }
        }
    }

    /**
     * The lowest level cell fetch.  This method uses the Set<Long> to fetch an
     * exact cell, while maintaining the original input coordinate that the location
     * was derived from (required because a given input coordinate could map to more
     * than one cell).  Once the cell is located, it is executed and the value from
     * the executed cell is returned. In the case of Command Cells, it is the return
     * value of the execution, otherwise the return is the value stored in the cell,
     * and if there is no cell, then a default value from NCube is returned, if one
     * is set. Default value ordering - first, a column level default is used if
     * one exists (under Column's meta-key: 'DEFAULT_CELL'). If no column-level
     * default is specified (no non-null value provided), then the NCube level default
     * is chosen (if it exists). If no NCube level default is specified, then the
     * defaultValue passed in is used, if it is non-null. The default value cache
     * should only be used with mapReduce because of its repeated calculation of each
     * column on all axes.
     * REQUIRED: The coordinate passed to this method must have already been run
     * through validateCoordinate(), which duplicates the coordinate and ensures the
     * coordinate has at least an entry for each axis (entry not needed for axes with
     * default column or rule axes).
     */
    T getCellById(final Set<Long> colIds, final Map coordinate, final Map output, Object defaultValue = null, boolean shouldExecute = true)
    {
        // First, get a ThreadLocal copy of an NCube execution stack
        Deque<StackEntry> stackFrame = (Deque<StackEntry>) executionStack.get()
        boolean pushed = false
        try
        {
            // Form fully qualified cell lookup (NCube name + coordinate)
            // Add fully qualified coordinate to ThreadLocal execution stack
            final StackEntry entry = new StackEntry(name, coordinate)
            stackFrame.addFirst(entry)
            pushed = true
            T cellValue

// Handy trick for debugging a failed binding (like space after an input)
//            if (coordinate.containsKey("debug"))
//            {   // Dump out all kinds of binding info
//                log.info("*** DEBUG getCellById() ***")
//                log.info("Axes:")
//                for (Axis axis : axisList.values())
//                {
//                    log.info("  axis name: " + axis.name)
//                    log.info("  axis ID: " + axis.getId())
//                    log.info("  axis type: " + axis.getType())
//                    log.info("  axis valueType: " + axis.getValueType())
//                    log.info("  Columns:")
//                    for (Column column : axis.getColumns())
//                    {
//                        if (hasContent(column.getColumnName()))
//                        {
//                            log.info("    column name: " + column.getColumnName())
//                        }
//                        log.info("    column value: " + column.value)
//                        log.info("    column id: " + column.getId())
//                    }
//                }
//                log.info("Cells:")
//                log.info("  " + cells)
//                log.info("Input:")
//                log.info("  coord IDs: " + idCoord)
//                log.info("  coord Map: " + coordinate)
//            }

            cellValue = cells.get(colIds)
            if (cellValue == null && !cells.containsKey(colIds))
            {   // No cell, look for default
                cellValue = (T) getColumnDefault(colIds)
                if (cellValue == null)
                {   // No Column Default, try NCube default, and finally passed in default
                    cellValue = defaultCellValue == null ? (T) defaultValue : defaultCellValue
                }
            }

            if (cellValue instanceof CommandCell)
            {
                if (shouldExecute)
                {
                    Map ctx = prepareExecutionContext(coordinate, output)
                    return (T) executeExpression(ctx, (CommandCell)cellValue)
                }
                else
                {
                    return cellValue
                }
            }
            else
            {
                trackInputKeysUsed(coordinate, output)
            }
            return cellValue
        }
        finally
        {	// Unwind stack: always remove if stacked pushed, even if Exception has been thrown
            if (pushed)
            {
                stackFrame.removeFirst()
            }
        }
    }

    /**
     * Pre-compile command cells, meta-properties, and rule conditions that are expressions
     */
    CompileInfo compile()
    {
        CompileInfo compileInfo = new CompileInfo()
        compileInfo.setCubeName(this.name)

        cells.each { ids, cell ->
            if (cell instanceof GroovyBase) {
                compileCell(getCoordinateFromIds(ids), (GroovyBase)cell, compileInfo)
            }
        }

        metaProps.each { key, value ->
            if (value instanceof GroovyBase) {
                compileCell([metaProp:key], (GroovyBase)value, compileInfo)
            }
        }

        axisList.each { axisName, axis ->
            axis.columns.each { column ->
                if (column.value instanceof GroovyBase) {
                    compileCell([axis:axisName,column:column.columnName], (GroovyBase)column.value, compileInfo)
                }

                if (column.metaProps) {
                    column.metaProps.each { key, value ->
                        if (value instanceof GroovyBase) {
                            compileCell([axis:axisName,column:column.columnName,metaProp:key], (GroovyBase)value, compileInfo)
                        }
                    }
                }
            }

            if (axis.metaProps) {
                axis.metaProps.each { key, value ->
                    if (value instanceof GroovyBase) {
                        compileCell([axis:axisName,metaProp:key], (GroovyBase)value, compileInfo)
                    }
                }
            }
        }

        return compileInfo
    }

    private void compileCell(Map input, GroovyBase groovyBase, CompileInfo compileInfo) {
        try
        {
            groovyBase.prepare(groovyBase.cmd ?: groovyBase.url, prepareExecutionContext(input,[:]))
        }
        catch (Exception e)
        {
            compileInfo.addException(input,e)
            log.warn("Failed to compile cell for cube: ${name} with coords: ${input.toString()}", e)
        }
    }

    /**
     * Given the passed in column IDs, return the column level default value
     * if one exists or null otherwise.  In the case of intersection, then null
     * is returned, meaning that the n-cube level default cell value will be
     * returned at intersections. The default value cache should only be used
     * with mapReduce because of its repeated calculation of each column on all axes.
     */
    def getColumnDefault(Set<Long> colIds)
    {
        def colDef = null
        Iterator<Long> i = colIds.iterator()
        while (i.hasNext())
        {
            long colId = i.next()
            Axis axis = getAxisFromColumnId(colId, false)
            if (axis == null)
            {   // bad column id, continue check rest of column ids
                continue
            }
            Column boundCol = axis.getColumnById(colId)
            Object defColValue
            if (boundCol != null)
            {
                defColValue = boundCol.getMetaProperty(Column.DEFAULT_VALUE)
            }
            if (defColValue != null)
            {
                if (colDef != null && colDef != defColValue)
                {   // More than one specified in this set (intersection), therefore return null (use n-cube level default)
                    return null
                }
                colDef = defColValue
            }
        }

        return colDef
    }

    private static void trackInputKeysUsed(Map input, Map output)
    {
        if (input instanceof TrackingMap)
        {
            RuleInfo ruleInfo = getRuleInfo(output)
            ruleInfo.addInputKeysUsed(((TrackingMap)input).keysUsed())
        }
    }

    /**
     * Prepare the execution context by providing it with references to
     * important items like the input coordinate, output map, stack,
     * and this (ncube).
     */
    protected Map prepareExecutionContext(final Map coord, final Map output)
    {
        return [input: coord, output: output, ncube: this]  // Input coordinate is already a duplicate at this point
    }

    /**
     * Get a Map of column values and corresponding cell values where all axes
     * but one are held to a fixed (single) column, and one axis allows more than
     * one value to match against it.
     * @param coordinate Map - A coordinate where the keys are axis names, and the
     * values are intended to match a column on each axis, with one exception.  One
     * of the axis values in the coordinate input map must be an instanceof a Set.
     * If the set is empty, all columns and cell values for the given axis will be
     * returned in a Map.  If the Set has values in it, then only the columns
     * on the 'wildcard' axis that match the values in the set will be returned (along
     * with the corresponding cell values).
     * @param output Map that can be written to by the code within the the n-cubes (for example,
     *               GroovyExpressions.  Optional.
     * @param defaultValue Object placed here will be returned if there is no cell at the location
     *                     pinpointed by the input coordinate.  Normally, the defaulValue of the
     *                     n-cube is returned, but if this parameter is passed a non-null value,
     *                     then it will be returned.  Optional.
     * @return a Map containing Axis names and values to bind to those axes.  One of the
     * axes must have a Set bound to it.
     */
    Map<Object, T> getMap(final Map coordinate, Map output = [:], Object defaultValue = null)
    {
        final Map coord = validateCoordinate(coordinate, [:])
        final Map<String, Object> answers = getWildcardInfo(coord)
        Axis wildcardAxis = (Axis)answers.axis
        final List<Column> columns = getWildcardColumns(wildcardAxis, (Set<Comparable>)answers.set, coord)
        final Map<Object, T> result = [:]
        final String axisName = wildcardAxis.name

        for (column in columns)
        {
            final Map params = new CompactCILinkedMap<>(coord)
            params.put(axisName, wildcardAxis.getValueToLocateColumn(column))
            result.put(column.value, getCell(params, output, defaultValue))
        }

        return result
    }

    /**
     * Filter rows of an n-cube.  Use this API to fetch a subset of an n-cube, similar to SQL SELECT.
     * @param rowAxisName String name of axis acting as the ROW axis.
     * @param colAxisName String name of axis acting as the COLUMN axis.
     * @param where Closure groovy closure.  Written as condition in terms of the columns on the colAxisName.
     * Example: { Map input -> (input.state == 'TX' || input.state == 'OH') && (input.attribute == 'fuzzy')}.
     * This will only return rows where this condition is true ('state' and 'attribute' are two column values from
     * the colAxisName). The values for each row in the rowAxis is bound to the where expression for each row.  If
     * the row passes the 'where' condition, it is included in the output.
     * @param options - options map that can include any of the following keys:
     *    - "input" Map just like it is used for getCell() or at().  Only needed when there are three (3)
     *      or more dimensions.  All values in the input map (excluding the axis specified by rowAxisName and colAxisName) are
     *      bound just as they are in getCell() or at().
     *    - "output" the output Map use to write multiple return values to, just like getCell() or at().
     *    - "selectList" is a Collection of Column objects that indicates which will be returned (instead of *, less
     *      columns can be return in the 'result set').
     *    - "whereColumns" is a Collection of Column objects that will be sent to the 'where' closure.  Rather than
     *      send all columns, fewer is better because each where column must be bound to a value for each row.
     *    - MAP_REDUCE_COLUMNS_TO_SEARCH Set which allows reducing the number of columns bound for use in the where clause.  If not
     *      specified, all columns on the colAxisName can be used.  For example, if you had an axis named 'attribute', and it
     *      has 10 columns on it, you could list just two (2) of the columns here, and only those columns would be placed into
     *      values accessible to the where clause via input.xxx == 'someValue'.  The mapReduce() API runs faster when fewer
     *      columns are included in the columnsToSearch.
     *    - MAP_REDUCE_COLUMNS_TO_RETURN Set of values to indicate which columns to return.  If not specified, the entire 'row' is
     *      returned.  For example, if you had an axis named 'attribute', and it has 10 columns on it, you could list just
     *      two (2) of the columns here, in the returned Map of rows, only these two columns will be in the returned Map.
     *      The columnsToSearch and columnsToReturn can be completely different, overlap, or not be specified.  This param
     *      is similar to the 'Select List' portion of the SQL SELECT statement.  It essentially defaults to '*', but you
     *      can have it return less column/value pairs in the returned Map if you add only the columns you want returned here.
     *    - MAP_REDUCE_DEFAULT_VALUE Object placed here will be returned if there is no cell at the location
     *                     pinpointed by the input coordinate.  Normally, the defaulValue of the
     *                     n-cube is returned, but if this parameter is passed a non-null value,
     *                     then it will be returned.  Optional.
     *    - MAP_REDUCE_SHOULD_EXECUTE null or true means that each cell is executed, otherwise each cell is returned
     *      as though a getCellNoExecute() is called on it.
     * @return Map of Maps - The outer Map is keyed by the column values of all row columns.  If the row Axis is a discrete
     * axis, then the keys of the map are all the values of the columns.  If a non-discrete axis is used, then the keys
     * are the name meta-key for each column.  If a non-discrete axis is used and there are no name attributes on the columns,
     * and exception will be thrown.  The 'value' associated to the key (column value or column name) is a Map record,
     * where the keys are the column values (or names) for axis named colAxisName.  The associated values are the values
     * for each cell in the same column, for when the 'where' condition holds true (groovy true).
     */
    private Map internalMapReduce(String rowAxisName, String colAxisName, Closure where = { true }, Map options = [:])
    {
        Map input =  options.containsKey('input') ? (Map) options.input : [:]
        Map output = options.containsKey('output') ? (Map) options.output : [:]
        Object defaultValue = options.get(MAP_REDUCE_DEFAULT_VALUE)
        Collection<Column> selectList = (Collection<Column>) options.selectList
        Collection<Column> whereColumns = (Collection<Column>) options.whereColumns
        final TrackingMap commandInput = new TrackingMap<>(new CompactLinkedMap<>(input))
        Set<Long> boundColumns = bindAdditionalColumns(rowAxisName, colAxisName, commandInput)
        boolean shouldExecute = options.get(MAP_REDUCE_SHOULD_EXECUTE)

        Axis rowAxis = getAxis(rowAxisName)
        Axis colAxis = getAxis(colAxisName)
        boolean isRowDiscrete = rowAxis.type == AxisType.DISCRETE
        boolean isColDiscrete = colAxis.type == AxisType.DISCRETE
        boolean isRowCISTRING = rowAxis.valueType == AxisValueType.CISTRING
        boolean isColCISTRING = colAxis.valueType == AxisValueType.CISTRING

        if (rowAxis.type != AxisType.RULE)
        {
            commandInput.informAdditionalUsage(Arrays.asList(rowAxisName))
        }
        if (colAxis.type != AxisType.RULE)
        {
            commandInput.informAdditionalUsage(Arrays.asList(colAxisName))
        }
        trackInputKeysUsed(commandInput, output)

        final Set<Long> ids = new LinkedHashSet<>(boundColumns)
        final Map matchingRows = isRowCISTRING ? new CompactCILinkedMap<>() : new CompactLinkedMap<>()
        final Map whereVars = isColCISTRING ? new CompactCILinkedMap<>(input) : new CompactLinkedMap<>(input)

        Collection<Column> rowColumns
        Object rowAxisValue = input.get(rowAxisName)
        if (rowAxisValue)
        {   // Caller is limiting which rows to include by supplying the row axis name with a bound value on input.
            Set set
            if (rowAxisValue instanceof Collection)
            {
                set = new HashSet((Collection)rowAxisValue)
            }
            else
            {
                set = new HashSet()
                set.add(rowAxisValue)
            }
            rowColumns = selectColumns(rowAxis, set)
            while (rowColumns.contains(null))
            {
                rowColumns.remove(null)
            }
        }
        else
        {   // Scan all rows
            rowColumns = rowAxis.columns
        }

        boolean isOneParamWhere = where.maximumNumberOfParameters == 1
        for (Column row : rowColumns)
        {
            commandInput.put(rowAxisName, rowAxis.getValueToLocateColumn(row))
            long rowId = row.id
            ids.add(rowId)

            for (Column column : whereColumns)
            {
                long whereId = column.id
                ids.add(whereId)
                commandInput.put(colAxisName, colAxis.getValueToLocateColumn(column))
                Object colKey = isColDiscrete ? column.value : column.columnName
                def val
                try
                {
                    val = getCellById(ids, commandInput, output, defaultValue, shouldExecute)
                }
                catch (Exception e)
                {
                    val = "err: ${getExceptionMessage(getDeepestException(e))}".toString()
                }
                whereVars.put(colKey, val)
                ids.remove(whereId)
            }

            def whereResult = isOneParamWhere ? where(whereVars) : where(whereVars, commandInput)

            if (whereResult)
            {
                Comparable key
                if (isRowDiscrete)
                {
                    key = row.value
                }
                else
                {
                    if (isEmpty(row.columnName))
                    {
                        throw new IllegalStateException("Non-discrete axis columns must have a meta-property 'name' set in order to use them for mapReduce().  Cube: ${name}, Axis: ${rowAxis.name}")
                    }
                    key = row.columnName
                }

                String axisName = colAxis.name
                Map result = isColCISTRING ? new CompactCILinkedMap<>() : new CompactLinkedMap<>()
                for (Column column : selectList)
                {
                    def colValue = isColDiscrete ? column.value : column.columnName
                    if (whereVars.containsKey(colValue))
                    {
                        result.put(colValue, whereVars.get(colValue))
                        continue
                    }
                    commandInput.put(axisName, column.valueThatMatches)
                    long colId = column.id
                    ids.add(colId)
                    result.put(colValue, getCellById(ids, commandInput, output, defaultValue, shouldExecute))
                    ids.remove(colId)
                }
                matchingRows.put(key, result)
            }
            ids.remove(rowId)
        }
        return matchingRows
    }

    /**
     * Use mapReduce() [select] on n-dimensional n-cube where n >= 2.  Axes other than the where clause can be left off, or
     * can have a value specifically bound to them (reducing search time).
     * @param colAxisName String name of axis acting as the COLUMN axis.
     * @param where Closure groovy closure.  Written as condition in terms of the columns on the colAxisName.
     * Example: { Map input -> (input.state == 'TX' || input.state == 'OH') && (input.attribute == 'fuzzy')}.
     * This will only return rows where this condition is true ('state' and 'attribute' are two column values from
     * the colAxisName). The values for each row in the rowAxis is bound to the where expression for each row.  If
     * the row passes the 'where' condition, it is included in the output.
     * @param options - options map that can include any of the following keys:
     *    - "input" Map just like it is used for getCell() or at().  Only needed when there are three (3)
     *      or more dimensions.  All values in the input map (excluding the axis specified by rowAxisName and colAxisName) are
     *      bound just as they are in getCell() or at().
     *    - "output" the output Map use to write multiple return values to, just like getCell() or at().
     *    - MAP_REDUCE_COLUMNS_TO_SEARCH Set which allows reducing the number of columns bound for use in the where clause.  If not
     *      specified, all columns on the colAxisName can be used.  For example, if you had an axis named 'attribute', and it
     *      has 10 columns on it, you could list just two (2) of the columns here, and only those columns would be placed into
     *      values accessible to the where clause via input.xxx == 'someValue'.  The mapReduce() API runs faster when fewer
     *      columns are included in the columnsToSearch.
     *    - MAP_REDUCE_COLUMNS_TO_RETURN Set of values to indicate which columns to return.  If not specified, the entire 'row' is
     *      returned.  For example, if you had an axis named 'attribute', and it has 10 columns on it, you could list just
     *      two (2) of the columns here, in the returned Map of rows, only these two columns will be in the returned Map.
     *      The columnsToSearch and columnsToReturn can be completely different, overlap, or not be specified.  This param
     *      is similar to the 'Select List' portion of the SQL SELECT statement.  It essentially defaults to '*', but you
     *      can have it return less column/value pairs in the returned Map if you add only the columns you want returned here.
     *    - MAP_REDUCE_DEFAULT_VALUE Object placed here will be returned if there is no cell at the location
     *                     pinpointed by the input coordinate.  Normally, the defaulValue of the
     *                     n-cube is returned, but if this parameter is passed a non-null value,
     *                     then it will be returned.  Optional.
     *    - MAP_REDUCE_SHOULD_EXECUTE null or true means that each cell is executed, otherwise each cell is returned
     *      as though a getCellNoExecute() is called on it.
     * @return Map of Maps - The outer Map is keyed by the column values of all row columns.  If the row Axis is a discrete
     * axis, then the keys of the map are all the values of the columns.  If a non-discrete axis is used, then the keys
     * are the name meta-key for each column.  If a non-discrete axis is used and there are no name attributes on the columns,
     * and exception will be thrown.  The 'value' associated to the key (column value or column name) is a Map record,
     * where the keys are the column values (or names) for axis named colAxisName.  The associated values are the values
     * for each cell in the same column, for when the 'where' condition holds true (groovy true).
     */
    Map mapReduce(String colAxisName, Closure where = { true }, Map options = [:])
    {
        throwIf(!colAxisName, 'The column axis name cannot be null')
        throwIf(!where, 'The where clause cannot be null')

        Axis colAxis = axisList[colAxisName]
        throwIf(!colAxis,"The column axis name does not exist. Cube: ${name}, Axis: ${colAxisName}")

        Map input = options.containsKey('input') ? (Map)options.input : [:]
        Set columnsToSearch = (Set)options[MAP_REDUCE_COLUMNS_TO_SEARCH]
        Set columnsToReturn = (Set)options[MAP_REDUCE_COLUMNS_TO_RETURN]

        Map commandOpts = new TrackingMap<>(new CompactCILinkedMap(options))
        commandOpts.input = new TrackingMap<>(new CompactCILinkedMap(input))
        commandOpts.selectList = selectColumns(colAxis, columnsToReturn)
        commandOpts.whereColumns = selectColumns(colAxis, columnsToSearch)
        commandOpts.put(MAP_REDUCE_SHOULD_EXECUTE, options.get(MAP_REDUCE_SHOULD_EXECUTE) == null ? true : options.get(MAP_REDUCE_SHOULD_EXECUTE))

        String rowAxisName
        Set<String> searchAxes = new CaseInsensitiveSet<>(axisNames)
        searchAxes.remove(colAxisName)
        searchAxes.removeAll(input.keySet())
        
        if (searchAxes.empty)
        {
            searchAxes.addAll(axisNames)
            searchAxes.remove(colAxisName)
            rowAxisName = searchAxes.first()
        }
        else
        {
            searchAxes.sort { getAxis(it).columns.size() }
            rowAxisName = searchAxes.last() // take axis with most columns first
        }
        Set<String> otherAxes = new CaseInsensitiveSet<>(searchAxes)
        otherAxes.remove(rowAxisName)
        Map result
        if (otherAxes.empty)
        {
            result = internalMapReduce(rowAxisName, colAxisName, where, commandOpts)
        }
        else
        {
            result = executeMultidimensionalMapReduce(otherAxes, rowAxisName, colAxisName, where, commandOpts)
        }
        return result
    }

    private Map executeMultidimensionalMapReduce(Set<String> axes, String rowAxisName, String colAxisName, Closure where, Map options)
    {
        Map result
        Map ret = new LinkedHashMap()
        String axisName = axes.last() // take axis with most columns first
        List<Column> columns = getAxis(axisName).columns
        Set<String> otherAxes = axes - axisName
        boolean noMoreAxes = otherAxes.empty
        Map input = (Map) options.input

        for (Column column : columns)
        {
            input.put(axisName, column.value)
            if (noMoreAxes)
            {
                result = internalMapReduce(rowAxisName, colAxisName, where, options)
                for (Map.Entry resultEntry : result)
                {
                    Map inputVal = new LinkedHashMap(input)
                    inputVal.put(rowAxisName, resultEntry.key)
                    ret.put(inputVal, resultEntry.value)
                }
            }
            else
            {
                result = executeMultidimensionalMapReduce(otherAxes, rowAxisName, colAxisName, where, options)
                ret.putAll(result)
            }
        }
        return ret
    }

    private Collection<Column> selectColumns(Axis axis, Set<String> valuesMatchingColumns)
    {
        Collection<Column> columns = []
        Set<String> missingColumnNames = []

        boolean isDiscrete = axis.type == AxisType.DISCRETE

        if (valuesMatchingColumns == null || valuesMatchingColumns.empty)
        {   // If empty or null, then treat as '*' (all columns)
            if (isDiscrete)
            {
                return axis.columns
            }
            else
            {
                for (Column column : axis.columns)
                {
                    if (isEmpty(column.columnName))
                    {
                        throw new IllegalStateException("Non-discrete axis columns must have a meta-property 'name' set in order to use them for mapReduce().  Cube: ${name}, Axis: ${axis.name}")
                    }
                    columns.add(column)
                }
                return columns
            }
        }

        if (isDiscrete)
        {
            for (String value : valuesMatchingColumns)
            {
                Column col = axis.findColumn(value)
                if (col)
                {
                    columns.add(col)
                }
                else
                {
                    missingColumnNames.add(value)
                }
            }
        }
        else
        {
            for (String value : valuesMatchingColumns)
            {
                if (isEmpty(value))
                {
                    throw new IllegalStateException("Non-discrete axis columns must have a meta-property 'name' set in order to use them for mapReduce().  Cube: ${name}, Axis: ${axis.name}")
                }
                Column col = axis.findColumnByName(value)
                if (col)
                {
                    columns.add(col)
                }
                else
                {
                    missingColumnNames.add(value)
                }
            }
        }
        if (missingColumnNames)
        {
            log.warn("Column(s) do not exist on Cube: {}, Axis: {}, Columns: {}", name, axis.name, missingColumnNames)
        }
        return columns
    }

    private static void throwIf(boolean throwCondition, String msg)
    {
        if (throwCondition)
        {
            throw new IllegalArgumentException(msg)
        }
    }

    private Set<Long> bindAdditionalColumns(String rowAxisName, String colAxisName, Map input)
    {
        if (axisList.size() <= 2)
        {
            return [] as Set
        }
        Set<String> axisNames = axisList.keySet()
        Set<String> otherAxisNames = axisNames - [rowAxisName, colAxisName]
        Set<String> inputAxisNames = input.keySet()
        if (!inputAxisNames.containsAll(otherAxisNames))
        {
            Set<String> otherAxes = axisNames - [rowAxisName, colAxisName]
            Set<String> otherAxesWithDefaults = otherAxes.findAll { String axisName ->
                getAxis(axisName).hasDefaultColumn()
            }

            if (!input.keySet().containsAll(otherAxes - otherAxesWithDefaults))
            {
                throw new IllegalArgumentException("Using row axis: ${rowAxisName} and query axis: ${colAxisName} for cube: ${this.name} - bindings for axes: ${otherAxisNames} must be supplied.")
            }
        }

        Set<Long> boundColumns = new LongHashSet()
        for (String axisName : otherAxisNames)
        {
            Axis otherAxis = getAxis(axisName)
            def value = input.get(axisName)
            Column column = otherAxis.findColumn((Comparable)value)
            if (!column)
            {
                throw new CoordinateNotFoundException("Column: ${value} not found on axis: ${axisName} on cube: ${name}", name, null, axisName, value)
            }
            boundColumns.add(column.id)
        }
        return boundColumns
    }

    /**
     * Get / Create the RuleInfo Map stored at output[NCube.RULE_EXEC_INFO]
     */
    static RuleInfo getRuleInfo(Map output)
    {
        RuleInfo ruleInfo
        if (output.containsKey(RULE_EXEC_INFO))
        {   // RULE_EXEC_INFO Map already exists, must be a recursive call.
            return (RuleInfo) output.get(RULE_EXEC_INFO)
        }
        // RULE_EXEC_INFO Map does not exist, create it.
        ruleInfo = new RuleInfo()
        output.put(RULE_EXEC_INFO, ruleInfo)
        return ruleInfo
    }

    /**
     * Follow the exact same treatment of TRUTH as Groovy
     */
    static boolean isTrue(Object ruleValue)
    {
        if (ruleValue == null)
        {   // null indicates rule did NOT fire
            return false
        }

        if (ruleValue instanceof Boolean)
        {
            return ruleValue
        }

        if (ruleValue instanceof Number)
        {
            boolean isZero = ((byte) 0) == ruleValue ||
                    ((short) 0) == ruleValue ||
                    0i == ruleValue ||
                    0L == ruleValue ||
                    0.0d == ruleValue ||
                    0.0f == ruleValue ||
                    BigInteger.ZERO == ruleValue ||
                    BigDecimal.ZERO == ruleValue
            return !isZero
        }

        if (ruleValue instanceof String)
        {
            return "" != ruleValue
        }

        if (ruleValue instanceof Map)
        {
            return ((Map)ruleValue).size() > 0
        }

        if (ruleValue instanceof Collection)
        {
            return ((Collection)ruleValue).size() > 0
        }

        if (ruleValue instanceof Enumeration)
        {
            return ((Enumeration)ruleValue).hasMoreElements()
        }

        if (ruleValue instanceof Iterator)
        {
            return ((Iterator)ruleValue).hasNext()
        }

        return true
    }

    private static boolean isZero(Integer count)
    {
        return count == null || count == 0
    }

    /**
     * Bind the input coordinate to each axis.  The reason the column is a List of columns that the coordinate
     * binds to on the axis, is to support RULE axes.  On a regular axis, the coordinate binds
     * to a column (with a binary search or hashMap lookup), however, on a RULE axis, the act
     * of binding to an axis results in a List<Column>.
     * @param input The passed in input coordinate to bind (or multi-bind) to each axis.
     * @param output Map used by rules to write subtotals, etc. to.
     * @param multiRule boolean true will return all appropriate rules, false will only return the first matching rule.
     */
    private Map<String, List<Column>> selectColumns(Map<String, Object> input, Map output, boolean multiRule = true)
    {
        Map<String, List<Column>> bindings = new CompactCIHashMap<>()
        for (entry in axisList.entrySet())
        {
            final String axisName = entry.key
            final Axis axis = entry.value
            final Object value = input.get(axisName)

            if (AxisType.RULE == axis.type)
            {   // For RULE axis, all possible columns must be added (they are tested later during execution)
                if (multiRule)
                {
                    if (value instanceof String)
                    {
                        bindings.put(axisName, axis.getRuleColumnsStartingAt((String) value))
                    }
                    else if (value instanceof Collection)
                    {   // Collection of rule names to select (orchestration)
                        Collection<String> orchestration = (Collection)value
                        bindings.put(axisName, axis.findColumns(orchestration))
                        assertAtLeast1Rule(bindings.get(axisName), "No rule selected on rule-axis: ${axis.name}, rule names ${orchestration}, cube: ${name}")
                    }
                    else if (value instanceof Map)
                    {   // key-value pairs that meta-properties of rule columns must match to select rules.
                        Map<String, Object> required = (Map)value
                        bindings.put(axisName, axis.findColumns(required))
                        assertAtLeast1Rule(bindings[axisName], "No rule selected on rule-axis: ${axis.name}, meta-properties must match ${required}, cube: ${name}")
                    }
                    else if (value instanceof Closure)
                    {
                        bindings.put(axisName, axis.findColumns((Closure)value))
                        assertAtLeast1Rule(bindings.get(axisName), "No rule selected on rule-axis: ${axis.name}, meta-properties must match closure, cube: ${name}")
                    }
                    else
                    {
                        bindings.put(axisName, axis.columns)
                    }
                }
                else
                {
                    bindings.put(axisName, [axis.findColumn((Comparable)value)])
                }
            }
            else
            {   // Find the single column that binds to the input coordinate on a regular axis.
                final Column column = axis.findColumn((Comparable)value)
                if (column == null || column.default)
                {
                    trackUnboundAxis(output, name, axisName, value)
                }
                if (column == null)
                {
                    throw new CoordinateNotFoundException("Value '${value}' not found on axis: ${axisName}, cube: ${name}",
                            name, input, axisName, value)
                }
                bindings.put(axisName, [column])    // Binding is a List of one column on non-rule axis
            }
        }

        return bindings
    }

    private static void trackUnboundAxis(Map output, String cubeName, String axisName, Object value)
    {
        boolean track = ncubeRuntime?.trackBindingsOn
        if (track)
        {
            RuleInfo ruleInfo = getRuleInfo(output)
            ruleInfo.addUnboundAxis(cubeName, axisName, value)
        }
    }

    private static void assertAtLeast1Rule(Collection<Column> columns, String errorMessage)
    {
        if (columns.empty)
        {   // Match default (if it exists) and none of the orchestration columns have matched
            throw new CoordinateNotFoundException(errorMessage)
        }
    }

    private static Map<String, Integer> getCountersPerAxis(final String[] axisNames)
    {
        final Map<String, Integer> counters = new CompactCIHashMap<>()

        // Set counters to 1
        for (axisName in axisNames)
        {
            counters.put(axisName, 1)
        }
        return counters
    }

    /**
     * @param coordinate Set<Long> coordinate usuable by getCellById(), setCellByEdit(), ...
     * @return Map<Long, Long> which maps an axis ID to the Column ID on that axis.
     */
    private Map<Long, Long> getAxisToCoord(Set<Long> coordinate)
    {
        Iterator<Long> i = coordinate.iterator()
        Map<Long, Long> axisToCoord = new HashMap<>()

        while (i.hasNext())
        {
            Long id = i.next()
            long axisId = MathUtil.divide(id, Axis.BASE_AXIS_ID)
            axisToCoord.put(axisId, id)
        }
        return axisToCoord
    }

    /**
     * Make sure the returned Set<Long> contains a column ID for each axis, even if the input set does
     * not have a coordinate for an axis, but the axis has a default column (the default column's ID will
     * be added to the returned Set).
     * @return Set<Long> that contains only the necessary coordinates from the passed in Collection.  If it cannot
     * bind, null is returned.
     * Example of what this method does:
     * axis 1 (has columns with IDs 10, 11, ...)
     * axis 2 (20, 21, ...)
     * axis 3 (30, 31, ...)
     * axis 4 (40, 41, ...)
     * passed in [16, 35]
     * returned [16, 35, 2.def, 4.def]  // 2.def = ID of default column on axis 2, 4.def (ditto)
     */
    protected Set<Long> ensureFullCoordinate(Set<Long> coordinate)
    {
        if (coordinate == null)
        {
            coordinate = new LongHashSet()
        }
        int numDim = numDimensions

        if (coordinate.size() >= numDim)
        {   // Fast-path (no default column projecting)
            Map<Long, Long> axisToCoord = new HashMap<>()
            Iterator<Long> i = coordinate.iterator()

            while (i.hasNext())
            {
                Long id = i.next()
                // Fast way to get unique Axis ids without division
                axisToCoord.put(id - (id % Axis.BASE_AXIS_ID), 0L)
            }

            if (axisToCoord.size() == numDim)
            {   // Switch to LongHashSet if the instance passed in is not already LongHashSet
                return coordinate instanceof LongHashSet ? coordinate : new LongHashSet(coordinate)
            }
        }

        // Help-out - add default column binds, etc.
        Set<Long> ids = new LongHashSet()
        Map<Long, Long> axisToCoord = getAxisToCoord(coordinate)

        for (axis in axisList.values())
        {
            Long coordId = axisToCoord.get(axis.id)
            if (coordId)
            {
                ids.add(coordId)
            }
            else if (coordId == null && axis.hasDefaultColumn())
            {   // If inbound coordinate does not have an ID for an axis, snag the default column ID for that axis (if the axis has a default)
                ids.add(axis.defaultColId)
            }
        }

        return ids.size() == numDim ? ids : null
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
    Object[] getCells(Object[] idArrays, Map input, Map output = [:], Object defaultValue = null)
    {
        final Map commandInput = new TrackingMap<>(new CompactCILinkedMap(input ?: [:]))

        Object[] ret = new Object[idArrays.length]
        int idx = 0

        for (coord in idArrays)
        {
            Set<Long> key = new LinkedHashSet<>()
            for (item in coord)
            {
                key.add(convertToLong(item))
            }
            key = ensureFullCoordinate(key)
            ensureInputBindings(commandInput, key)
            CellInfo cellInfo
            try
            {
                def value = getCellById(key, commandInput, output, defaultValue)
                try
                {
                    cellInfo = new CellInfo(value)
                    ret[idx++] = [coord, cellInfo as Map]
                }
                catch (Exception ignored)
                {
                    cellInfo = new CellInfo(value.toString())    // Convert non-logical primitive to String
                    ret[idx++] = [coord, cellInfo as Map]
                }
            }
            catch (Exception e)
            {
                cellInfo = new CellInfo("err: ${getExceptionMessage(getDeepestException(e))}".toString())
                ret[idx++] = [coord, cellInfo as Map]
            }
        }

        return ret
    }

    private static String getExceptionMessage(Throwable t)
    {
        return t.message ?: t.class.name
    }
    
    /**
     * This API ensures that the passed in inputMap has bindings for all axes specified by ids.
     */
    protected void ensureInputBindings(Map input, Set<Long> ids)
    {
        Iterator i = ids.iterator()
        while (i.hasNext())
        {
            Long id = i.next()
            Axis axis = getAxisFromColumnId(id, false)
            if (axis != null)
            {
                if (!input.containsKey(axis.name))
                {
                    Column column = axis.getColumnById(id)
                    if (column)
                    {
                        input.put(axis.name, axis.getValueToLocateColumn(column))
                    }
                }
            }
        }
    }

    /**
     * Convert an Object to a Map.  This allows an object to then be passed into n-cube as a coordinate.  Of course
     * the returned map can have additional key/value pairs added to it after calling this method, but before calling
     * getCell().
     * @param o Object any Java object to bind to an NCube.
     * @return Map where the fields of the object are the field names from the class, and the associated values are
     * the values associated to the fields on the object.
     */
    static Map objectToMap(final Object o)
    {
        if (o == null)
        {
            throw new IllegalArgumentException("null passed into objectToMap.  No possible way to convert null into a Map.")
        }

        try
        {
            final Collection<Field> fields = getDeepDeclaredFields(o.class)
            final Iterator<Field> i = fields.iterator()
            final Map newCoord = new CompactCILinkedMap<>()

            while (i.hasNext())
            {
                final Field field = i.next()
                final String fieldName = field.name
                final Object fieldValue = field.get(o)
                if (newCoord.containsKey(fieldName))
                {   // This can happen if field name is same between parent and child class (dumb, but possible)
                    newCoord[field.declaringClass.name + '.' + fieldName] = fieldValue
                }
                else
                {
                    newCoord[fieldName] = fieldValue
                }
            }
            return newCoord
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to access field of passed in object.", e)
        }
    }

    private static String stackToString()
    {
        final Deque<StackEntry> stack = executionStack.get()
        final Iterator<StackEntry> i = stack.descendingIterator()
        final StringBuilder s = new StringBuilder()

        while (i.hasNext())
        {
            final StackEntry key = i.next()
            s.append("-> cell:${key.toString()}")
            if (i.hasNext())
            {
                s.append('\n')
            }
        }

        return s.toString()
    }

    /**
     * Increment the variable radix number passed in.  The number is represented by a Map, where the keys are the
     * digit names (axis names), and the values are the associated values for the number.
     * @return false if more incrementing can be done, otherwise true.
     */
    private static boolean incrementVariableRadixCount(final Map<String, Integer> counters,
                                                       final Map<String, List<Column>> bindings,
                                                       final String[] axisNames)
    {
        int digit = axisNames.length - 1

        while (true)
        {
            final String axisName = axisNames[digit]
            final int count = counters[axisName]
            final List<Column> cols = bindings[axisName]

            if (count >= cols.size())
            {   // Reach max value for given dimension (digit)
                if (digit == 0)
                {   // we have reached the max radix for the most significant digit - we are done
                    return false
                }
                counters[axisNames[digit--]] = 1
            }
            else
            {
                counters[axisName] = count + 1  // increment counter
                return true
            }
        }
    }

    /**
     * @param coordinate passed in coordinate for accessing this n-cube
     * @return Map with two keys.  One key is the String 'axis' with the associated value being the Axis instance
     * that matches the coordinate which has a Set associated to it.  The other key is the String 'set' which has
     * the Set of values (or empty Set) to match against the axis.
     */
    private Map<String, Object> getWildcardInfo(final Map<String, Object> coordinate)
    {
        Map<String, Object> answers = [:]
        int count = 0

        for (entry in coordinate.entrySet())
        {
            if (entry.value instanceof Set)
            {
                count++
                answers.axis = axisList.get(entry.key)      // intentional case insensitive match
                answers.set = entry.value
            }
        }

        if (count == 0)
        {
            throw new IllegalArgumentException("No 'Set' value found within input coordinate, cube: ${name}")
        }

        if (count > 1)
        {
            throw new IllegalArgumentException("More than one 'Set' found as value within input coordinate, cube: ${name}")
        }

        return answers
    }

    /**
     * @param coordinate Map containing Axis names as keys, and Comparable's as
     * values.  The coordinate key matches an axis name, and then the column on the
     * axis is found that best matches the input coordinate value.  Use this when
     * the input is expected to only match one value on an axis.  For example, for a
     * RULE axis, the key name is the RULE axis name, and the value is the rule name
     * to match.
     * @return a Set key in the form of Column1,Column2,...Column-n where the Columns
     * are the Column IDs along the axis that match the value associated to the key (axis
     * name) of the passed in input coordinate. The ordering is the order the axes are
     * stored within in NCube.  The returned Set is the 'key' of NCube's cells Map, which
     * maps a coordinate (Set of column IDs) to the cell value.
     */
    Set<Long> getCoordinateKey(final Map coordinate, Map output = new CompactCILinkedMap())
    {
        Map safeCoord

        if (coordinate instanceof TrackingMap || coordinate instanceof CompactCILinkedMap || coordinate instanceof CompactCIHashMap || coordinate instanceof CaseInsensitiveMap)
        {
            safeCoord = coordinate
        }
        else
        {
            safeCoord = (coordinate == null) ? new CompactCILinkedMap<>() : new CompactCILinkedMap<>(coordinate)
        }

        Map<String, List<Column>> bindings = selectColumns(safeCoord, output, false)
        Set<Long> ids = new HashSet<>()
        for (Map.Entry<String, List<Column>> entry : bindings.entrySet())
        {
            Column col = entry.value.get(0);
            if (col == null)
            {
                String axisName = entry.key
                throw new CoordinateNotFoundException("Value '${safeCoord.get(axisName)}' not found on axis: ${axisName}, cube: ${name}", name, safeCoord, axisName, safeCoord.get(axisName))
            }
            ids.add(col.id)
        }
        return new LongHashSet(ids)
    }

    /**
     * Ensure that the Map coordinate dimensionality satisfies this nCube.
     * This method verifies that all axes are listed by name in the input coordinate.
     * @param coordinate Map input coordinate
     */
    private Map validateCoordinate(final Map coordinate, final Map output)
    {
        if (coordinate == null)
        {
            throw new IllegalArgumentException("'null' passed in for coordinate Map, n-cube: ${name}")
        }

        // Ensure required scope is supplied within the input coordinate
        Set<String> requiredScope = getRequiredScope(coordinate, output)
        requiredScope.removeAll(coordinate.keySet())

        if (!requiredScope.empty)
        {
            Set<String> missingScope = new LinkedHashSet<>()
            for (String key : requiredScope)
            {
                Axis axis = getAxis(key)
                if (axis == null || !axis.hasDefaultColumn())
                {
                    missingScope.add(key)
                }
            }

            if (!missingScope.empty)
            {
                // Message below intentionally only shows the required scope keys left in the requireScope Set, as opposed
                // to showing all required keys.  Note that the InvalidateCoordinateException does store the full set
                // of required scope keys.
                throw new InvalidCoordinateException("Input coordinate: ${coordinate.keySet()}, missing required scope key(s): ${missingScope}, cube: ${name}, appId: ${appId}",
                        name, coordinate.keySet(), getRequiredScope(coordinate, output))
            }
        }

        return coordinate
    }

    /**
     * Ensure the input coordinate Map is in the structure of TrackingMap --> CompactCILinkedMap --> MapEntries.
     * If not, the passed in Map will be converted to a CompactCILinkedMap, wrapped by a TrackingMap.
     * @param coordinate Map input coordinate
     * @return Map new input coordinate (if needed) or the original Map passed in.
     */
    private Map wrapCoordinate(final Map coordinate)
    {
        if (coordinate instanceof TrackingMap)
        {
            TrackingMap trackingMap = (TrackingMap) coordinate
            Map wrapped = trackingMap.getWrappedMap()
            if (wrapped instanceof CompactCIHashMap || wrapped instanceof CompactCILinkedMap || wrapped instanceof CaseInsensitiveMap)
            {   // Already wrapped (called from 'inside' - a GroovyExpression or GroovyTemplate using at() ,go(), or use()
                return coordinate
            }
            return new TrackingMap(new CompactCILinkedMap<>(trackingMap.getWrappedMap()))
        }

        // 1. Ensure the input coordinate Map is protected from overwrites.
        // 2. Ensure the input coordinate Map is case-insensitive.
        return new TrackingMap(new CompactCILinkedMap<>(coordinate))
    }

    /**
     * @param coordinate Map containing Axis names as keys, and Comparable's as
     * values.  The coordinate key matches an axis name, and then the column on the
     * axis is found that best matches the input coordinate value. The input coordinate
     * must contain one Set as a value for one of the axes of the NCube.  If empty,
     * then the Set is treated as '*' (star).  If it has 1 or more elements in
     * it, then for each entry in the Set, a column position value is returned.
     *
     * @return a List of all columns that match the values in the Set, or in the
     * case of an empty Set, all columns on the axis.
     */
    private List<Column> getWildcardColumns(final Axis wildcardAxis, final Set<Comparable> wildcardSet, final Map coordinate)
    {
        final List<Column> columns = []

        // To support '*', an empty Set is bound to the axis such that all columns are returned.
        if (wildcardSet.empty)
        {
            columns.addAll(wildcardAxis.columns)
        }
        else
        {
            // This loop grabs all the columns from the axis which match the values in the Set
            for (value in wildcardSet)
            {
                final Column column = wildcardAxis.findColumn(value)
                if (column == null)
                {
                    String axisName = wildcardAxis.name
                    throw new CoordinateNotFoundException("Value '${value}' not found using Set on axis: ${axisName}, cube: ${name}",
                            name, coordinate, axisName, value)
                }

                columns.add(column)
            }
        }

        return columns
    }

    /**
     * @return T the default value that will be returned when a coordinate specifies
     * a cell that has no entry associated to it.  This is a space-saving technique,
     * as the most common cell value can be set as the defaultCellValue, and then the
     * cells that would have had this value can be left empty.
     */
    T getDefaultCellValue()
    {
        return defaultCellValue
    }

    /**
     * Set the default cell value for this n-cube.  This is a space-saving technique,
     * as the most common cell value can be set as the defaultCellValue, and then the
     * cells that would have had this value can be left empty.
     * @param defaultCellValue T the default value that will be returned when a coordinate
     * specifies a cell that has no entry associated to it.
     */
    void setDefaultCellValue(final T defaultCellValue)
    {
        this.defaultCellValue = defaultCellValue
        clearSha1()
    }

    /**
     * Clear all cell values.  All axes and columns remain.
     */
    void clearCells()
    {
        cells.clear()
        clearSha1()
    }

    /**
     * Add a column to the n-cube
     * @param axisName String name of the Axis to which the column will be added.
     * @param value Comparable that will be the value for the given column.  Cannot be null.
     * @param colName The optional name of the column, useful for RULE axis columns.  Optional.
     * @param suggestedId Long id.  If id is not valid for the column (unique id, and axis portion matches axis on which
     *                    it is added), then the ID will be generated.  Optional.
     * @return Column the added Column.
     */
    Column addColumn(final String axisName, final Comparable value, String colName = null, Long suggestedId = null)
    {
        final Axis axis = getAxis(axisName)
        if (axis == null)
        {
            throw new IllegalArgumentException("Could not add column. Axis name '${axisName}' was not found on cube: ${name}")
        }
        Column newCol = axis.addColumn(value, colName, suggestedId)
        clearSha1()
        return newCol
    }

    /**
     * Delete a column from the named axis.  All cells that reference this
     * column will be deleted.
     * @param axisName String name of Axis contains column to be removed.
     * @param value Comparable value used to identify column (Long if identifying a RULE column)
     * @return boolean true if deleted, false otherwise
     */
    boolean deleteColumn(final String axisName, final Comparable value)
    {
        final Axis axis = getAxis(axisName)
        if (axis == null)
        {
            throw new IllegalArgumentException("Could not delete column. Axis name '${axisName}' was not found on cube: ${name}")
        }

        Column column
        if (axis.type == AxisType.RULE)
        {   // Rule axes are deleted by ID, name, or null (default - can be deleted with null or ID).
            if (value instanceof Long)
            {
                column = axis.deleteColumnById((Long)value)
            }
            else if (value instanceof String)
            {
                column = axis.findColumnByName((String)value)
                if (column != null)
                {
                    axis.deleteColumnById(column.id)
                }
            }
            else if (value == null)
            {
                column = axis.deleteColumn(null)
            }
            else
            {
                return false
            }
        }
        else
        {
            column = axis.deleteColumn(value)
        }
        if (column == null)
        {
            return false
        }

        clearSha1()
        long colId = column.id

        // Remove all cells that reference the deleted column
        final Iterator<Set<Long>> i = cells.keySet().iterator()

        while (i.hasNext())
        {
            final Set<Long> key = i.next()
            // Locate the uniquely identified column, regardless of axis order
            if (key.contains(colId))
            {
                i.remove()
            }
        }
        return true
    }

    /**
     * Convenience method to locate column when you have the axis name as a String and the value to find.
     * If the named axis is a rule axis, then it is expected that value is either a String name of the rule
     * or the long ID of the rule column.
     * @param axisName String name of axis.  Case does not matter when locating by name.
     * @param value Comparable value used to find the column.
     * @return Column instance if located, otherwise null.
     */
    Column findColumn(String axisName, Comparable value)
    {
        Axis axis = getAxis(axisName)
        if (!axis)
        {
            return null
        }

        return axis.findColumn(value)
    }

    /**
     * Change the value of a Column along an axis.
     * @param id long indicates the column to change
     * @param value Comparable new value to set into the column
     * @param order int (optional) new display order for column
     */
    void updateColumn(long id, Comparable value, String name = null, int order = -1i)
    {
        Axis axis = getAxisFromColumnId(id)
        if (axis == null)
        {
            throw new IllegalArgumentException("No column exists with the id ${id} within cube: ${name}")
        }
        clearSha1()
        axis.updateColumn(id, value, name, order)
    }

    /**
     * Update all of the columns along an axis at once.  Any cell referencing a column that
     * is deleted, will also be deleted from the internal sparse matrix (Map) of cells.
     * @param axisName String axis name to update
     * @param newCols  List<Column> display ordered list of columns.  An n-cube editor program,
     *             for example, would call this API with an axisName and set of new columns.
     * @return Set<Long> column ids, indicating which columns were deleted.
     */
    Set<Long> updateColumns(final String axisName, final Collection<Column> newCols, boolean allowPositiveColumnIds = false)
    {
        if (newCols == null)
        {
            throw new IllegalArgumentException("Cannot pass in null for list of columns when updating columns, cube: ${name}")
        }
        if (!axisList.containsKey(axisName))
        {
            throw new IllegalArgumentException("No axis exists with the name: ${axisName}, cube: ${name}")
        }

        final Axis axisToUpdate = axisList[axisName]
        final Set<Long> colsToDel = axisToUpdate.updateColumns(newCols, allowPositiveColumnIds)

        if (!colsToDel.empty)
        {   // If there are columns to delete, then delete any cells referencing those columns
            Iterator<Set<Long>> i = cells.keySet().iterator()
            while (i.hasNext())
            {
                Set<Long> cols = i.next()

                for (id in cols)
                {
                    if (colsToDel.contains(id))
                    {   // If cell referenced deleted column, drop the cell
                        i.remove()
                        break
                    }
                }
            }
        }

        clearSha1()
        return colsToDel
    }

    /**
     * Given the passed in Column ID, return the axis that contains the column.
     * @param id Long id of a Column on one of the Axes within this n-cube.
     * @param columnMustExist boolean, defaults to true. The axis will only be
     * returned if the column id passed in is that of a column on the axis. For
     * example, a deleted column ID, while it may contain the correct axis id,
     * it is no longer on the axis.  If this is false, then the axis will still
     * be returned even if the column id represents a column no longer on the
     * axis.  If true, both the axis and column must exist.
     * @return Axis containing the column id, or null if the id does not match
     * any columns.
     */
    Axis getAxisFromColumnId(long id, boolean columnMustExist = true)
    {
        long axisId = MathUtil.divide(id, Axis.BASE_AXIS_ID)
        Axis axis = idToAxis.get((Long)axisId)
        if (axis == null)
        {
            return null
        }

        if (columnMustExist)
        {
            return axis.getColumnById(id) != null ? axis : null
        }
        else
        {
            return axis
        }
    }

    /**
     * @return int total number of cells that are uniquely set (non default)
     * within this NCube.
     */
    int getNumCells()
    {
        return cells.size()
    }

    /**
     * @return long number of potential cells this n-cube potentially has
     */
    long getNumPotentialCells()
    {
        long space = 1
        for (axis in axisList.values())
        {
            space *= axis.size()
        }
        return space
    }

    /**
     * @return read-only copy of the n-cube cells.
     */
    Map<Set<Long>, T> getCellMap()
    {
        return Collections.unmodifiableMap(cells)
    }

    /**
     * Retrieve an axis (by name) from this NCube.
     * @param axisName String name of Axis to fetch.
     * @return Axis instance requested by name, or null
     * if it does not exist.
     */
    Axis getAxis(final String axisName)
    {
        return axisList.get(axisName)
    }

    /**
     * Add an Axis to this NCube.
     * If the axis has a default column, all cells will be added to the default column.
     * Otherwise, all cells will be cleared.
     * @param axis Axis to add
     */
    void addAxis(final Axis axis)
    {
        String axisName = axis.name
        long startId = 1
        long originalId = axis.id
        while (idToAxis.containsKey(originalId))
        {
            originalId = startId++
        }
        if (originalId != axis.id)
        {
            axis.reindex(originalId)
        }

        if (axisList.containsKey(axisName))
        {
            throw new IllegalArgumentException("An axis with the name '${axisName}' already exists on cube: ${name}")
        }

        for (axe in axisList.values())
        {
            if (axe.id == axis.id)
            {
                throw new IllegalArgumentException("An axis with the id '${axe.id}' already exists on cube: ${name}")
            }
        }

        if (axis.hasDefaultColumn())
        {   // Add default column ID of the new axis to all populated cells, effectively shifting them to the
            // default column on the new axis.
            Collection<Map.Entry<Set<Long>, T>> newCells = new ArrayDeque<>()
            long defaultColumnId = axis.defaultColId
            for (cell in cells)
            {
                Set<Long> cellKey = cell.key
                cellKey.add(defaultColumnId)
                newCells.add(cell)
            }

            cells.clear()
            for (cell in newCells)
            {
                cells.put(cell.key, (T)internValue(cell.value))
            }
        }
        else
        {
            cells.clear()
        }

        axisList.put(axisName, axis)
        idToAxis.put(axis.id, axis)
        clearSha1()
    }

    /**
     * Rename an axis
     * @param oldName String old name
     * @param newName String new name
     */
    void renameAxis(final String oldName, final String newName)
    {
        if (isEmpty(oldName) || isEmpty(newName))
        {
            throw new IllegalArgumentException("Axis name cannot be empty or blank")
        }
        if (getAxis(newName) != null)
        {
            throw new IllegalArgumentException("There is already an axis named '${oldName}' on cube: ${name}")
        }
        final Axis axis = getAxis(oldName)
        if (axis == null)
        {
            throw new IllegalArgumentException("Axis '${oldName}' not on cube: ${name}")
        }
        axisList.remove(oldName)
        axis.name = newName
        axisList.put(newName, axis)
        clearSha1()
    }

    /**
     * Convert a reference axis to a non-reference axis.  'Break the Reference.'
     * @param axisName String name of reference axis to convert.
     */
    void breakAxisReference(final String axisName)
    {
        Axis axis = getAxis(axisName)
        axis.breakReference()
        clearSha1()
    }

    protected void convertAxisToRefAxis(final String axisName, final ApplicationID refAppId, final String refCubeName, final String refAxisName)
    {
        Axis axis = getAxis(axisName)
        if (axis.reference)
        {
            return
        }
        axis.makeReference(refAppId, refCubeName, refAxisName)
        clearSha1()
    }

    protected void convertExistingAxisToRefAxis(final String axisName, final ApplicationID refAppId, final String refCubeName, final String refAxisName)
    {
        if (name == refCubeName && appId == refAppId)
        {
            throw new IllegalArgumentException("Axis cube and reference axis cube must be different, app: ${appId}, cube: ${name}, axis: ${axisName}")
        }
        // copy list of columns before axis changes
        Axis axis = getAxis(axisName)
        if (axis.reference)
        {
            return
        }
        List<Column> oldColumns = axis.columns

        // make copy of the cell map to reference after the axis changes
        Map<Set<Long>, T> cellMapCopy = new CellMap(cellMap)

        Map<String, Object> args = [:]
        args[ReferenceAxisLoader.REF_TENANT] = refAppId.tenant
        args[ReferenceAxisLoader.REF_APP] = refAppId.app
        args[ReferenceAxisLoader.REF_VERSION] = refAppId.version
        args[ReferenceAxisLoader.REF_STATUS] = refAppId.status
        args[ReferenceAxisLoader.REF_BRANCH] = refAppId.branch
        args[ReferenceAxisLoader.REF_CUBE_NAME] = refCubeName  // cube name of the holder of the referring (pointing) axis
        args[ReferenceAxisLoader.REF_AXIS_NAME] = refAxisName    // axis name of the referring axis (the variable that you had missing earlier)
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(name, axisName, args)
        Axis newAxis = new Axis(axisName, axis.id, axis.hasDefaultColumn(), refAxisLoader)

        Map<Long, Long> oldToNewId = [:]
        for (Column oldCol : oldColumns)
        {   // Locate columns in O(1) to O(log n)
            // Use value that exists on OLD column to locate NEW column
            Column column = newAxis.findColumn(axis.getValueToLocateColumn(oldCol))
            if (column != null)
            {
                oldToNewId[oldCol.id] = column.id
            }
        }

        deleteAxis(axisName)
        addAxis(newAxis)

        cells.clear()
        // change cell ids and put back into cube
        for (Map.Entry<Set<Long>, T> entry : cellMapCopy.entrySet())
        {
            Set<Long> coord = entry.key
            // change coord to have existing ref ax value
            for (long oldCoordPart : coord)
            {
                Long newCoordPart = oldToNewId[oldCoordPart]
                if (newCoordPart)
                {
                    coord.remove(oldCoordPart)
                    coord.add(newCoordPart)
                }
            }
            
            cells.put(coord, (T) internValue(entry.value))
        }

        // Eliminate orphans, where source axis (A, B, C, D, E) pointed to existing ref axis (A, C, E).
        // Cells in columns B & D must be dropped!
        Iterator<Set<Long>> i = cells.keySet().iterator()
        while (i.hasNext())
        {
            Set<Long> ids = i.next()
            Iterator<Long> j = ids.iterator()

            while (j.hasNext())
            {
                long id = j.next()
                if (getAxisFromColumnId(id) == null)
                {
                    i.remove()
                }
            }
        }

        // clearSha1() // called by other APIs [deleteAxis(), addAxis()]
    }

    /**
     * Remove transform from a reference axis.
     * @param axisName String name of reference axis.
     */
    void removeAxisReferenceTransform(final String axisName)
    {
        Axis axis = getAxis(axisName)
        axis.removeTransform()
        clearSha1()
    }

    /**
     * Remove an axis from an NCube.
     * All cells will be cleared when an axis is deleted.
     * @param axisName String name of axis to remove
     * @return boolean true if removed, false otherwise
     */
    boolean deleteAxis(final String axisName)
    {
        Axis axis = axisList[axisName]
        if (!axis)
        {
            return false
        }
        cells.clear()
        clearSha1()
        idToAxis.remove(axis.id)
        return axisList.remove(axisName) != null
    }

    /**
     * @return int the number of axis (dimensions) for this n-cube.
     */
    int getNumDimensions()
    {
        return axisList.size()
    }

    /**
     * @return List<Axis> a List of all axis within this n-cube.
     */
    List<Axis> getAxes()
    {
        return new ArrayList<>(axisList.values())
    }

    /**
     * @return Set<String> containing all axis names on this n-cube.
     */
    Set<String> getAxisNames()
    {
        return new CaseInsensitiveSet<>(axisList.keySet())
    }

    /**
     * Get the optional scope keys. These are keys that if supplied, might change the returned value, but if not
     * supplied a value is still returned.  For example, an axis that has a Default column is an optional scope.
     * If no value is supplied for that axis, the Default column is chosen.  However, supplying a value for it
     * *may* change the column selected.  Similarly, a cube may reference another cube, and the 'sub-cube' may
     * use different scope keys than the calling cube.  These additional keys are located and added as optional
     * scope.
     *
     * @return Set of String scope key names that are optional.
     */
    Set<String> getOptionalScope(Map input, Map output)
    {
        final Set<String> optionalScope = new CaseInsensitiveSet<>()

        for (axis in axisList.values())
        {   // Use original axis name (not .toLowerCase() version)
            if (axis.hasDefaultColumn() || axis.type == AxisType.RULE)
            {   // Rule axis is always optional scope - it does not need a axisName to value binding like the other axis types.
                optionalScope.add(axis.name)
            }
        }

        Collection<String> declaredOptionalScope = (Collection<String>) extractMetaPropertyValue(getMetaProperty(NCubeConstants.OPTIONAL_SCOPE), input, output)
        optionalScope.addAll(declaredOptionalScope == null ? new CaseInsensitiveSet<String>() : new CaseInsensitiveSet<String>(declaredOptionalScope) as Collection<? extends String>)
        return optionalScope
    }

    /**
     * Determine the required 'scope' needed to access all cells within this
     * NCube.  Effectively, you are determining how many axis names (keys in
     * a Map coordinate) are required to be able to access any cell within this
     * NCube.  Keep in mind, that CommandCells allow this NCube to reference
     * other NCubes and therefore the referenced NCubes must be checked as
     * well.  This code will not get stuck in an infinite loop if one cube
     * has cells that reference another cube, and it has cells that reference
     * back (it has cycle detection).
     * @return Set<String> names of axes that will need to be in an input coordinate
     * in order to use all cells within this NCube.
     */
    Set<String> getRequiredScope(Map input, Map output)
    {
        final Set<String> requiredScope = requiredAxes
        requiredScope.addAll(getDeclaredScope(input, output))
        return requiredScope
    }

    /**
     * @return Set<String> Axis names that do not have default columns.  Note, a RULE axis name will never be included
     * as a required Axis, even if it does not have a default column.  This is because you typically do not bind a
     * value to a rule axis name, but instead the columns on the rule axis execute in order.
     */
    protected Set<String> getRequiredAxes()
    {
        final Set<String> required = new CaseInsensitiveSet<>()

        for (axis in axisList.values())
        {   // Use original axis name (not .toLowerCase() version)
            if (!axis.hasDefaultColumn() && !(AxisType.RULE == axis.type))
            {
                required.add(axis.name)
            }
        }
        return required
    }

    /**
     * Get the declared required scope keys for this n-cube.  These keys are required to
     * 'talk' to this cube and any of it's subordinate cubes.  Note that the keys can be
     * a list of Strings of an expression (which could join another n-cube).
     * @param input Map containing input
     * @param output Map for writing output.
     * @return Set<String> required scope keys.
     */
    protected Set<String> getDeclaredScope(Map input, Map output)
    {
        if (!metaProps.containsKey(NCubeConstants.REQUIRED_SCOPE))
        {
            return new CaseInsensitiveSet<>()
        }
        Object value = metaProps.get(NCubeConstants.REQUIRED_SCOPE)
        Collection<String> declaredRequiredScope = (Collection<String>) extractMetaPropertyValue(value, input, output)
        return declaredRequiredScope == null ? new CaseInsensitiveSet<String>() : new CaseInsensitiveSet<>(declaredRequiredScope)
    }

    /**
     * @return Map&lt;Map, Set&lt;String&gt;&gt; A map keyed by cell coordinates within this specific NCube.
     * Each map entry contains the cube names referenced by the cell coordinate in question.
     * It is not recursive.
     */
    Map<Map, Set<String>> getReferencedCubeNames()
    {
        Map<Map, Set<String>> refs = new LinkedHashMap<>()

        cells.each { Set<Long> ids, T cell ->
            if (cell instanceof CommandCell)
            {
                Map<String, Object> coord = getDisplayCoordinateFromIds(ids)
                getReferences(refs, coord, (CommandCell)cell)
            }
        }

        for (axis in axes)
        {
            if (axis.type == AxisType.RULE)
            {
                for (column in axis.columnsWithoutDefault)
                {
                    Map<String, Object> coord = getDisplayCoordinateFromIds([column.id] as Set<Long>)
                    getReferences(refs, coord, column.value as CommandCell)
                }
            }

            for (column in axis.columns)
            {
                Object defaultValue = column.metaProperties[Column.DEFAULT_VALUE]
                if (defaultValue instanceof CommandCell)
                {
                    Map<String, Object> coord = getDisplayCoordinateFromIds([column.id] as Set<Long>)
                    getReferences(refs, coord, defaultValue as CommandCell)
                }
            }
        }

        // If the DefaultCellValue references another n-cube, add it into the dependency list.
        if (defaultCellValue instanceof CommandCell)
        {
            getReferences(refs, [:], (CommandCell)defaultCellValue)
        }
        return refs
    }

    private static Map<Map, Set<String>> getReferences(Map<Map, Set<String>> refs, Map coord, CommandCell cmdCell)
    {
        final Set<String> cubeNames = new CaseInsensitiveSet<>()
        cmdCell.getCubeNamesFromCommandText(cubeNames)
        if (cubeNames)
        {
            refs[coord] = cubeNames
        }
        return refs
    }

    /**
     * Use this API to generate an HTML view of this NCube.
     * @param headers String list of axis names to place at top.  If more than one is listed, the first axis encountered that
     * matches one of the passed in headers, will be the axis chosen to be displayed at the top.
     * @return String containing an HTML view of this NCube.
     */
    String toHtml(String ... headers)
    {
        return new HtmlFormatter(headers).format(this)
    }

    /**
     * Format this n-cube into JSON using the passed in 'options' Map to control the desired output.
     * See the JsonFormatter's format() API for available options.
     * @return String JSON representing this entire n-cube, in the format controlled by the passed
     * in options Map.
     */
    String toFormattedJson(Map options = null)
    {
        return new JsonFormatter().format(this, options)
    }

    String toString()
    {
        return toFormattedJson()
    }

    // ----------------------------
    // Overall cube management APIs
    // ----------------------------

    /**
     * @param ncube NCube to be formatted
     * @param options Map containing various formatting options.  Valid options, listed in
     * (String key : String value) format, mode: html, mode: index, mode: pretty, mode: nocells.
     * 'html' mode is a visual, nice excel-like view.  'index' mode is a different format where
     * the columns are indexed by name below the axis, as opposed to axis having an array of
     * columns. 'pretty' mode is the original json format but formatted nicely for viewing.
     * 'json' mode is the default, but can be explicitly specified.  'nocells' mode is the original
     * json format but the cells array is empty. The 'pretty' mode can be added to 'index', 'nocells',
     * or 'json' like this 'index-pretty', 'nocells-pretty', or 'json-index'.
     * @return String format, generated from the passed in n-cube, based upon passed in options.
     */
    static String formatCube(NCube ncube, Map options)
    {
        String mode = options.mode
        if ('html' == mode)
        {
            return ncube.toHtml()
        }

        Map formatOptions = [:]
        if (mode.contains('index'))
        {
            formatOptions.indexFormat = true
        }
        if (mode.contains('nocells'))
        {
            formatOptions.nocells = true
        }

        String json = ncube.toFormattedJson(formatOptions)
        if (mode.contains('pretty'))
        {
            return JsonWriter.formatJson(json)
        }
        return json
    }

    /**
     * Create an NCube from the given the passed in Map representing an NCube record.
     * @param record NCubeInfoDto
     * @return NCube created from the passed in Map (record) format of an NCube.
     */
    static NCube createCubeFromRecord(NCubeInfoDto record)
    {
        if (record == null || !record.hasCubeData())
        {
            return null
        }
        NCube ncube = createCubeFromBytes(record.bytes)
        ncube.applicationID = record.applicationID
        if (record.hasTestData())
        {
            ncube.testData = NCubeTestReader.convert(record.testData).toArray()
        }
        return ncube
    }

    /**
     * Use this API to create a single NCube from JSON.
     *
     * @param json Simple JSON format
     * @return NCube instance created from the passed in JSON.  It is
     * not added to the static list of NCubes.  If you want that, call
     * addCube() after creating the NCube with this API.
     */
    static <T> NCube<T> fromSimpleJson(final String json)
    {
        if (isEmpty(json))
        {
            throw new IllegalArgumentException("JSON String cannot be null or empty.")
        }
        InputStream stream = new ByteArrayInputStream(json.getBytes('UTF-8'))
        return fromSimpleJson(stream)
    }

    /**
     * Use this API to create NCubes from JSON.
     *
     * @param stream Simple JSON format
     * @return NCube instance created from the passed in JSON.  It is
     * not added to the static list of NCubes.  If you want that, call
     * addCube() after creating the NCube with this API.
     */
    static <T> NCube<T> fromSimpleJson(final InputStream stream)
    {
        if (stream == null)
        {
            throw new IllegalArgumentException("InputStream cannot be null.")
        }
        try
        {
            JsonParser jsonParser = new JsonFactory().enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS).createParser(stream)
            NCube<T> ncube = parseJson(jsonParser)
            return ncube
        }
        catch (RuntimeException | ThreadDeath e)
        {
            throw e
        }
        catch (Throwable e)
        {
            throw new IllegalStateException("Error reading cube from passed in JSON", e)
        }
    }

    private static final Map<JsonToken, Closure> ncubeToken = [:]
    private static final Map<JsonToken, Closure> axisToken = [:]
    private static final Map<JsonToken, Closure> columnToken = [:]
    private static final Map<Object, Closure> parserValue = [:]
    private static final Map<Object, Closure> ncubeField = [:]
    private static final Map<Object, Closure> axisField = [:]
    private static final Map<Object, Closure> columnField = [:]

    private static final int PARSE_USERID_TO_UNIQUE = 1
    private static final int PARSE_META_PROPERTY = 2
    private static final int PARSE_BASE_AXIS_ID = 3
    private static final int PARSE_NCUBE_PROPS = 4
    private static final int PARSE_AXIS_OBJ = 5
    private static final int PARSE_AXIS_PROPS = 6
    private static final int PARSE_COL_OBJ = 8
    private static final int PARSE_COL_PROPS = 9
    private static final int PARSE_CELL_PROPS = 11
    private static final int PARSE_TEMP_COLS = 12

    static
    {
        Closure fieldClosure = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input, Map fieldMap, String fieldName ->
            Closure method = fieldMap.get(fieldName)
            if (method == null)
            {
                method = fieldMap[(PARSE_META_PROPERTY)]
                state = (Map)method(ncube, state, parser, token, input, fieldName)
            }
            else
            {
                state = (Map)method(ncube, state, parser, token, input)
            }
            return state
        }
        Closure getParserValue = { JsonParser parser, Object token ->
            Closure method = parserValue.get(token)
            if (method == null)
            {
                throw new IllegalStateException("Unexpected parser state, no method for token: ${token}")
            }
            return method(parser)
        }
        parserValue[(JsonToken) JsonToken.VALUE_STRING] = { JsonParser parser -> return parser.text }
        parserValue[(JsonToken) JsonToken.VALUE_TRUE] = { JsonParser parser -> return true }
        parserValue[(JsonToken) JsonToken.VALUE_FALSE] = { JsonParser parser -> return false }
        parserValue[(JsonToken) JsonToken.VALUE_NUMBER_FLOAT] = { JsonParser parser -> return parser.valueAsDouble }
        parserValue[(JsonToken) JsonToken.VALUE_NUMBER_INT] = { JsonParser parser -> return parser.valueAsLong }
        parserValue[(JsonToken) JsonToken.VALUE_NULL] = { JsonParser parser -> return null }
        parserValue[(JsonToken) JsonToken.START_ARRAY] = { JsonParser parser ->
            JsonToken token = parser.nextToken()
            Comparable low = (Comparable)getParserValue(parser, token)
            token = parser.nextToken()
            Comparable high = (Comparable)getParserValue(parser, token)
            token = parser.nextToken()
            if (JsonToken.VALUE_NUMBER_INT == token)
            {
                Object priority = getParserValue(parser, token)
                parser.nextToken()
                return new Range(low, high, convertToInteger(priority))
            }
            else
            {
                return new Range(low, high)
            }
        }
        parserValue['SET_START_ARRAY'] = { JsonParser parser ->
            RangeSet rangeSet = new RangeSet()
            while (true)
            {
                JsonToken token = parser.nextToken()
                if (token == JsonToken.END_ARRAY)
                {
                    break
                }
                rangeSet.add((Comparable)getParserValue(parser, token))
            }
            return rangeSet
        }
        parserValue[(JsonToken) JsonToken.START_OBJECT] = { JsonParser parser ->
            JsonObject jsonObject = new JsonObject()
            while (true)
            {
                JsonToken token = parser.nextToken()
                if (token == JsonToken.END_OBJECT)
                {
                    break
                }
                String fieldName = parser.text
                token = parser.nextToken()
                jsonObject.put(fieldName, getParserValue(parser, token))
            }
            return jsonObject
        }

        ncubeToken[JsonToken.START_OBJECT] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            return state
        }
        ncubeToken[JsonToken.FIELD_NAME] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            String fieldName = parser.text
            token = parser.nextToken()
            return fieldClosure(ncube, state, parser, token, input, ncubeField, fieldName)
        }
        ncubeToken[JsonToken.END_OBJECT] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            if (input.containsKey(DEFAULT_CELL_VALUE) || input.containsKey(DEFAULT_CELL_VALUE_URL))
            {
                Object value = input.get(DEFAULT_CELL_VALUE)
                String defUrl = (String)input.get(DEFAULT_CELL_VALUE_URL)
                String defType = (String)input.get(DEFAULT_CELL_VALUE_TYPE)
                boolean defCache = getBoolean(input, DEFAULT_CELL_VALUE_CACHE)
                ncube.setDefaultCellValue(CellInfo.parseJsonValue(value, defUrl, defType, defCache))
            }
            transformMetaProperties((Map)input.get(PARSE_NCUBE_PROPS))
            ncube.addMetaProperties((Map)input.get(PARSE_NCUBE_PROPS))
            ncube.removeMetaProperty('ruleMode')
            ncube.removeMetaProperty('sha1')
            return state
        }

        ncubeField['ncube'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ncube.name = parser.text
            return state
        }
        ncubeField['axes'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            if (JsonToken.START_ARRAY != token)
            {
                throw new IllegalStateException("Expecting start array '[' for axes but instead found: ${token}")
            }
            return axisToken
        }
        ncubeField['cells'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            if (JsonToken.START_ARRAY != token)
            {
                throw new IllegalStateException("Expecting start array '[' for cells but instead found: ${token}")
            }
            Map userIdToUniqueId = (Map)input.get(PARSE_USERID_TO_UNIQUE)
            while (!parser.closed)
            {
                token = parser.nextToken()
                if (token == JsonToken.START_OBJECT)
                {
                    Set<Long> colIds = null
                    Map keyMapId = null
                    Object value = null
                    String type = null
                    String url = null
                    boolean cache = false

                    token = parser.nextToken()
                    if (JsonToken.FIELD_NAME != token)
                    {
                        throw new IllegalStateException("Expecting field but found: ${token}")
                    }
                    while (!parser.closed)
                    {
                        String fieldName = parser.text
                        if ('id' == fieldName)
                        {
                            token = parser.nextToken()
                            if (JsonToken.START_ARRAY != token)
                            {
                                throw new IllegalStateException("Expecting start array '[' for cell id but instead found: ${token}")
                            }

                            colIds = new LinkedHashSet<>()
                            Object id

                            while (!parser.closed)
                            {
                                token = parser.nextToken()
                                if (JsonToken.VALUE_NUMBER_INT == token)
                                {
                                    id = parser.valueAsLong
                                }
                                else if (JsonToken.END_ARRAY == token)
                                {
                                    parser.nextToken()
                                    break
                                }
                                else if (JsonToken.VALUE_STRING == token)
                                {
                                    id = parser.text
                                }
                                else if (JsonToken.VALUE_NUMBER_FLOAT == token)
                                {
                                    id = parser.valueAsDouble
                                }
                                else
                                {
                                    throw new IllegalStateException("Unexpected token: ${token} when parsing cell ID")
                                }
                                Long mappedId = userIdToUniqueId[id]
                                if (mappedId != null)
                                {
                                    colIds.add(mappedId)
                                }
                            }
                        }
                        else if ('value' == fieldName)
                        {
                            token = parser.nextToken()
                            value = getParserValue(parser, token)
                            token = parser.nextToken()
                        }
                        else if ('type' == fieldName)
                        {
                            parser.nextToken()
                            type = parser.text
                            token = parser.nextToken()
                        }
                        else if (JsonToken.END_OBJECT == token)
                        {
                            Object v = CellInfo.parseJsonValue(value, url, type, cache)

                            if (colIds != null)
                            {
                                try
                                {
                                    ncube.setCellById(v, colIds)
                                }
                                catch (InvalidCoordinateException ignore)
                                {
                                    log.debug("Orphaned cell on n-cube: ${ncube.name}, ids: ${colIds}")
                                }
                            }
                            else
                            {
                                for (entry in keyMapId.entrySet())
                                {
                                    keyMapId[entry.key] = CellInfo.parseJsonValue(entry.value, null, null, false)
                                }
                                try
                                {
                                    ncube.setCell(v, keyMapId)
                                }
                                catch (CoordinateNotFoundException ignore)
                                {
                                    log.debug("Orphaned cell on n-cube: ${ncube.name}, coord: ${keyMapId}")
                                }
                            }
                            break
                        }
                        else if ('url' == fieldName)
                        {
                            parser.nextToken()
                            url = parser.text
                            token = parser.nextToken()
                        }
                        else if ('cache' == fieldName)
                        {
                            parser.nextToken()
                            cache = 'true' == parser.text
                            token = parser.nextToken()
                        }
                        else if ('key' == fieldName)
                        {
                            token = parser.nextToken()
                            if (JsonToken.START_OBJECT != token)
                            {
                                throw new IllegalStateException("Expecting start object '{' for cell key but instead found: ${token}")
                            }
                            keyMapId = new CompactCILinkedMap<>()
                            token = parser.nextToken()
                            while (!parser.closed)
                            {
                                if (JsonToken.FIELD_NAME == token)
                                {
                                    String keyName = parser.text
                                    token = parser.nextToken()
                                    keyMapId[keyName] = getParserValue(parser, token)
                                    token = parser.nextToken()
                                }
                                else if (JsonToken.END_OBJECT == token)
                                {
                                    parser.nextToken()
                                    break
                                }
                                else
                                {
                                    throw new IllegalStateException("Unexpected token: ${token} when parsing cell key")
                                }
                            }
                        }
                        else
                        {
                            throw new IllegalStateException("Unknown field name for cell: ${fieldName}")
                        }
                    }
                }
                else if (token == JsonToken.END_ARRAY)
                {
                    break
                }
                else
                {
                    throw new IllegalStateException("Unexpected token: ${token} when parsing cells")
                }
            }
            return state
        }
        ncubeField[DEFAULT_CELL_VALUE] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            input.put(DEFAULT_CELL_VALUE, getParserValue(parser, token))
            return state
        }
        ncubeField[DEFAULT_CELL_VALUE_TYPE] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            input.put(DEFAULT_CELL_VALUE_TYPE, parser.text)
            return state
        }
        ncubeField[DEFAULT_CELL_VALUE_URL] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            input.put(DEFAULT_CELL_VALUE_URL, parser.text)
            return state
        }
        ncubeField[DEFAULT_CELL_VALUE_CACHE] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            input.put(DEFAULT_CELL_VALUE_CACHE, getParserValue(parser, token))
            return state
        }
        ncubeField[(PARSE_META_PROPERTY)] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input, String fieldName ->
            ((Map)input.get(PARSE_NCUBE_PROPS)).put(fieldName, getParserValue(parser, token))
            return state
        }

        axisToken[JsonToken.START_OBJECT] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            input.put(PARSE_AXIS_OBJ, [:])
            input.put(PARSE_TEMP_COLS, [])
            input.put(PARSE_AXIS_PROPS, [:])
            return state
        }
        axisToken[JsonToken.FIELD_NAME] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            String fieldName = parser.text
            token = parser.nextToken()
            return fieldClosure(ncube, state, parser, token, input, axisField, fieldName)
        }
        axisToken[JsonToken.END_OBJECT] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            Axis axis
            Map axisObj = (Map)input.get(PARSE_AXIS_OBJ)
            Map axisProps = (Map)input.get(PARSE_AXIS_PROPS)
            transformMetaProperties(axisProps)
            AxisType type = (AxisType)axisObj.get('type')
            List<Map> tempColumns = (List<Map>)input.get(PARSE_TEMP_COLS)
            Map<Object, Long> userIdToUniqueId = (Map)input.get(PARSE_USERID_TO_UNIQUE)
            boolean isRef = getBoolean(axisProps, 'isRef')
            String axisName = (String)axisObj.get('name')
            boolean hasDefault = getBoolean(axisObj, 'hasDefault')
            Long axisIdLong = axisObj.get('id')
            long axisId

            if (axisIdLong)
            {
                axisId = axisIdLong
            }
            else
            {    // Older n-cube format with no 'id' on the 'axes' in the JSON
                long idBase = (long)input.get(PARSE_BASE_AXIS_ID)
                axisId = idBase++
                input.put(PARSE_BASE_AXIS_ID, idBase)
            }

            axisProps.remove('id')
            axisProps.remove('name')
            axisProps.remove('isRef')
            axisProps.remove('hasDefault')

            if (isRef)
            {
                ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(ncube.name, axisName, axisProps)
                axis = new Axis(axisName, axisId, hasDefault, refAxisLoader)
                ncube.addAxis(axis)
                for (column in axis.columns)
                {
                    userIdToUniqueId[column.id] = column.id
                }

                moveAxisMetaPropsToDefaultColumn(axis, axisProps)
                if (tempColumns)
                {
                    tempColumns.each { Map column ->
                        Column col = axis.getColumnById((Long)column.get('id'))
                        if (col)
                        {    // skip deleted columns
                            Map columnProps = (Map)column.get(PARSE_COL_PROPS)
                            transformMetaProperties(columnProps)
                            Iterator<Map.Entry> i = columnProps.entrySet().iterator()
                            while (i.hasNext())
                            {
                                Map.Entry<String, Object> entry = i.next()
                                String key = entry.key
                                if ('id' != key)
                                {
                                    col.setMetaProperty(key, entry.value)
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                final int preferredOrder = getLong(axisObj, 'preferredOrder').intValue()
                boolean fireAll = true
                if (axisObj.containsKey('fireAll'))
                {
                    fireAll = getBoolean(axisObj, 'fireAll')
                }

                axis = new Axis(axisName, (AxisType)axisObj['type'], (AxisValueType)axisObj['valueType'], (Boolean)axisObj['hasDefault'], preferredOrder, axisId, fireAll)
                ncube.addAxis(axis)
                axis.clearMetaProperties()
                axis.addMetaProperties(axisProps)

                if (axis.metaProperties.size() < 1)
                {
                    axis.clearMetaProperties()
                }
                else
                {
                    moveAxisMetaPropsToDefaultColumn(axis, axisProps)
                }

                // Temporary - eventually should be removed.  Fixes rule columns with no or non-unique names
                healUnamedRules(type, tempColumns)

                for (Map col : tempColumns)
                {
                    String colType = (String)col.get('type')
                    String colName = (String)col.get('name')
                    String url = (String)col.get('url')
                    Map columnProps = (Map)col.get((PARSE_COL_PROPS))
                    boolean cache = getBoolean(col, 'cache')
                    Column colAdded
                    Object colId = col.get('id')
                    Long suggestedId = (colId instanceof Long) ? (Long)colId: null

                    if (AxisType.DISCRETE.is(type) || AxisType.NEAREST.is(type))
                    {
                        Comparable value = (Comparable)CellInfo.parseJsonValue(col.get('value'), null, colType, false)
                        colAdded = axis.addColumn(value, colName, suggestedId)
                    }
                    else if (AxisType.RANGE.is(type))
                    {
                        Range range = (Range)col.get('value')
                        colAdded = ncube.addColumn(axis.name, range, colName, suggestedId)
                    }
                    else if (AxisType.SET.is(type))
                    {
                        RangeSet rangeSet = (RangeSet)col.get('value')
                        colAdded = ncube.addColumn(axis.name, rangeSet, colName, suggestedId)
                    }
                    else if (AxisType.RULE.is(type))
                    {
                        Object value = (Object)col.get('value')
                        Object cmd = CellInfo.parseJsonValue(value, url, colType, cache)
                        if (!(cmd instanceof CommandCell))
                        {
                            cmd = new GroovyExpression('false', null, cache)
                        }
                        colAdded = ncube.addColumn(axis.name, (CommandCell)cmd, colName, suggestedId)
                    }
                    else
                    {
                        throw new IllegalArgumentException("Unsupported Axis Type '${axisObj.type}' for simple JSON input, axis: ${axisName}, cube: ${ncube.name}")
                    }
                    transformMetaProperties(columnProps)
                    colAdded.addMetaProperties(columnProps)

                    if (colId != null)
                    {
                        userIdToUniqueId.put(colId, colAdded.id)
                    }
                }
            }
            transformMetaProperties(axisProps)
            axis.addMetaProperties(axisProps)
            return state
        }
        axisToken[JsonToken.END_ARRAY] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            return ncubeToken
        }

        axisField['id'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('id', parser.valueAsLong)
            return state
        }
        axisField['name'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('name', parser.text)
            return state
        }
        axisField['type'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('type', AxisType.valueOf(parser.text))
            return state
        }
        axisField['valueType'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('valueType', AxisValueType.valueOf(parser.text))
            return state
        }
        axisField['hasDefault'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('hasDefault', getParserValue(parser, token))
            return state
        }
        axisField['preferredOrder'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('preferredOrder', parser.valueAsInt)
            return state
        }
        axisField['fireAll'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_AXIS_OBJ)).put('fireAll', getParserValue(parser, token))
            return state
        }
        axisField['columns'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            if (JsonToken.START_ARRAY != token)
            {
                throw new IllegalStateException("Expecting start array '[' for columns but instead found: ${token}")
            }
            return columnToken
        }
        axisField[(PARSE_META_PROPERTY)] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input, String fieldName ->
            ((Map)input.get(PARSE_AXIS_PROPS)).put(fieldName, getParserValue(parser, token))
            return state
        }

        columnToken[JsonToken.START_OBJECT] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            input.put(PARSE_COL_OBJ, [:])
            input.put(PARSE_COL_PROPS, [:])
            return state
        }
        columnToken[JsonToken.FIELD_NAME] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            String fieldName = parser.text
            token = parser.nextToken()
            return fieldClosure(ncube, state, parser, token, input, columnField, fieldName)
        }
        columnToken[JsonToken.END_OBJECT] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            Map column = (Map)input.get(PARSE_COL_OBJ)
            // Put column's prop map inside column_obj map so that it is together when tempColumns is processed on end axis
            column.put(PARSE_COL_PROPS, input.get(PARSE_COL_PROPS))
            List<Map> tempColumns = (List)input.get(PARSE_TEMP_COLS)
            tempColumns.add(column)

            if (column.get('value') == null)
            {
                if (column.get('id') == null)
                {
                    Map axisMap = (Map)input.get(PARSE_AXIS_OBJ)
                    throw new IllegalArgumentException("Missing 'value' field on column or it is null, axis: ${axisMap.name}, cube: ${ncube.name}")
                }
                else
                {   // Allows you to skip setting both id and value to the same value.
                    column.put('value', column.get('id'))
                }
            }
            return state
        }
        columnToken[JsonToken.END_ARRAY] = {NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            return axisToken
        }

        columnField['id'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_COL_OBJ)).put('id', getParserValue(parser, token))
            return state
        }
        columnField['value'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            Object key = token
            if (((Map)input.get(PARSE_AXIS_OBJ)).get('type') == AxisType.SET)
            {
                key = "${AxisType.SET}_${token.name()}".toString()
            }
            ((Map)input.get(PARSE_COL_OBJ)).put('value', getParserValue(parser, key))
            return state
        }
        columnField['name'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_COL_OBJ)).put('name', parser.text)
            return state
        }
        columnField['url'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_COL_OBJ)).put('url', parser.text)
            return state
        }
        columnField['type'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_COL_OBJ)).put('type', parser.text)
            return state
        }
        columnField['cache'] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input ->
            ((Map)input.get(PARSE_COL_OBJ)).put('cache', parser.text)
            return state
        }
        columnField[(PARSE_META_PROPERTY)] = { NCube ncube, Map state, JsonParser parser, JsonToken token, Map input, String fieldName ->
            ((Map)input.get(PARSE_COL_PROPS)).put(fieldName, getParserValue(parser, token))
            return state
        }
    }

    private static <T> NCube<T> parseJson(JsonParser parser)
    {
        Map state = ncubeToken
        NCube<T> ncube = new NCube('tmp')
        Map input = [
                (PARSE_BASE_AXIS_ID): 1L,
                (PARSE_USERID_TO_UNIQUE): new CompactCILinkedMap(),
                (PARSE_NCUBE_PROPS): [:],
                (PARSE_AXIS_PROPS): [:],
                (PARSE_COL_PROPS): [:],
                (PARSE_CELL_PROPS): [:]
        ]

        while (!parser.closed)
        {
            JsonToken token = parser.nextToken()
            if (token == null)
            {
                break
            }

            Closure method = state[token]
            if (method == null)
            {
                throw new IllegalStateException("Unexpected token: ${token} while parsing JSON into an NCube.")
            }
            state = (Map)method(ncube, state, parser, token, input)
        }

        parser.close()
        if (ncube.getMetaProperty(DECISION_TABLE))
        {
            new DecisionTable(ncube)
        }
        return ncube
    }
    
    private static void healUnamedRules(AxisType type, List columns)
    {
        if (type != AxisType.RULE)
        {
            return
        }

        int count = 1
        Set<String> names = new CaseInsensitiveSet<>()

        for (Object col : columns)
        {
            Map column = (Map)col
            String name = column.name
            if (!name || names.contains(name))
            {
                MapEntry result = generateRuleName(names, count)
                column.name = result.key
                count = result.value as int
                names.add((String)column.name)
            }
            else
            {
                names.add(name)
            }
        }
    }

    private static MapEntry generateRuleName(Set<String> names, int count)
    {
        String name
        while (names.contains(name = "BR${count++}"))
        {}
        return new MapEntry(name, count)
    }

    /**
     * Snag all meta-properties on Axis that start with Axis.DEFAULT_COLUMN_PREFIX, as this
     * is where the default column's meta properties are stored, and copy them to the default
     * column (if one exists)
     */
    private static void moveAxisMetaPropsToDefaultColumn(Axis axis, Map axisProps = [:])
    {
        Column defCol = axis.defaultColumn
        if (!defCol)
        {
            return
        }
        Iterator<Map.Entry<String, Object>> i = axis.metaProps.entrySet().iterator()
        while (i.hasNext())
        {
            Map.Entry<String, Object> entry = i.next()
            String key = entry.key
            if (key.startsWith(JsonFormatter.DEFAULT_COLUMN_PREFIX))
            {
                defCol.setMetaProperty(key - JsonFormatter.DEFAULT_COLUMN_PREFIX, entry.value)
                i.remove()  // do not leave the column_default_* properties on the Axis
                axisProps.remove(key)
            }
        }
    }

    /**
     * Convert the values on the value side of the Map from JsonObject to an appropriate
     * CellInfo.  If the value is not a JsonObject, it is left alone (primitives).
     * @param props Map of String meta-property keys to values
     */
    protected static void transformMetaProperties(Map props)
    {
        List<MapEntry> entriesToUpdate = []
        for (entry in props.entrySet())
        {
            if (entry.value instanceof JsonObject)
            {
                JsonObject map = (JsonObject) entry.value
                Boolean cache = (Boolean) map['cache']
                Object value = CellInfo.parseJsonValue(map['value'], (String) map['url'], (String) map['type'], cache == null ? false : cache)
                entriesToUpdate.add(new MapEntry(entry.key, value))
            }
            else if (entry.value instanceof CellInfo)
            {
                CellInfo info = (CellInfo)entry.value
                entriesToUpdate.add(new MapEntry(entry.key, info.recreate()))
            }
        }

        for (entry in entriesToUpdate)
        {
            props[entry.key] = entry.value
        }
    }

    protected static Long getLong(Map obj, String key)
    {
        Object val = obj[key]
        if (val instanceof Number)
        {
            return ((Number) val).longValue()
        }
        if (val instanceof String)
        {
            try
            {
                return Long.parseLong((String)val)
            }
            catch(Exception ignored)
            { }
        }
        String clazz = val == null ? 'null' : val.class.name
        throw new IllegalArgumentException("Expected 'Long' for key '${key}' but instead found: ${clazz}")
    }

    protected static boolean getBoolean(Map obj, String key)
    {
        Object val = obj[key]
        if (val instanceof Boolean)
        {
            return (Boolean) val
        }
        if (val instanceof String)
        {
            return 'true'.equalsIgnoreCase((String) val)
        }
        if (val == null)
        {
            return false
        }
        String clazz = val.class.name
        throw new IllegalArgumentException("Expected 'Boolean' for key '${key}' but instead found: ${clazz}")
    }

    /**
     * Create an equivalent n-cube as 'this'.
     */
    NCube duplicate(String newName = name)
    {
        NCube copy = createCubeFromBytes(cubeAsGzipJsonBytes)
        copy.name = newName
        copy.applicationID = this.applicationID
        return copy
    }

    /**
     * Create an 'empty' NCube that matches this NCube, but with no columns on it (same number of axes).
     */
    protected NCube createStubCube()
    {
        NCube stub = duplicate(name)
        stub.axes.each { Axis axis ->
            if (!axis.reference)
            {
                axis.columns.each { Column column ->
                    stub.deleteColumn(axis.name, axis.getValueToLocateColumn(column))
                }
            }
        }
        return stub
    }

    boolean equals(Object other)
    {
        if (!(other instanceof NCube))
        {
            return false
        }

        if (this.is(other))
        {
            return true
        }

        return sha1().equalsIgnoreCase(((NCube) other).sha1())
    }

    int hashCode()
    {
        return name.hashCode()
    }

    void clearSha1()
    {
        sha1 = null
    }

    /**
     * @return List&lt;Map&lt;String, T&gt;&gt; which is a List of coordinates, one for each populated cell within the
     * n-cube. These coordinates can be used with setCell(), removeCell(), containsCell(), and getCellNoExecute()
     * APIs.  To use with getCell(), any rule axis bindings in the coordinate would need to be removed.  This is
     * because it is expected that getCell() will run all conditions on a rule axis.
     */
    List<Map<String, Object>> getPopulatedCellCoordinates()
    {
        List<Map<String, Object>> coords = []
        for (entry in cells.entrySet())
        {
            Set<Long> colIds = entry.key
            Map<String, Object> coord = getCoordinateFromIds(colIds)
            coords.add(coord)
        }

        return coords
    }

    /**
     * @return SHA1 value for this n-cube.  The value is durable in that Axis order and
     * cell order do not affect the SHA1 value.
     */
    String sha1()
    {
        // Check if the SHA1 is already calculated.  If so, return it.
        // In order to cache it successfully, all mutable operations on n-cube must clear the SHA1.
        if (hasContent(sha1))
        {
            return sha1
        }

        Map<String, String> axisNameMap = [:]
        final byte sep = 0
        MessageDigest sha1Digest = SHA1Digest
        sha1Digest.update(name == null ? ''.bytes : name.bytes)
        sha1Digest.update(sep)

        deepSha1(sha1Digest, defaultCellValue, sep)
        Map copy = new TreeMap(metaProperties)
        copy.remove(METAPROPERTY_TEST_DATA)
        deepSha1(sha1Digest, copy, sep)

        // Need deterministic ordering (sorted by Axis name will do that)
        Map<String, Axis> sortedAxes = new TreeMap<>(axisList)
        sha1Digest.update(A_BYTES)       // a=axes
        sha1Digest.update(sep)

        for (entry in sortedAxes.entrySet())
        {
            Axis axis = entry.value
            String axisName = axis.name
            String axisNameLower = axisName.toLowerCase()
            axisNameMap.put(axisName, axisNameLower)
            sha1Digest.update(axisNameLower.bytes)
            sha1Digest.update(sep)
            sha1Digest.update(String.valueOf(axis.columnOrder).bytes)
            sha1Digest.update(sep)
            sha1Digest.update(axis.type.name().bytes)
            sha1Digest.update(sep)
            sha1Digest.update(axis.valueType.name().bytes)
            sha1Digest.update(sep)
            sha1Digest.update(axis.hasDefaultColumn() ? TRUE_BYTES : FALSE_BYTES)
            sha1Digest.update(sep)
            if (!axis.fireAll)
            {   // non-default value, add to SHA1 because it's been changed (backwards sha1 compatible)
                sha1Digest.update(O_BYTES)
                sha1Digest.update(sep)
            }
            if (!MapUtilities.isEmpty(axis.metaProps))
            {
                deepSha1(sha1Digest, new TreeMap<>(axis.metaProps), sep)
            }
            sha1Digest.update(sep)
            boolean displayOrder = axis.columnOrder == Axis.DISPLAY
            if (axis.reference)
            {
                for (column in axis.columns)
                {
                    if (!MapUtilities.isEmpty(column.metaProps))
                    {
                        deepSha1(sha1Digest, column.metaProps, sep)
                    }
                }
            }
            else
            {
                for (column in axis.columnsWithoutDefault)
                {
                    Object v = column.value
                    Object safeVal = (v == null) ? '' : v
                    sha1Digest.update(safeVal.toString().bytes)
                    sha1Digest.update(sep)
                    if (!MapUtilities.isEmpty(column.metaProps))
                    {
                        deepSha1(sha1Digest, column.metaProps, sep)
                    }
                    sha1Digest.update(sep)
                    if (displayOrder)
                    {
                        String order = String.valueOf(column.displayOrder)
                        sha1Digest.update(order.bytes)
                        sha1Digest.update(sep)
                    }
                }

                if (axis.hasDefaultColumn() && !MapUtilities.isEmpty(axis.defaultColumn.metaProperties))
                {
                    deepSha1(sha1Digest, axis.defaultColumn.metaProperties, sep)
                }
            }
        }

        // Deterministic ordering of cell values with coordinates.
        // 1. Build String SHA-1 of coordinate + SHA-1 of cell contents.
        // 2. Combine and then sort.
        // 3. Build SHA-1 from this.
        sha1Digest.update(C_BYTES)  // c = cells
        sha1Digest.update(sep)

        if (numCells)
        {
            List<String> sha1s = new ArrayList<>(cells.size()) as List<String>
            MessageDigest tempDigest = SHA1Digest

            for (entry in cells.entrySet())
            {
                String keySha1 = columnIdsToString(axisNameMap, entry.key)
                deepSha1(tempDigest, entry.value, sep)
                String valueSha1 = encode(tempDigest.digest())
                sha1s.add(calculateSHA1Hash((keySha1 + valueSha1).bytes))
                tempDigest.reset()
            }

            Collections.sort(sha1s)

            for (sha_1 in sha1s)
            {
                sha1Digest.update(sha_1.bytes)
            }
        }
        sha1 = encode(sha1Digest.digest())
        return sha1
    }

    private String columnIdsToString(Map<String, String> axisNameMap, Set<Long> columns)
    {
        List<String> list = new ArrayList(columns.size())
        for (colId in columns)
        {
            Axis axis = getAxisFromColumnId(colId)
            if (axis != null)
            {   // Rare case where a column has an invalid ID.
                Column column = axis.getColumnById(colId)
                Object value = column.columnName ?: column.value
                list.add("${axisNameMap.get(axis.name)}|${value == null ? 'Default' : value.toString()}".toString())
            }
        }
        Collections.sort(list)
        StringBuilder s = new StringBuilder()
        for (str in list)
        {
            s.append(str)
            s.append('|')
        }
        return s.toString()
    }

    private static void deepSha1(MessageDigest md, Object value, byte sep)
    {
        Deque<Object> stack = new LinkedList<>()
        stack.addFirst(value)
        Set<Object> visited = new HashSet<>()

        while (!stack.empty)
        {
            value = stack.removeFirst()
            if (visited.contains(value))
            {
                continue
            }
            visited.add(value)

            if (value == null)
            {
                md.update(NULL_BYTES)
                md.update(sep)
            }
            else if (value.class.array)
            {
                int len = Array.getLength(value)

                md.update(ARRAY_BYTES)
                md.update(String.valueOf(len).bytes)
                md.update(sep)
                for (int i=0; i < len; i++)
                {
                    stack.addFirst(Array.get(value, i))
                }
            }
            else if (value instanceof Collection)
            {
                Collection col = (Collection) value
                md.update(COL_BYTES)
                md.update(String.valueOf(col.size()).bytes)
                md.update(sep)
                stack.addAll(col)
            }
            else if (value instanceof Map)
            {
                Map map = (Map)value
                md.update(MAP_BYTES)
                md.update(String.valueOf(map.size()).bytes)
                md.update(sep)

                for (entry in map.entrySet())
                {
                    stack.addFirst(entry.value)
                    stack.addFirst(entry.key)
                }
            }
            else
            {
                if (value instanceof String)
                {
                    md.update(((String)value).bytes)
                    md.update(sep)
                }
                else if (value instanceof CommandCell)
                {
                    CommandCell cmdCell = (CommandCell)value
                    md.update(cmdCell.class.name.bytes)
                    md.update(sep)
                    if (cmdCell.url != null)
                    {
                        md.update(cmdCell.url.bytes)
                        md.update(sep)
                    }
                    if (cmdCell.cmd != null)
                    {
                        md.update(cmdCell.cmd.bytes)
                        md.update(sep)
                    }
                    md.update(cmdCell.url != null ? TRUE_BYTES : FALSE_BYTES)  // t (url) or f (no url)
                    md.update(sep)
                    md.update(cmdCell.cacheable ? TRUE_BYTES : FALSE_BYTES)
                    md.update(sep)
                }
                else
                {
                    String strKey = value.toString()
                    if (strKey.contains('@'))
                    {
                        md.update(toJson(value).bytes)
                    }
                    else
                    {
                        md.update(strKey.bytes)
                    }
                }
                md.update(sep)
            }
        }
    }

    /**
     * Test if another n-cube is 'comparable' with this n-cube.  This means that they have the same number of
     * dimensions (axes) and each axis has the same name.  This test will allow many operations to
     * be performed on two cubes once it is known they are 'compatible' such as union, intersection, even matrix
     * operations like multiply, etc.
     * @param other NCube to compare to this ncube.
     * @return boolean true if the passed in cube has the same number of axes, the axes have the same names, otherwise
     * false.
     */
    boolean isComparableCube(NCube<T> other)
    {
        if (numDimensions != other.numDimensions)
        {   // Must have same dimensionality
            return false
        }

        Set<String> a1 = new CaseInsensitiveSet<>(axisList.keySet())
        Set<String> a2 = new CaseInsensitiveSet<>(other.axisList.keySet())
        a1.removeAll(a2)

        if (!a1.empty)
        {   // Axis names must be all be the same (ignoring case)
            return false
        }

        for (axis in axisList.values())
        {
            Axis otherAxis = other.getAxis(axis.name)
            if (axis.type != otherAxis.type)
            {   // Axes must be same type (DISCRETE, RANGE, SET, NEAREST, or RULE)
                return false
            }
            if (axis.valueType != otherAxis.valueType)
            {   // Axes must be same value type (LONG, DOUBLE, DATE, etc.)
                return false
            }
        }

        return true
    }

    /**
     * Merge the passed in List of Delta's into this n-cube.
     * @param deltas List of Delta instances.
     */
    void mergeDeltas(List<Delta> deltas)
    {
        List<Delta> columnReorders = []
        for (Delta delta : deltas)
        {
            if (delta.location == Delta.Location.NCUBE)
            {
                if ('NAME'.equals(delta.locId))
                {
                    name = delta.destVal
                }
                else if ('DEFAULT_CELL'.equals(delta.locId))
                {
                    CellInfo cellInfo = delta.destVal as CellInfo
                    Object cellValue = cellInfo.isUrl ?
                            CellInfo.parseJsonValue(null, cellInfo.value, cellInfo.dataType, cellInfo.isCached) :
                            CellInfo.parseJsonValue(cellInfo.value, null, cellInfo.dataType, cellInfo.isCached)
                    setDefaultCellValue((T) cellValue)
                }
            }
            else if (delta.location == Delta.Location.CELL)
            {
                Set<Long> coords = delta.locId as Set<Long>
                if (delta.type == Delta.Type.ADD || delta.type == Delta.Type.UPDATE)
                {
                    setCellById((T) (delta.destVal as CellInfo).recreate(), coords)
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    removeCellById(coords)
                }
            }
            else if (delta.location == Delta.Location.NCUBE_META)
            {
                if (delta.type == Delta.Type.ADD || delta.type == Delta.Type.UPDATE)
                {
                    MapEntry entry = delta.destVal as MapEntry
                    setMetaProperty(entry.key as String, entry.value)
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    MapEntry entry = delta.sourceVal as MapEntry
                    removeMetaProperty(entry.key as String)
                }
            }
            else if (delta.location == Delta.Location.AXIS)
            {
                Axis receiverAxis = delta.sourceVal as Axis
                Axis transmitterAxis = delta.destVal as Axis

                if (delta.type == Delta.Type.ADD)
                {
                    if (getAxis(transmitterAxis.name) == null)
                    {   // Only add if not already there.
                        addAxis(transmitterAxis)
                    }
                }
                else if (delta.type == Delta.Type.UPDATE)
                {
                    if (receiverAxis)
                    {
                        receiverAxis.columnOrder = transmitterAxis.columnOrder
                        receiverAxis.fireAll = transmitterAxis.fireAll
                    }
                }
                if (delta.type == Delta.Type.DELETE)
                {
                    deleteAxis(receiverAxis.name)
                }
            }
            else if (delta.location == Delta.Location.AXIS_META)
            {
                Axis axis = getAxis(delta.locId as String)
                if (!axis)
                {
                    break
                }

                if (delta.type == Delta.Type.ADD || delta.type == Delta.Type.UPDATE)
                {
                    MapEntry entry = delta.destVal as MapEntry
                    axis.setMetaProperty(entry.key as String, entry.value)
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    MapEntry entry = delta.sourceVal as MapEntry
                    axis.removeMetaProperty(entry.key as String)
                }
            }
            else if (delta.location == Delta.Location.COLUMN)
            {
                String axisName = delta.locId as String
                Axis axis = getAxis(axisName)
                if (!axis)
                {   // axis not found
                    break
                }

                if (delta.type == Delta.Type.ADD)
                {
                    Column column = delta.destVal as Column
                    if (column.default)
                    {
                        if (!axis.hasDefaultColumn())
                        {
                            addColumn(axisName, null, column.columnName)
                        }
                    }
                    else
                    {
                        Column existingCol = axis.locateDeltaColumn(column)
                        if (!existingCol || existingCol.default)
                        {   // Only add column if it is not already there
                            addColumn(axisName, column.value, column.columnName, column.id)
                        }
                    }
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    Column column = delta.sourceVal as Column
                    Column existingCol = axis.locateDeltaColumn(column)
                    if (axis.type == AxisType.RULE)
                    {
                        deleteColumn(axisName, existingCol.columnName ?: existingCol.id as Long)
                    }
                    else
                    {
                        deleteColumn(axisName, existingCol.value)
                    }
                }
                else if (delta.type == Delta.Type.UPDATE)
                {
                    Column oldCol = delta.sourceVal as Column
                    Column newCol = delta.destVal as Column
                    Column existingCol = axis.locateDeltaColumn(oldCol)
                    if (existingCol)
                    {
                        updateColumn(existingCol.id, newCol.value)
                    }
                }
                else if (delta.type == Delta.Type.ORDER)
                {
                    columnReorders.add(delta)
                }
            }
            else if (delta.location == Delta.Location.COLUMN_META)
            {
                Map<String, Object> helperId = delta.locId as Map<String, Object>
                Axis axis = getAxis(helperId.axis as String)
                if (!axis)
                {
                    break
                }
                Column column = axis.locateDeltaColumn(helperId.column as Column)
                if (!column)
                {
                    break
                }
                MapEntry oldPair = delta.sourceVal as MapEntry
                MapEntry newPair = delta.destVal as MapEntry

                if (delta.type == Delta.Type.ADD || delta.type == Delta.Type.UPDATE)
                {
                    column.setMetaProperty(newPair.key as String, newPair.value)
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    column.removeMetaProperty(oldPair.key as String)
                }
            }
            else if (delta.location == Delta.Location.CELL_META)
            {
                // TODO - cell meta-properties not yet implemented
            }
            else if (delta.location == Delta.Location.TEST)
            {
                List<NCubeTest> tests = delta.sourceList.toList() as List<NCubeTest>

                if (delta.type == Delta.Type.ADD)
                {
                    tests.add(delta.destVal as NCubeTest)
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    String testName = delta.locId as String
                    NCubeTest test = tests.find { NCubeTest cubeTest -> cubeTest.name == testName }
                    tests.remove(test)
                }
                testData = tests.toArray()
            }
            else if (delta.location == Delta.Location.TEST_COORD)
            {
                List<NCubeTest> tests = delta.sourceList.toList() as List<NCubeTest>
                String testName = delta.locId as String
                NCubeTest test = tests.find { NCubeTest cubeTest -> cubeTest.name == testName }
                Map<String, CellInfo> coords = test.coord

                if (delta.type == Delta.Type.ADD || delta.type == Delta.Type.UPDATE)
                {
                    Map.Entry<String, CellInfo> newCoordEntry = delta.destVal as Map.Entry<String, CellInfo>
                    coords[newCoordEntry.key] = newCoordEntry.value
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    Map.Entry<String, CellInfo> oldCoordEntry = delta.sourceVal as Map.Entry<String, CellInfo>
                    coords.remove(oldCoordEntry.key)
                }

                NCubeTest newTest = new NCubeTest(test.name, coords, test.assertions)
                tests.remove(test)
                tests.add(newTest)
                testData = tests.toArray()
            }
            else if (delta.location == Delta.Location.TEST_ASSERT)
            {
                List<NCubeTest> tests = delta.sourceList.toList() as List<NCubeTest>
                String testName = delta.locId as String
                NCubeTest test = tests.find { NCubeTest cubeTest -> cubeTest.name == testName }
                List<CellInfo> assertions = test.assertions.toList()
                CellInfo newAssert = delta.destVal as CellInfo
                CellInfo oldAssert = delta.sourceVal as CellInfo

                if (delta.type == Delta.Type.ADD)
                {
                    assertions.add(newAssert)
                }
                else if (delta.type == Delta.Type.UPDATE)
                {
                    assertions.remove(oldAssert)
                    assertions.add(newAssert)
                }
                else if (delta.type == Delta.Type.DELETE)
                {
                    assertions.remove(oldAssert)
                }

                NCubeTest newTest = new NCubeTest(test.name, test.coord, assertions.toArray() as CellInfo[])
                tests.remove(test)
                tests.add(newTest)
                testData = tests.toArray()
            }
        }

        for (Delta delta : columnReorders)
        {
            String axisName = delta.locId as String
            if (axisName)
            {
                Axis axis = getAxis(delta.locId as String)
                if (axis)
                {
                    Set<Column> updatedCols = []
                    Set<Column> oldCols = delta.destVal as Set<Column>
                    for (Column oldCol : oldCols)
                    {
                        Column newCol = axis.getColumnById(oldCol.id)
                        if (newCol)
                        {
                            newCol.displayOrder = oldCol.displayOrder
                            updatedCols.add(newCol)
                        }
                    }
                    updateColumns(axisName, updatedCols, true)
                }
            }
        }

        clearSha1()
    }

    List<NCubeTest> getTestData()
    {
        NCubeTestReader.convert(getMetaProperty(METAPROPERTY_TEST_DATA) as String)
    }

    void setTestData(Object[] tests)
    {
        setMetaProperty(METAPROPERTY_TEST_DATA, new NCubeTestWriter().format(tests))
    }

    /**
     * Fetch a 'display' coordinate from the passed in Set of IDs.  The returned coordinate
     * will have the String 'default column' for column ids that represent default columns.
     * If any of the columns have a name (like columns on a rule axis), the name will be
     * included in the String associated to the axis name entry.
     * @param idCoord Set&lt;Long&gt; column IDs.
     * @return Map&lt;String, T&gt; where the keys of the Map are axis names, and the values
     * are the associated values that would bind to the axes, except for default column
     * and rule axes.  This API is typically used for display purposes.
     */
    Map<String, Object> getDisplayCoordinateFromIds(Set<Long> idCoord)
    {
        Map<String, Object> properCoord = new CompactCILinkedMap<>()
        for (colId in idCoord)
        {
            Axis axis = getAxisFromColumnId(colId)
            if (axis == null)
            {
                continue
            }
            Column column = axis.getColumnById(colId)
            Object value = column.valueThatMatches
            if (value == null)
            {
                value = "default column"
            }

            String name = column.columnName
            if (name != null)
            {
                properCoord[axis.name] = "(${name.toString()}): ${value}"
            }
            else
            {
                properCoord[axis.name] = value
            }
        }
        return properCoord
    }

    /**
     * Turn a Set of column IDs into a 'normal' coordinate that has values that will
     * bind to axes the 'normal' way.  For RULE axes, the key will be the rule axis name
     * and the value will be the rule name.  If there is no rule name, then the value will
     * be the long ID of the column on the rule axis.
     * @param idCoord Set<Long> ids that describe a binding to a cell
     * @return Map standard coordinate.  Note that this coordinate has an
     * entry for a rule axis, which typically would NOT be used for a call to getCell(),
     * as normally rule axes have no binding (so all conditions on the rule axis execute).
     * The returned coordinate, however, is useful for setCell(), removeCell(), containsCell(),
     * and getCellNoExecute() APIs.  To use with getCell(), remove the entry or entries
     * that have rule axis names.
     */
    Map<String, Object> getCoordinateFromIds(Set<Long> idCoord)
    {
        Map<String, Object> coord = new CompactCILinkedMap<>()
        for (colId in idCoord)
        {
            Axis axis = getAxisFromColumnId(colId)
            if (axis == null)
            {
                continue
            }
            Column column = axis.getColumnById(colId)
            coord[axis.name] = axis.getValueToLocateColumn(column)
        }
        return coord
    }

    /**
     * Determine highest ID used by the axes.  When adding an additional axis, it is recommended to use this value
     * plus 1 for adding another axis.
     * @return long the highest ID used by any axis.
     */
    long getMaxAxisId()
    {
        long max = 0
        for (axis in axisList.values())
        {
            long axisId = axis.id
            if (axisId > max)
            {
                max = axisId
            }
        }
        return max
    }

    protected static String toJson(Object o)
    {
        if (o == null)
        {
            return "null"
        }
        try
        {
            return JsonWriter.objectToJson(o)
        }
        catch (Exception ignore)
        {
            throw new IllegalStateException("Unable to convert value to JSON: ${o.toString()}")
        }
    }

    /**
     * Ensure that the passed in name is a valid n-cube name,
     * @param cubeName String cube name to test.
     * @throws IllegalArgumentException if the name is invalid, otherwise it is silent.
     */
    static void validateCubeName(String cubeName)
    {
        if (isEmpty(cubeName))
        {
            throw new IllegalArgumentException("n-cube name cannot be null or empty")
        }

        Matcher m = Regexes.validCubeName.matcher(cubeName)
        if (m.matches())
        {
            return
        }
        throw new IllegalArgumentException("Invalid n-cube name: '${cubeName}'. Name can only contain a-z, A-Z, 0-9, '.', '_', '-'")
    }

    /**
     * Create a cube from a byte[] of JSON bytes, or a gzip byte[] of JSON bytes, both
     * are JSON content representing an n-cube.  Calling ncube.toFormattedJson() is the source
     * of the JSON format used.
     */
    static <T> NCube<T> createCubeFromBytes(byte[] bytes, int pos = 0, int length = bytes.length)
    {
        return createCubeFromStream(new ByteArrayInputStream(bytes, pos, length))
    }

    /**
     * Create an n-cube from a stream of bytes.  The stream can be either a JSON stream
     * of an n-cube or a g-zip JSON stream.
     */
    static <T> NCube<T> createCubeFromStream(InputStream stream)
    {
        try
        {
            return fromSimpleJson(rawStreamToInputStream(stream))
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Error reading cube from stream.", e)
        }
        finally
        {
            close(stream)
        }
    }

    static protected InputStream rawStreamToInputStream(InputStream stream)
    {
        if (stream == null)
        {
            throw new IllegalArgumentException("Stream cannot be null to create cube.")
        }

        byte[] header = new byte[2]
        InputStream newStream = new BufferedInputStream(stream)
        newStream.mark(5)

        int count = newStream.read(header)
        if (count < 2)
        {
            throw new IllegalStateException("Invalid cube existing of 0 or 1 bytes")
        }

        newStream.reset()
        return ByteUtilities.isGzipped(header) ? new GZIPInputStream(newStream) : newStream
    }

    /**
     * @return byte[] containing the bytes of this N-Cube when converted to JSON format and then gzipped.
     */
    byte[] getCubeAsGzipJsonBytes()
    {
        FastByteArrayOutputStream byteOut = new FastByteArrayOutputStream()
        OutputStream gzipOut = null

        try
        {
            gzipOut = new AdjustableGZIPOutputStream(byteOut, 8192, Deflater.BEST_SPEED)
            new JsonFormatter(gzipOut).formatCube(this, null)
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Error writing cube to stream", e)
        }
        finally
        {
            close(gzipOut)
        }
        return byteOut.toByteArrayUnsafe()
    }

    /**
     * Set the name of this n-cube
     * @param name String name
     */
    void setName(String name)
    {
        this.name = name
        clearSha1()
    }

    /**
     * This API allows Groovy code to do this:
     * ncube.axisName or ncube['axisName'] to fetch an axis.
     * @param axisName String name of axis to get (case ignored)
     * @return Axis if found, null otherwise.
     */
    @Deprecated
    Axis get(String axisName)
    {
        return axisList[axisName]
    }

    /**
     * Intern the passed in value.  Collapses (folds) equivalent instances into same instance.
     * @param value Object to intern (if possible)
     * @return interned instance (if internable) otherwise passed-in instance is returned.
     */
    private static Object internValue(Object value)
    {
        if (value == null)
        {
            return null
        }

        if (!isLogicalPrimitive(value.class) && !(value instanceof RangeSet) && !(value instanceof Range))
        {   // don't attempt to intern null (NPE) or non-primitive instances
            return value
        }

        if (primitives.containsKey(value))
        {   // intern it (re-use instance)
            return primitives.get(value)
        }

        Object singletonInstance = primitives.putIfAbsent(value, value)
        if (singletonInstance != null)
        {
            return singletonInstance
        }
        return value
    }

    /**
     * Sets the max size for coordinate value strings in StackEntry.
     * @param maxSize int
     */
    static void setStackEntryCoordinateValueMaxSize(int maxSize)
    {
        stackEntryCoordinateValueMaxSize = maxSize
    }

    /**
     * Unload any dynamically generated classes.  This looks at not only cells that may be GroovyExpressions,
     * but also rule Columns, and metaProperties [value-side] for ncube, axis, and column, which can also contain
     * GroovyExpressions.
     */
    void unloadAnyGeneratedClasses()
    {
        cellMap.each { ids, cell ->
            if (cell instanceof GroovyBase) {
                cell.clearClassLoaderCache(appId)
            }
        }

        metaProps.each { key, value ->
            if (value instanceof GroovyBase) {
                value.clearClassLoaderCache(appId)
            }
        }

        axes.each { Axis axis ->
            axis.columns.each { Column column ->
                if (column.value instanceof GroovyBase) {
                    ((GroovyBase)column.value).clearClassLoaderCache(appId)
                }

                column.metaProperties.each { key, value ->
                    if (value instanceof GroovyBase) {
                        ((GroovyBase)value).clearClassLoaderCache(appId)
                    }
                }
            }

            axis.metaProperties.each { key, value ->
                if (value instanceof GroovyBase) {
                    ((GroovyBase)value).clearClassLoaderCache(appId)
                }
            }
        }
    }

    DecisionTable getDecisionTable()
    {
        setMetaProperty(DECISION_TABLE, true)
        return new DecisionTable(this)
    }
}
