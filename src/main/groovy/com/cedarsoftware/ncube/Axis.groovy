package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.AxisOverlapException
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.MapUtilities
import com.cedarsoftware.util.io.JsonReader
import com.google.common.collect.RangeMap
import com.google.common.collect.TreeRangeMap
import gnu.trove.TLongObjectHashMap
import groovy.transform.CompileStatic

import java.security.SecureRandom
import java.util.regex.Matcher

import static com.cedarsoftware.ncube.ReferenceAxisLoader.*
import static com.cedarsoftware.util.Converter.convertToBigDecimal
import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.Converter.convertToDouble
import static com.cedarsoftware.util.Converter.convertToLong
import static com.cedarsoftware.util.Converter.convertToString
import static com.cedarsoftware.util.EncryptionUtilities.calculateSHA1Hash
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.StringUtilities.isEmpty
import static java.lang.Math.abs

/**
 * Implements an Axis of an NCube. When modeling, think of an axis as a 'condition'
 * or decision point.  An input variable (like 'X:1' in a cartesian coordinate system)
 * is passed in, and the Axis's job is to locate the column that best matches the input,
 * as quickly as possible.<pre>
 *
 * Five types of axes are supported, DISCRETE, RANGE, SET, NEAREST, and RULE.
 * DISCRETE matches discrete values with .equals().  Locates items in O(1)
 * RANGE matches [low, high) values in O(Log n) time.
 * SET matches repeating DISCRETE and RANGE values in O(Log n) time.
 * NEAREST finds the column matching the closest value to the input.  Runs in O(Log n) for
 * Number and Date types, O(n) for String, Point2D, Point3D, LatLon.
 * RULE fires all conditions that evaluate to true.  Runs in O(n).</pre>
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
@CompileStatic
class Axis
{
    public static final String DONT_CARE = '_︿_ψ_☼'
    public static final int SORTED = 0
    public static final int DISPLAY = 1
    protected static final long BASE_AXIS_ID = 1000000000000L
    protected static final long MAX_COLUMN_ID = 2000000000L

    private String name
    private AxisType type
    private AxisValueType valueType
    protected Map<String, Object> metaProps = null
    private Column defaultCol
    protected long id
    private int preferredOrder = SORTED
    protected boolean fireAll = true
    private boolean isRef

    // Internal indexes
    private final transient TLongObjectHashMap<Column> idToCol = new TLongObjectHashMap<>()
    private final transient Map<String, Column> colNameToCol = new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
    private final transient SortedMap<Integer, Column> displayOrder = new TreeMap<>()
    private transient Map<Comparable, Column> valueToCol
    protected transient RangeMap<Comparable, Column> rangeToCol = TreeRangeMap.create()

    private static final ThreadLocal<Random> LOCAL_RANDOM = new ThreadLocal<Random>() {
        Random initialValue()
        {
            String s = convertToString(System.nanoTime())
            s = calculateSHA1Hash(s.bytes)
            return new SecureRandom(s.bytes)
        }
    }

    /**
     * Implement to provide data for this Axis
     */
    static interface AxisRefProvider
    {
        void load(Axis axis)
    }

    // for construction during serialization
    private Axis() { id=0 }

    /**
     * Use this constructor for non-rule Axes.
     * @param name String Axis name
     * @param type AxisType (DISCRETE, RANGE, SET, NEAREST, RULE)
     * @param valueType AxisValueType (STRING, LONG, BIG_DECIMAL, DOUBLE, DATE, EXPRESSION, COMPARABLE)
     * @param hasDefault boolean set to true to have a Default column that will match when no other columns match
     * @param order SORTED or DISPLAY (insertion order)
     * @param id long id of Axis.  Ask n-cube for max ID, then add 1 to it, and use that.
     * @param fireAll boolean if set to true, all conditions that evaluate to true will have their associated
     * statements executed.  If set to false, the first condition that evaluates to true will be executed, but
     * then no conditions on the RULE axis will be evaluated.
     */
    Axis(String name, AxisType type, AxisValueType valueType, boolean hasDefault, int order = SORTED, long id = 1, boolean fireAll = true)
    {
        isRef = false
        this.id = id
        this.name = name
        this.type = type
        preferredOrder = order
        this.fireAll = fireAll
        if (AxisType.RULE.is(type))
        {
            preferredOrder = DISPLAY
            this.valueType = AxisValueType.EXPRESSION
        }
        else if (AxisType.NEAREST.is(type))
        {
            preferredOrder = SORTED
            this.valueType = valueType
            defaultCol = null
        }
        else
        {
            this.valueType = valueType
        }
        this.valueToCol = createValueToColMap()

        if (hasDefault && type != AxisType.NEAREST)
        {
            defaultCol = new Column(null, defaultColId)
            indexColumn(defaultCol)
        }

        verifyAxisType()
    }

    /**
     * Use this constructor to create a 'reference' axis.  This allows a single MASTER DATA axis to be referenced
     * by many other axes without repeating the columnar data.
     * @param name String Axis name
     * @param id long id of Axis.  Ask n-cube for max ID, then add 1 to it, and use that.
     * @param axisRefProvider implementer is expected to load(this), e.g. load this axis completely, setting
     * all fields, etc.
     */
    Axis(String name, long id, boolean hasDefault, AxisRefProvider axisRefProvider)
    {
        this.name = name
        this.id = id
        isRef = true
        this.valueToCol = null

        // Ask the provider to load this axis up.
        axisRefProvider.load(this)

        if (hasDefault && type != AxisType.NEAREST)
        {
            defaultCol = new Column(null, defaultColId)
            indexColumn(defaultCol)
        }

        // Verify that the axis is indeed valid
        verifyAxisType()

        if (!AxisValueType.values().contains(valueType))
        {
            throw new IllegalStateException("AxisValueType not set, axis: ${name}")
        }

        if (preferredOrder != DISPLAY && preferredOrder != SORTED)
        {
            throw new IllegalStateException("preferred order not set, axis: ${name}")
        }
    }

    /**
     * Re-index the axis (optionally assign new ID to the axis).
     * @param newId long optional new axis id
     */
    protected void reindex(long newId = getId())
    {
        id = newId
        List<Column> columns = columns
        clearIndexes()
        long axisIdPart = BASE_AXIS_ID * id
        for (Column column : columns)
        {
            column.id = axisIdPart + column.id % BASE_AXIS_ID
            indexColumn(column)
        }
    }

    private void reloadReferenceAxis(String cubeName, Map<String, Object> props)
    {
        clearIndexes()
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(cubeName, name, props)
        refAxisLoader.load(this)
    }

    protected updateMetaProperties(Map<String, Object> newMetaProperties, String cubeName, Closure dropOrphans)
    {
        // Backup meta-properties
        Map<Long, Map> metaMap = [:] as Map
        columns.each { Column column ->
            metaMap.put(column.id, column.metaProperties)
        }

        clearMetaProperties()
        addMetaProperties(newMetaProperties)
        if (isRef)
        {
            reloadReferenceAxis(cubeName, newMetaProperties)
        }

        Set<Long> colIds = new LinkedHashSet()
        columns.each { Column column ->
            colIds.add(column.id)
        }

        dropOrphans(colIds)

        // Restore meta-properties
        columns.each { Column column ->
            if (metaMap.containsKey(column.id))
            {
                Map map = metaMap.get(column.id)
                column.addMetaProperties(map)
            }
        }
    }

    private void verifyAxisType()
    {
        if (AxisType.RULE.is(type))
        {
            rangeToCol = null
            valueToCol = null
        }
        else if (AxisType.DISCRETE.is(type) || AxisType.NEAREST.is(type))
        {
            rangeToCol = null
        }
        else if (AxisType.RANGE.is(type) || AxisType.SET.is(type))
        {
            valueToCol = null
        }
        else
        {
            throw new IllegalArgumentException("Unknown axis type: ${type}")
        }
    }

    /**
     * @return ApplicationID of the referenced cube (if this Axis is a reference Axis, or
     * null otherwise).
     */
    ApplicationID getReferencedApp()
    {
        String status = (getMetaProperty(REF_STATUS) as String) ?: ReleaseStatus.RELEASE
        String branch = (getMetaProperty(REF_BRANCH) as String) ?: ApplicationID.HEAD
        return isRef ? new ApplicationID(getMetaProperty(REF_TENANT) as String,
                getMetaProperty(REF_APP) as String,
                getMetaProperty(REF_VERSION) as String,
                status,
                branch) : null
    }

    /**
     * @return String name of referenced cube (or null if this is not a reference axis)
     */
    String getReferenceCubeName()
    {
        return getMetaProperty(REF_CUBE_NAME)
    }

    /**
     * @return String name of referenced cube axis (or null if this is not a reference axis)
     */
    String getReferenceAxisName()
    {
        return getMetaProperty(REF_AXIS_NAME)
    }

    /**
     * @return ApplicationID of the transformer cube (if this Axis is a reference Axis and it
     * specifies a transformer cube, otherwise null).
     */
    ApplicationID getTransformApp()
    {
        String status = (getMetaProperty(TRANSFORM_STATUS) as String) ?: ReleaseStatus.RELEASE
        String branch = (getMetaProperty(TRANSFORM_BRANCH) as String) ?: ApplicationID.HEAD
        return referenceTransformed ? new ApplicationID(getMetaProperty(REF_TENANT) as String,
                getMetaProperty(TRANSFORM_APP) as String,
                getMetaProperty(TRANSFORM_VERSION) as String,
                status,
                branch) : null
    }

    /**
     * @return String name of referenced cube (or null if this is not a reference axis)
     */
    String getTransformCubeName()
    {
        return getMetaProperty(TRANSFORM_CUBE_NAME)
    }

    /**
     * @return boolean true if this Axis is a reference Axis AND there is a transformer app
     * specified for the reference.
     */
    boolean isReferenceTransformed()
    {
        String status = (getMetaProperty(TRANSFORM_STATUS) as String) ?: ReleaseStatus.RELEASE
        String branch = (getMetaProperty(TRANSFORM_BRANCH) as String) ?: ApplicationID.HEAD
        return isRef && hasContent(getMetaProperty(TRANSFORM_APP) as String) &&
                hasContent(getMetaProperty(TRANSFORM_VERSION) as String) &&
                hasContent(status) &&
                hasContent(branch) &&
                hasContent(transformCubeName)
    }

    /**
     * @return boolean true if this Axis is a reference to another axis, not a 'real' axis.  A reference axis
     * cannot be modified.
     */
    boolean isReference()
    {
        return isRef
    }

    /**
     * Break the reference to the other axis.  After calling this method, this axis will
     * be a copy of the axis to which it had pointed.
     */
    void breakReference()
    {
        isRef = false
        removeMetaProperty(REF_TENANT)
        removeMetaProperty(REF_APP)
        removeMetaProperty(REF_VERSION)
        removeMetaProperty(REF_STATUS)
        removeMetaProperty(REF_BRANCH)
        removeMetaProperty(REF_CUBE_NAME)
        removeMetaProperty(REF_AXIS_NAME)
        removeTransform()
    }

    void makeReference(ApplicationID refAppId, String refCubeName, refAxisName)
    {
        isRef = true
        Map args = [
                (REF_TENANT): refAppId.tenant,
                (REF_APP): refAppId.app,
                (REF_VERSION): refAppId.version,
                (REF_STATUS): refAppId.status,
                (REF_BRANCH): refAppId.branch,
                (REF_CUBE_NAME): refCubeName,
                (REF_AXIS_NAME): refAxisName
        ] as Map<String, Object>
        addMetaProperties(args)
        reloadReferenceAxis(refCubeName, args)
    }

    /**
     * Remove transform from reference axis.
     */
    void removeTransform()
    {
        removeMetaProperty(TRANSFORM_APP)
        removeMetaProperty(TRANSFORM_VERSION)
        removeMetaProperty(TRANSFORM_STATUS)
        removeMetaProperty(TRANSFORM_BRANCH)
        removeMetaProperty(TRANSFORM_CUBE_NAME)
    }

    /**
     * @return long next id for use on a new Column.
     */
    protected long getNextColId()
    {
        long baseAxisId = id * BASE_AXIS_ID
        Random random = LOCAL_RANDOM.get()
        long uniqueId = random.nextLong()
        long total = uniqueId % MAX_COLUMN_ID

        while (uniqueId <= 0L || idToCol.containsKey(baseAxisId + total))
        {
            uniqueId = random.nextLong()
            total = uniqueId % MAX_COLUMN_ID
        }
        return baseAxisId + total
    }

    protected long getDefaultColId()
    {
        return id * BASE_AXIS_ID + Integer.MAX_VALUE
    }

    /**
     * @return Map (case insensitive keys) containing meta (additional) properties for the n-cube.
     */
    Map<String, Object> getMetaProperties()
    {
        Map<String, Object> ret = metaProps == null ? new CaseInsensitiveMap<String, Object>() : metaProps
        return Collections.unmodifiableMap(ret)
    }

    /**
     * Set (add / overwrite) a Meta Property associated to this axis.
     * @param key String key name of meta property
     * @param value Object value to associate to key
     * @return prior value associated to key or null if none was associated prior
     */
    Object setMetaProperty(String key, Object value)
    {
        if (metaProps == null)
        {
            metaProps = new CaseInsensitiveMap<>()
        }
        return metaProps.put(key, value)
    }

    /**
     * Fetch the value associated to the passed in Key from the MetaProperties (if any exist).  If
     * none exist, null is returned.
     */
    Object getMetaProperty(String key)
    {
        if (metaProps == null)
        {
            return null
        }
        return metaProps.get(key)
    }

    /**
     * Remove a meta-property entry
     */
    Object removeMetaProperty(String key)
    {
        if (metaProps == null)
        {
            return null
        }
        return metaProps.remove(key)
    }

    /**
     * Add a Map of meta properties all at once.
     * @param props Map of meta properties to add
     */
    void addMetaProperties(Map<String, Object> props)
    {
        if (metaProps == null)
        {
            metaProps = new CaseInsensitiveMap<>()
        }
        metaProps.putAll(props)
    }

    /**
     * Remove all meta properties associated to this Axis.
     */
    void clearMetaProperties()
    {
        if (metaProps != null)
        {
            metaProps.clear()
            metaProps = null
        }
    }

    /**
     * @return boolean true if this RULE axis is set to fire all conditions that evaluate to true, false otherwise.
     */
    boolean isFireAll()
    {
        return fireAll
    }

    /**
     * Set the 'fire-all' mode - true to have all conditions that evaluate to true have the associated cell execute,
     * false for only the first condition that is true to have it's associated cell execute.
     */
    void setFireAll(boolean fireAll)
    {
        this.fireAll = fireAll
    }

    /**
     * Use Column id to retrieve column (hash map lookup), O(1)
     */
    Column getColumnById(long colId)
    {
        return idToCol.get(colId)
    }

    /**
     * Index the passed in column.  It is expected the Column value, id, name (optional), and display ID are
     * already established.
     */
    private void indexColumn(Column column)
    {
        // 1: Index columns by ID
        idToCol.put(column.id, column)

        // 2: Index columns by name (if they have one) - held in CaseInsensitiveMap
        String colName = column.columnName
        if (hasContent(colName))
        {
            colNameToCol.put(colName, column)
        }

        // 3. Index column by value
        if (column.value == null)
        {   // No value to index (default column) AND do not add to displayOrder below
            return
        }

        // 4. Index columns by display order
        int order = column.displayOrder
        if (displayOrder.containsKey(column.displayOrder))
        {   // collision - move it to end
            order = displayOrder.lastKey() + 1
            column.displayOrder = order
        }
        displayOrder.put(order, column)

        if (AxisType.DISCRETE.is(type) || AxisType.NEAREST.is(type))
        {
            valueToColumnMap.put(standardizeColumnValue(column.value), column)
        }
        else if (AxisType.RANGE.is(type))
        {
            rangeToCol.put(valueToRange(column.value), column)
        }
        else if (AxisType.SET.is(type))
        {
            RangeSet set = (RangeSet)column.value
            final int len = set.size()
            for (int i=0; i < len; i++)
            {
                Comparable elem = set.get(i)
                rangeToCol.put(valueToRange(elem), column)
            }
        }
    }

    /**
     * @return long id of this Axis
     */
    long getId()
    {
        return id
    }

    /**
     * Set long id of this Axis
     */
    void setId(long id)
    {
        this.id = id
    }

    /**
     * @return String version of axis properties.  Useful for SHA-1 computations.
     */
    String getAxisPropString()
    {
        StringBuilder s = new StringBuilder()
        s.append('Axis: ')
        s.append(name)
        s.append(' [')
        s.append(type)
        s.append(', ')
        s.append(valueType)
        s.append(hasDefaultColumn() ? ', default-column' : ', no-default-column')
        s.append(SORTED == preferredOrder ? ', sorted' : ', unsorted')
        s.append(']')
        return s.toString()
    }

    String toString()
    {
        StringBuilder s = new StringBuilder(axisPropString)
        if (!MapUtilities.isEmpty(metaProps))
        {
            s.append('\n')
            s.append("  metaProps: ${metaProps}")
        }

        return s.toString()
    }

    /**
     * @return String Axis name.  The name is the value that String keys match (bind to) on the input.  Although
     * the case of the name is maintained, it is compared case-insensitively.
     */
    String getName()
    {
        return name
    }

    protected void setName(String name)
    {
        this.name = name
    }

    /**
     * @return AxisType of this Axis, which is one of: DISCRETE, RANGE, SET, NEAREST, RULE
     */
    AxisType getType()
    {
        return type
    }

    protected void setType(AxisType newType)
    {
        type = newType
    }

    /**
     * @return AxisValueType of this Axis, which is one of: STRING, LONG, BIG_DECIMAL, DOUBLE, DATE, EXPRESSION, COMPARABLE
     */
    AxisValueType getValueType()
    {
        return valueType
    }

    protected void setValueType(AxisValueType newValueType)
    {
        valueType = newValueType
        Map<Comparable, Column> existing = valueToCol
        valueToCol = createValueToColMap()

        if (existing)
        {
            valueToCol.putAll(existing)
        }
    }

    private Map<Comparable, Column> getValueToColumnMap()
    {
        if (valueToCol == null)
        {
            valueToCol = createValueToColMap()
        }
        return valueToCol
    }

    private Map createValueToColMap()
    {
        valueToCol = AxisValueType.CISTRING.is(valueType) ? new TreeMap(String.CASE_INSENSITIVE_ORDER) : new TreeMap()
        return valueToCol
//        if (AxisType.NEAREST.is(type))
//        {
//            valueToCol = AxisValueType.CISTRING.is(valueType) ? new TreeMap(String.CASE_INSENSITIVE_ORDER) : new TreeMap()
//        }
//        else
//        {
//            // Note: the code below makes findColumn() faster, but then getColumnsWithoutDefault() is slower()
//            valueToCol = AxisValueType.CISTRING.is(valueType) ? new CaseInsensitiveMap<Comparable, Column>() : new HashMap<Comparable, Column>()
//        }
//        return valueToCol
    }

    protected void clearIndexes()
    {
        idToCol.clear()
        colNameToCol.clear()
        displayOrder.clear()
        valueToCol?.clear()
        rangeToCol?.clear()
    }

    /**
     * Given the passed in 'raw' value, get a Column from the passed in value, which entails
     * converting the 'raw' value to the correct type, promoting the value to the appropriate
     * internal value for comparison, and so on.
     * @param value Comparable typically a primitive, but can also be an n-cube Range, RangeSet, CommandCell,
     * or 2D, 3D, or LatLon.
     * @param suggestedId Long suggested column ID.  Can be null or 0, in which case an ID will be generated. If not,
     * then the ID will be used (only the column portion, not the Axis ID portion).  If that matches an existing ID
     * on the Axis, then an ID will be generated.
     * @return a Column with the up-promoted value as the column's value, and a unique ID on the column.  If
     * the original value is a Range or RangeSet, the components in the Range or RangeSet are also up-promoted.
     */
    protected Column createColumnFromValue(Comparable value, Long suggestedId, Map<String, Object> metaProperties = null)
    {
        value = standardizeColumnValue(value)
        if (suggestedId != null && suggestedId > 0 && value != null)
        {
            long attemptId = (id * BASE_AXIS_ID) + (suggestedId % BASE_AXIS_ID)
            long finalId

            if (idToCol.containsKey(attemptId))
            {
                long colId = getValueBasedColumnId(value)
                finalId = idToCol.containsKey(colId) ? nextColId : colId
            }
            else
            {
                finalId = attemptId
            }
            return new Column(value, finalId, metaProperties)
        }
        else
        {
            return new Column(value, value == null ? defaultColId : nextColId, metaProperties)
        }
    }

    private long getValueBasedColumnId(Comparable value)
    {
        String s = value == null ? '' : value.toString()
        long hash = abs(calculateSHA1Hash(s.getBytes('UTF-8')).hashCode()) % MAX_COLUMN_ID
        hash += id * BASE_AXIS_ID
        return hash
    }

    /**
     * Will throw IllegalArgumentException if passed in value duplicates a value on this axis.
     */
    private void ensureUnique(Comparable value)
    {
        if (value == null)
        {  // Attempting to add Default column to axis
            if (hasDefaultColumn())
            {
                throw new IllegalArgumentException("Cannot add default column to axis '${name}' because it already has a default column.")
            }
            if (AxisType.NEAREST.is(type))
            {
                throw new IllegalArgumentException("Cannot add default column to NEAREST axis '${name}' as it would never be chosen.")
            }
        }
        else
        {
            if (AxisType.DISCRETE.is(type) || AxisType.NEAREST.is(type))
            {
                if (valueToColumnMap.containsKey(value))
                {
                    throw new AxisOverlapException("Passed in value '${value}' matches a value already on axis '${name}'")
                }
            }
            else if (AxisType.RANGE.is(type))
            {
                Range range = (Range)value
                if (doesOverlap(range))
                {
                    throw new AxisOverlapException("Passed in Range overlaps existing Range on axis: ${name}, value: ${value}")
                }
            }
            else if (AxisType.SET.is(type))
            {
                RangeSet set = (RangeSet)value
                if (doesOverlap(set))
                {
                    throw new AxisOverlapException("Passed in RangeSet overlaps existing RangeSet on axis: ${name}, value: ${value}")
                }
            }
            else if (AxisType.RULE.is(type))
            {
                if (!(value instanceof CommandCell))
                {
                    throw new IllegalArgumentException("Columns for RULE axis must be a CommandCell, axis: ${name}, value: ${value}")
                }
            }
            else
            {
                throw new IllegalStateException("New axis type added without complete support.")
            }
        }
    }

    /**
     * Add a new Column to this axis.  It will be added at the end in terms of display order.  If the
     * axis is SORTED, it will be returned in sorted order if getColumns() or getColumnsWithoutDefault()
     * are called.
     * @param value Comparable value to add to this Axis.
     * @param colName The name of the column (useful for Rule axes.  Any column can be given a name). Optional.
     * @param suggestedId Long use the suggested ID if possible.  This allows an axis to be recreated
     * from persistent storage and have the same IDs. Optional.
     * @return Column instanced created from the passed in value.
     */
    Column addColumn(Comparable value, String colName = null, Long suggestedId = null, Map<String, Object> colMetaProps = null)
    {
        final Column column = createColumnFromValue(value, suggestedId, colMetaProps)
        if (hasContent(colName))
        {
            column.columnName = colName
        }
        addColumnInternal(column)
        return column
    }

    /**
     * Add a Column from another Axis to this Axis.  It will
     * attempt to use the ID that is already on the Column, ignoring
     * the Axis portion of the ID.  If there is a conflict, it will
     * then use an ID deterministically generated from the value of
     * the column.
     * @param column Column to add
     * @return Column added - the ID may not be the same as the ID from
     * the Column passed in.
     */
    Column addColumn(Column column)
    {
        final Column newColumn = createColumnFromValue(column.value, column.id, column.metaProperties)
        addColumnInternal(newColumn)
        return newColumn
    }

    protected Column addColumnInternal(Column column)
    {
        ensureUnique(column.value)

        if (column.value == null)
        {
            column.id = defaultColId    // Safety check - should never happen
            defaultCol = column
        }

        if (AxisType.RULE.is(type) && !column.default)
        {
            String colName = column.columnName
            if (isEmpty(colName))
            {
                throw new IllegalArgumentException('Rule name cannot be empty.')
            }

            if (findColumnByName(colName))
            {
                throw new IllegalArgumentException("There is already a rule named: ${colName} on axis: ${name}.")
            }
        }

        // New columns are always added at the end in terms of displayOrder.
        int order = displayOrder.isEmpty() ? 1 : displayOrder.lastKey() + 1
        column.displayOrder = column.default ? Integer.MAX_VALUE : order
        indexColumn(column)
        return column
    }

    /**
     * This method deletes a column from an Axis.  It is intentionally package
     * scoped because there are two parts to deleting a column - this removes
     * the column from the Axis, the other part removes the Cells that reference
     * the column (that is within NCube).
     * @param value Comparable value used to identify the column to delete.
     * @return Column that was deleted, or null if no column would be deleted.
     */
    protected Column deleteColumn(Comparable value)
    {
        Column col = findColumn(value)
        if (col == null)
        {	// Not found.
            return null
        }

        return deleteColumnById(col.id)
    }

    protected Column deleteColumnById(long colId)
    {
        Column col = idToCol.get(colId)

        if (isRef && !col.default)
        {
            throw new IllegalStateException("You cannot delete non-default columns from a reference Axis, axis: ${name}")
        }

        if (col == null)
        {
            return null
        }

        if (col.default)
        {
            defaultCol = null
        }

        // Remove column from scaffolding
        removeColumnFromIndex(col)
        return col
    }

    private void removeColumnFromIndex(Column col)
    {
        // Remove from col id to column map
        idToCol.remove(col.id)
        if (col.columnName)
        {
            colNameToCol.remove(col.columnName)
        }
        displayOrder.remove(col.displayOrder)
        if (col.value == null || AxisType.RULE.is(type))
        {   // Default Column is not indexed by value/range (null), so we are done.
            // Rule columns are not indexed by value/range
            return
        }

        // Remove from 'value' storage
        if (AxisType.DISCRETE.is(type) || AxisType.NEAREST.is(type))
        {   // O(1) remove
            valueToColumnMap.remove(standardizeColumnValue(col.value))
        }
        else if (AxisType.RANGE.is(type))
        {   // O(Log n) remove
            rangeToCol.remove(valueToRange(col.value))
        }
        else if (AxisType.SET.is(type))
        {   // O(Log n) remove
            RangeSet set = (RangeSet) col.value
            Iterator<Comparable> i = set.iterator()
            while (i.hasNext())
            {
                Comparable item = i.next()
                rangeToCol.remove(valueToRange(item))
            }
        }
        else
        {
            throw new IllegalStateException("Unsupported axis type (${type}) for axis '${name}', trying to remove column from internal index")
        }
    }

    /**
     * Update (change) the value of an existing column.  This entails not only
     * changing the value, but resorting the axis's columns (columns are always in
     * sorted order for quick retrieval).  The display order of the columns is not
     * rebuilt, because the column is changed in-place (e.g., changing Mon to Monday
     * does not change it's display order.)
     * @param colId long Column ID to update
     * @param value 'raw' value to set into the new column (will be up-promoted).
     * @param order int (optional) new display order for column
     */
    void updateColumn(long colId, Comparable value, String name = null, int order = -1i)
    {
        if (isRef)
        {
            throw new IllegalStateException("You cannot update columns on a reference Axis, axis: ${name}")
        }
        Column column = idToCol.get(colId)
        deleteColumnById(colId)

        if (hasContent(name))
        {
            column.columnName = name
        }
        Column newColumn = createColumnFromValue(value, colId, column.metaProperties)  // re-use ID & column meta-props
        ensureUnique(newColumn.value)

        // re-use displayOrder or take it from order arg
        newColumn.displayOrder = order == -1i ? column.displayOrder : order
        indexColumn(newColumn)
    }

    /**
     * Update (merge) columns on this Axis from the passed in Collection.  Columns that exist on both axes,
     * will have their values updated.  Columns that exist on this axis, but not exist in the 'newCols'
     * will be deleted (and returned as a Set of deleted Columns).  Columns that exist in newCols but not
     * on this are new columns.
     *
     * NOTE: The columns field within the newCols axis are NOT in sorted order as they normally are
     * within the Axis class.  Instead, they are in display order (this order is typically set forth by a UI).
     */
    Set<Long> updateColumns(Collection<Column> newCols, boolean allowPositiveColumnIds = false)
    {
        if (isRef)
        {
            throw new IllegalStateException("You cannot update columns on a reference Axis, axis: ${name}")
        }

        Set<Long> colsToDelete = new LinkedHashSet<>()
        Map<Long, Column> newColumnMap = [:]

        // Step 1. Map all columns from passed in Collection by ID
        for (Column col : newCols)
        {
            Column newColumn = createColumnFromValue(col.value, col.id, col.metaProperties)
            newColumnMap.put(col.id, newColumn)
        }

        // Step 2.  Build list of columns that no longer exist (add to deleted list)
        // AND update existing columns that match by ID columns from the passed in DTO.
        List<Column> tempCol = columns
        Iterator<Column> i = tempCol.iterator()

        while (i.hasNext())
        {
            Column col = i.next()
            if (newColumnMap.containsKey(col.id))
            {   // Update case - matches existing column
                Column newCol = newColumnMap.get(col.id)
                col.value = newCol.value
                Map<String, Object> metaProperties = newCol.metaProperties
                
                for (Map.Entry<String, Object> entry : metaProperties.entrySet())
                {
                    col.setMetaProperty(entry.key, entry.value)
                }
            }
            else
            {   // Delete case - existing column id no longer found
                if (col.value != null)
                {
                    colsToDelete.add(col.id as Long)
                    i.remove()
                }
            }
        }
        clearIndexes()

        // Step 3. Save existing before clearing all columns
        Map<Long, Column> existingColumns = [:]

        for (Column column : tempCol)
        {
            existingColumns.put(column.id, column)
            if (!column.default)
            {
                ensureUnique(column.value)
                indexColumn(column)
            }
        }
        int dispOrder = 1

        // Step 4. Add new columns (they exist in the passed in newCols, but not in this Axis) and
        // set display order to match the columns coming in from the DTO axis (argument).
        for (Column col : newCols)
        {
            if (col.value == null)
            {   // Skip Default column
                continue
            }
            long existingId = col.id
            if (allowPositiveColumnIds && !existingColumns.containsKey(existingId))
            {
                Column newCol = addColumnInternal(newColumnMap[col.id])
                newCol.displayOrder = dispOrder++
                existingId = newCol.id
                existingColumns[existingId] = newCol
            }
            else
            {
                if (col.id < 0)
                {   // Add case - negative id, add new column to 'columns' List.
                    Column newCol = addColumnInternal(newColumnMap[col.id])
                    existingId = newCol.id
                    existingColumns.put(existingId, newCol)
                }

                Column realColumn = existingColumns.get(existingId)
                if (realColumn == null)
                {
                    throw new IllegalArgumentException('Columns to be added should have negative ID values.')
                }
                realColumn.displayOrder = dispOrder++
            }
        }

        clearIndexes()
        for (Column col : existingColumns.values())
        {
            indexColumn(col)
        }

        return colsToDelete
    }

    private static GroovyExpression createExpressionFromValue(String value)
    {
        value = value.trim()
        boolean isUrl = false
        boolean cache = false
        boolean madeChange

        while (true) {
            madeChange = false
            // Place in loop to allow url|cache|http://... OR cache|url|http://...   OR url|http://  OR  cache|http://...
            if (value.startsWith('url|'))
            {
                isUrl = true
                value = value.substring(4)
                madeChange = true
            }

            if (value.startsWith('cache|'))
            {
                cache = true
                value = value.substring(6)
                madeChange = true
            }

            if (!madeChange)
            {
                break
            }
        }

        if (isUrl)
        {
            return new GroovyExpression(null, value, cache)
        }
        return new GroovyExpression(value, null, cache)
    }

    private Range parseRange(String value)
    {
        value = value.trim()
        if (value.startsWith('[') && value.endsWith(']'))
        {   // Remove surrounding brackets (1st and last characters)
            value = value[1..-2]
        }
        Matcher matcher = Regexes.rangePattern.matcher(value)
        if (matcher.matches())
        {
            String one = matcher.group(1)
            String two = matcher.group(2)
            return new Range(trimQuotes(one), trimQuotes(two))
        }
        else
        {
            throw new IllegalArgumentException("Value (${value}) cannot be parsed as a Range.  Use [value1, value2], axis: ${name}")
        }
    }

    private Comparable parseSet(String value)
    {
        try
        {   // input will always be comma delimited list of items and ranges (we add the final array brackets)
            value = '[' + value + ']'
            Object[] list = (Object[]) JsonReader.jsonToJava(value, [(JsonReader.USE_MAPS):true] as Map)
            final RangeSet set = new RangeSet()

            for (Object item : list)
            {
                if (item instanceof Object[])
                {   // Convert to Range
                    Object[] subList = (Object[]) item
                    if (subList.length != 2)
                    {
                        throw new IllegalArgumentException('Range inside set must have exactly two (2) entries.')
                    }
                    Range range = promoteRange(new Range((Comparable)subList[0], (Comparable)subList[1]))
                    set.add(range)
                }
                else if (item == null)
                {
                    throw new IllegalArgumentException('Set cannot have null value inside.')
                }
                else
                {
                    set.add(promoteValue(valueType, item as Comparable))
                }
            }
            if (set.size() > 0)
            {
                return set
            }
            throw new IllegalArgumentException("Value: ${value} cannot be parsed as a Set. Must have at least one element within the set, axis: ${name}")
        }
        catch (IllegalArgumentException e)
        {
            throw e
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Value: ${value} cannot be parsed as a Set.  Use v1, v2, [low, high], v3, ... , axis: ${name}", e)
        }
    }

    private static String trimQuotes(String value)
    {
        if (value.startsWith('"') && value.endsWith('"'))
        {
            return value[1..-2]
        }
        if (value.startsWith("'") && value.endsWith("'"))
        {
            return value[1..-2]
        }
        return value.trim()
    }

    /**
     * @return SORTED (0) or DISPLAY (1) which indicates whether the getColumns() and
     * getColumnsWithoutDefault() methods will return the columns in sorted order
     * or display order (user order).
     */
    int getColumnOrder()
    {
        return preferredOrder
    }

    /**
     * Set the ordering for the axis.
     * @param order int SORTED (0) or DISPLAY (1).
     */
    void setColumnOrder(int order)
    {
        preferredOrder = order
    }

    /**
     * @return int total number of columns on this axis.  Default column (if present) counts as 1.
     */
    int size()
    {
        return idToCol.size()
    }

    /**
     * This method takes the input value (could be Number, String, Range, etc.)
     * and 'promotes' it to the same type as the Axis.
     * @param value Comparable value to promote (to highest of it's type [e.g., short to long])
     * @return Comparable promoted value.  For example, a Long would be returned a
     * Byte value were passed in, and this was a LONG axis.
     */
    Comparable standardizeColumnValue(Comparable value)
    {
        if (value == null)
        {
            return null
        }
        else if (AxisType.DISCRETE.is(type))
        {
            return promoteValue(valueType, value)
        }
        else if (AxisType.RULE.is(type))
        {
            if (value instanceof String)
            {
                return createExpressionFromValue(value as String)
            }
            if (!(value instanceof CommandCell))
            {
                throw new IllegalArgumentException("Must only add CommandCell values to ${type} axis '${name}' - attempted to add: ${value.class.name}")
            }
            return value
        }
        else if (AxisType.RANGE.is(type))
        {
            if (value instanceof String)
            {
                value = parseRange(value as String)
            }
            if (!(value instanceof Range))
            {
                throw new IllegalArgumentException("Must only add Range values to ${type} axis '${name}' - attempted to add: ${value.class.name}")
            }
            return promoteRange((Range)value)
        }
        else if (AxisType.SET.is(type))
        {
            if (value instanceof String)
            {
                return parseSet(value as String)
            }
            else if (value instanceof Range)
            {
                return new RangeSet(promoteRange(value as Range))
            }
            else if (!(value instanceof RangeSet))
            {
                return new RangeSet(promoteValue(valueType, value))
            }
            RangeSet set = new RangeSet()
            Iterator<Comparable> i = (value as RangeSet).iterator()
            while (i.hasNext())
            {
                Comparable val = i.next()
                if (val instanceof Range)
                {
                    promoteRange((Range)val)
                }
                else
                {
                    val = promoteValue(valueType, val)
                }
                set.add(val)
            }
            return set
        }
        else if (AxisType.NEAREST.is(type))
        {	// Standardizing a NEAREST axis entails ensuring conformity amongst values (must all be Point2D, LatLon, Date, Long, String, etc.)
            value = promoteValue(valueType, value)
            if ((defaultCol != null && size() == 1) || (defaultCol == null && size() == 0))
            {   // Empty - first column added
                return value
            }
            Column col = (Column) displayOrder.get(displayOrder.firstKey())
            if (value.class != col.value.class)
            {
                throw new IllegalArgumentException("Value '${value.class.name}' cannot be added to axis '${name}' where the values are of type: ${col.value.class.name}")
            }
            return value	// First value added does not need to be checked
        }
        else
        {
            throw new IllegalArgumentException("New AxisType added '${type}' but code support for it is not there.")
        }
    }

    /**
     * Promote passed in range's low and high values to the largest
     * data type of their 'kinds' (e.g., byte to long, float to double).
     * @param range Range to be promoted
     * @return Range with the low and high values promoted and in proper order (low < high)
     */
    private Range promoteRange(Range range)
    {
        final Comparable low = promoteValue(valueType, range.low)
        final Comparable high = promoteValue(valueType, range.high)
        if (low > high)
        {
            range.low = high
            range.high = low
        }
        else
        {
            range.low = low
            range.high = high
        }
        return range
    }

    /**
     * Convert passed in value to a similar value of the highest type.  If the
     * valueType is not the same basic type as the value passed in, intelligent
     * conversions will happen, and the result will be of the requested type.
     *
     * An intelligent conversion example - String to date, it will parse the String
     * attempting to convert it to a date.  Or a String to a long, it will try to
     * parse the String as a long.  Long to String, it will .toString() the long,
     * and so on.
     * @return promoted value, or the same value if no promotion occurs.
     */
    static Comparable promoteValue(AxisValueType srcValueType, Comparable value)
    {
        if (AxisValueType.STRING.is(srcValueType) || AxisValueType.CISTRING.is(srcValueType))
        {
            return convertToString(value)
        }
        else if (AxisValueType.LONG.is(srcValueType))
        {
            return convertToLong(value)
        }
        else if (AxisValueType.BIG_DECIMAL.is(srcValueType))
        {
            return convertToBigDecimal(value)
        }
        else if (AxisValueType.DATE.is(srcValueType))
        {
            return convertToDate(value)
        }
        else if (AxisValueType.DOUBLE.is(srcValueType))
        {
            return convertToDouble(value)
        }
        else if (AxisValueType.EXPRESSION.is(srcValueType))
        {
            return value
        }
        else if (AxisValueType.COMPARABLE.is(srcValueType))
        {
            if (value instanceof String)
            {
                Matcher m = Regexes.valid2Doubles.matcher((String) value)
                if (m.matches())
                {   // No way to determine if it was supposed to be a Point2D. Specify as JSON for Point2D
                    return new LatLon(convertToDouble(m.group(1)), convertToDouble(m.group(2)))
                }

                m = Regexes.valid3Doubles.matcher((String) value)
                if (m.matches())
                {
                    return new Point3D(convertToDouble(m.group(1)), convertToDouble(m.group(2)), convertToDouble(m.group(3)))
                }

                try
                {   // Try as JSON
                    return (Comparable) JsonReader.jsonToJava((String) value)
                }
                catch (Exception ignored)
                {
                    return value
                }
            }
            return value
        }
        throw new IllegalArgumentException("AxisValueType '${srcValueType}' added but no code to support it.")
    }

    /**
     * @return boolean true if this Axis has a default column, false otherwise.
     */
    boolean hasDefaultColumn()
    {
        return defaultCol != null
    }

    /**
     * @return Column (the default Column instance whose column.value is null) or null if there is no default column.
     */
    Column getDefaultColumn()
    {
        return defaultCol
    }

    /**
     * @param value to test against this Axis
     * @return boolean true if the value will be found along the axis, false
     * if the value does not match anything along the axis.
     */
    boolean contains(Comparable value)
    {
        try
        {
            return findColumn(value) != null
        }
        catch (Exception ignored)
        {
            return false
        }
    }

    /**
     * Fetch Columns on a rule axis where the passed in name is the first rule desired.  All columns
     * until the ends will also be selected.
     * @param ruleName String name of rule to locate.
     * @return List of Columns starting with the column whose name matches 'ruleName'
     */
    protected List<Column> getRuleColumnsStartingAt(String ruleName)
    {
        if (isEmpty(ruleName))
        {   // Since no rule name specified, all rule columns are returned to have their conditions evaluated.
            return columns
        }

        List<Column> cols = []
        Column firstRule = findColumn(ruleName)
        if (firstRule == null)
        {   // A name was specified for a rule, but did not match any rule names and there is no default column.
            throw new CoordinateNotFoundException("Rule named '${ruleName}' matches no column names on the rule axis '${name}', and there is no default column.", null, null, name, ruleName)
        }
        else if (firstRule.default)
        {   // Matched no names, but there is a default column
            cols.add(defaultCol)
            return cols
        }

        // tailMap() efficiently snags everything matching and later
        Map result = displayOrder.tailMap(firstRule.displayOrder)
        cols.addAll(result.values())
        if (hasDefaultColumn())
        {
            cols.add(defaultCol)
        }
        return cols
    }

    /**
     * Find all rule Columns which have meta-properties that match all of the passed in required meta-properties.
     * In order for a Column to be selected, all keys in the requiredProps Map must match (case-insensitively) keys
     * in the meta-properties map of the Column, and the values must match each other.  If a value is the special
     * value Axis.DONT_CARE, then only the key must be present, no comparison is performed on the value.
     * @param requiredProps Map of String key / value pair criteria
     * @return A List<Column> that properly match the passed in requiredProps Map.  If no Columns match, then if
     * there is a Default column, it will be the single Column in the returned List.  The order of the Columns
     * returned in the list will be in the order listed when .columns() is called.  A Column instance will be
     * included only one or zero times.
     */
    List<Column> findColumns(Map<String, Object> requiredProps)
    {
        Iterator<Column> i = columns.iterator()
        List<Column> columns = []

        while (i.hasNext())
        {
            Column column = i.next()
            if (hasRequiredProps(requiredProps, column.metaProperties))
            {
                columns.add(column)
            }
        }

        if (columns.empty && hasDefaultColumn())
        {
            columns.add(defaultCol)
        }

        return columns
    }

    /**
     * Pass the Axis (this) to the passed in Closure. The closure is expected to take one argument, this
     * Axis, and return a List<Column> instances.  The closure can do whatever filtering it wants in
     * order to generated the List of Columns.  The list can contain a column more than once.  The generated
     * List is the orchestration (or order) in which the columns will be selected and used.
     * <pre>
     * Example:
     * axis.findColumns { Axis axis -> List columns = []; ... ; return columns }
     * </pre>
     * @param Closure that takes a single Axis argument and returns a List of Column instances.
     * @param
     * @return A List<Column> selected by the Closure. If the closure returns no columns, then if
     * there is a Default column, it will be the single Column in the returned List.  The order of
     * the Columns returned in the list will be in the order the closure returned them.  A Column
     * instance can be included more than one time.
     */
    List<Column> findColumns(Closure closure)
    {
        List<Column> columns = closure(this) as List

        if (columns.empty && hasDefaultColumn())
        {
            columns.add(defaultCol)
        }

        return columns
    }

    /**
     * Find all Columns which have a name (meta-property name) in the passed in Collection.
     * The Columns are returned based on the order in the passed in Collection.  A Column
     * could be returned more than once.
     * @param orchestration Collection of String names used to select Columns.
     * @return List<Column> that matches the passed in orchestration list.  If no Columns match, then if
     * there is a Default column, it will be the single Column in the returned List.  The same Column
     * can be returned more than once if it was listed more than once in the passed in Collection.  The
     * order of the Columns returned in the list will match the orchestration Collection order.
     */
    List<Column> findColumns(Collection<String> orchestration)
    {
        List<Column> columns = []
        Iterator<String> i = orchestration.iterator()

        while (i.hasNext())
        {
            String ruleName = i.next()
            Column column = findColumnByName(ruleName) // O(1)
            if (column)
            {
                columns.add(column)
            }
        }

        if (columns.empty && hasDefaultColumn())
        {
            columns.add(defaultCol)
        }

        return columns
    }

    /**
     * @param requiredProps Map of required key-value pairs (Criteria)
     * @param metaProps Map of key-value meta properties
     * @return true if the meta-Props contains all of the keys from the requiredProps Map, and all of the
     * values from the corresponding entries match.  If the requireProps has the special value Axis.DONT_CARE
     * associated to it, then only the key must be present.
     */
    private static boolean hasRequiredProps(Map<String, Object> requiredProps, Map<String, Object> metaProps)
    {
        Iterator<Map.Entry<String, Object>> i = requiredProps.entrySet().iterator()

        while (i.hasNext())
        {
            Map.Entry<String, Object> entry = i.next()
            if (metaProps.containsKey(entry.key))
            {
                Object value = metaProps.get(entry.key)
                if (DONT_CARE != entry.value)
                {
                    if (value != entry.value)
                    {
                        return false
                    }
                }
            }
            else
            {
                return false
            }
        }

        return true
    }

    /**
     * Get a Comparable value that can be used to locate a Column on this axis.  The passed in column may be
     * from another Axis (as in merging an axis from another cube).  This API will return the name or ID if
     * this Axis is a RULE axis, otherwise it will return getValueThatMatches() API.
     * @param column Column source
     * @return Comparable value that can be passed to the findColumn() or deleteColumn() APIs.
     */
    protected Comparable getValueToLocateColumn(Column column)
    {
        if (AxisType.RULE.is(type))
        {
            return hasContent(column.columnName) ? column.columnName : column.id
        }
        return column.valueThatMatches
    }

    /**
     * Locate the column (value) along an axis.
     * @param value Comparable - A value that can be checked against the axis
     * @return Column that 'matches' the passed in value, or null if no column
     * found.  'Matches' because matches depends on AxisType.
     */
    Column findColumn(final Comparable value)
    {
        if (value == null)
        {   // By returning defaultCol, this lets null match it if there is one, or null if there is none.
            return defaultCol
        }

        final Comparable promotedValue = promoteValue(valueType, value)

        if (AxisType.DISCRETE.is(type))
        {
            Column colToFind = valueToColumnMap.get(promotedValue)
            return colToFind == null ? defaultCol : colToFind
        }
        else if (AxisType.RANGE.is(type) || AxisType.SET.is(type))
        {	// RANGE axis searched in O(Log n) time using a binary search
            Column column = rangeToCol.get(promotedValue)
            return column == null ? defaultCol : column
        }
        else if (AxisType.RULE.is(type))
        {
            if (promotedValue instanceof Long)
            {
                return idToCol.get(promotedValue as Long)
            }
            else if (promotedValue instanceof String || promotedValue instanceof GString)
            {
                Column colToFind = colNameToCol.get(promotedValue as String)
                return colToFind == null ? defaultCol : colToFind
            }
            else
            {
                throw new IllegalArgumentException("A column on a rule axis can only be located by the 'name' attribute (String) or ID (long), axis: ${name}, value: ${promotedValue}")
            }
        }
        else if (AxisType.NEAREST.is(type))
        {
            return findNearest(promotedValue)   // O(Log n)
        }
        else
        {
            throw new IllegalArgumentException("Axis type '${type}' added but no code supporting it.")
        }
    }

    /**
     * Locate a column on an axis using the 'name' meta property.  If the value passed in matches no names, then
     * null will be returned.
     * Note: This is a case-insensitive match.
     * @param colName String name of column to locate
     * @return Column instance with the given name, otherwise null.
     */
    Column findColumnByName(String colName)
    {
        return colNameToCol.get(colName)
    }

    private Column findNearest(final Comparable promotedValue)
    {
        if (valueToColumnMap.isEmpty())
        {
            return null
        }

        NavigableMap<Comparable, Column> navMap = (NavigableMap<Comparable, Column>) valueToCol

        if (navMap.size() == 1)
        {
            return navMap.firstEntry().value
        }

        if (promotedValue instanceof Number)
        {   // Provide O(Log n) access when Number (any Number derivative) used on a NEAREST axis
            Map.Entry<Comparable, Column> entry1 = navMap.floorEntry(promotedValue as Comparable)
            Map.Entry<Comparable, Column> entry2 = navMap.higherEntry(promotedValue as Comparable)
            if (entry1 == null)
            {
                return entry2.value
            }
            if (entry2 == null || entry1.key == entry2.key)
            {
                return entry1.value
            }
            Number low = entry1.key as Number
            Number high = entry2.key as Number
            Number value = promotedValue as Number
            Number delta1 = value - low
            Number delta2 = value - high
            if (delta1.abs() <= delta2.abs())
            {
                return entry1.value
            }
            return entry2.value
        }
        else if (promotedValue instanceof Date)
        {   // Provide O(Log n) access when Date (any Date derivative) used on a NEAREST axis
            Map.Entry<Comparable, Column> entry1 = navMap.floorEntry(promotedValue as Comparable)
            Map.Entry<Comparable, Column> entry2 = navMap.higherEntry(promotedValue as Comparable)
            if (entry1 == null)
            {
                return entry2.value
            }
            if (entry2 == null || entry1.key == entry2.key)
            {
                return entry1.value
            }
            Date low = entry1.key as Date
            Date high = entry2.key as Date
            Date value = promotedValue as Date
            long delta1 = abs(value.time - low.time)
            long delta2 = abs(value.time - high.time)
            if (delta1 <= delta2)
            {
                return entry1.value
            }
            return entry2.value
        }
        else
        {   // Handle String, Point2D, Point3D, LatLon, etc. anything that implements the Distance interface
            double min = Double.MAX_VALUE
            Column saveCol = null

            for (Column column : columnsWithoutDefault)
            {
                double d = Proximity.distance(promotedValue, column.value)
                if (d < min)
                {    // Record column that set's new minimum record
                    min = d
                    saveCol = column
                }
            }
            return saveCol
        }
    }

    /**
     * Convert the passed in Comparable to a Gauva Range
     * @param value Comparable, typically a DISCRETE or Range value
     * @return Guava Range instance - Range will be closedOpen [ ) for RANGE, and closed [ ] for DISCRETE value
     */
    private static com.google.common.collect.Range valueToRange(Comparable value)
    {
        if (value instanceof Range)
        {
            Range range = (Range) value
            return com.google.common.collect.Range.closedOpen(range.low, range.high)
        }
        else
        {
            return com.google.common.collect.Range.closed(value, value)
        }
    }

    /**
     * Ensure that the passed in range does not overlap an existing Range on this
     * 'Range-type' axis.  Test low range limit to see if it is valid.
     * Axis is already a RANGE type before this method is called.
     * @param value Range (value) that is intended to be a new low range limit.
     * @return true if the Range overlaps this axis, false otherwise.
     */
    private boolean doesOverlap(Range range)
    {
        RangeMap ranges = rangeToCol.subRangeMap(valueToRange(range))
        return ranges.asMapOfRanges().size() > 0
    }

    /**
     * Test RangeSet to see if it overlaps any of the existing columns on
     * this cube.  Axis is already a RangeSet type before this method is called.
     * @param value RangeSet (value) to be checked
     * @return true if the RangeSet overlaps this axis, false otherwise.
     */
    private boolean doesOverlap(RangeSet set)
    {
        Iterator<Comparable> i = set.iterator()
        while (i.hasNext())
        {
            Comparable item = i.next()
            RangeMap rangeMap = rangeToCol.subRangeMap(valueToRange(item))
            if (rangeMap.asMapOfRanges().size() > 0)
            {
                return true
            }
        }
        return false
    }

    /**
     * @return List<Column> representing all of the Columns on this Axis.  This is a copy, so operations
     * on the List will not affect the Axis columns.  However, the Column instances inside the List are
     * not 'deep copied' so modifications to them should not be made, as it would violate the internal
     * Map structures maintaining the column indexing. The Columms are in SORTED or DISPLAY order
     * depending on the 'preferredOrder' setting.
     */
    List<Column> getColumns()
    {
        // return 'view' of Columns that matches the desired order (sorted or display)
        List<Column> cols = columnsWithoutDefault
        if (defaultCol != null)
        {   // Add in optional Default Column
            cols.add(defaultCol)
        }
        return cols
    }

    /**
     * @return List<Column> that contains all Columns on this axis (excluding the Default Column if it exists).  The
     * Columns will be returned in sorted order.  It is a copy of the internal list, therefore operations on the
     * returned List are safe, however, no changes should be made to the contained Column instances, as it would
     * violate internal indexing structures of the Axis.
     */
    List<Column> getColumnsWithoutDefault()
    {
        if (AxisType.DISCRETE.is(type))
        {
            Collection<Column> col = preferredOrder == DISPLAY ? displayOrder.values() : valueToColumnMap.values()
            return new ArrayList<>(col)
//            // Note: If we decide that findColumn should be O(1) not O(logN), then we can add the code below.
//            // Note: However, this is slower than returning the values() side of the map (about 3x times slower).
//            if (AxisValueType.CISTRING.is(valueType))
//            {
//                List<Column> cols = new ArrayList<>(valueToColumnMap.values())
//                // Note: If we decide that findColumn should be O(1) not O(logN), then we can add this
//                cols.sort(new Comparator<Column>() {
//                    int compare(Column c1, Column c2)
//                    {
//                        String v1 = c1.value
//                        String v2 = c2.value
//                        return v1.compareToIgnoreCase(v2)
//                    }
//                })
//                return cols
//            }
//            else
//            {
//                List<Column> cols = new ArrayList<>(valueToColumnMap.values())
//                cols.sort(new Comparator<Column>() {
//                    int compare(Column c1, Column c2)
//                    {
//                        return c1.value <=> c2.value
//                    }
//                })
//                return cols
//            }
        }
        else if (AxisType.NEAREST.is(type))
        {
            return new ArrayList<>((preferredOrder == SORTED) ? valueToColumnMap.values() : displayOrder.values())
        }
        else if (AxisType.RULE.is(type))
        {
            return new ArrayList<>(displayOrder.values())
        }
        else if (AxisType.RANGE.is(type) || AxisType.SET.is(type))
        {
            List<Column> cols = new ArrayList<>(size())
            if (preferredOrder == SORTED)
            {   // Consolidate Columns on the value side of the Map (multiple ranges can point to the same Column)
                Set<Column> set = new LinkedHashSet<>() // maintain order
                set.addAll(rangeToCol.asMapOfRanges().values())
                cols.addAll(set)
            }
            else
            {
                cols.addAll(displayOrder.values())
            }
            return cols
        }
        throw new IllegalStateException("AxisValueType '${type}' added but no code to support it.")
    }

    /**
     * @return true if all the properties on the passed in object are the same as this Axis.  If the passed in
     * object is not an Axis, false is returned.
     */
    boolean areAxisPropsEqual(Object o)
    {
        if (this.is(o))
        {
            return true
        }
        if (!(o instanceof Axis))
        {
            return false
        }

        Axis axis = (Axis) o

        if (preferredOrder != axis.preferredOrder)
        {
            return false
        }
        if (name != axis.name)
        {
            return false
        }
        if (type != axis.type)
        {
            return false
        }
        if (valueType != axis.valueType)
        {
            return false
        }
        return fireAll == axis.fireAll
    }

    /**
     * Return a display-friendly String for the passed in column.
     */
    protected static String getDisplayColumnName(Column column)
    {
        String colName = hasContent(column.columnName) ? column.columnName : column.value
        return colName ?: 'default'
    }

    /**
     * Find the Column on 'this' axis which has the same ID as the passed in axis.  If not found,
     * then check for Column with the same value (or name in case of RULE axis).
     * @param source
     * @return
     */
    protected Column locateDeltaColumn(Column source)
    {
        Column column = getColumnById(source.id)
        if (column)
        {
            return column
        }

        if (AxisType.RULE.is(type))
        {
            return findColumn(source.columnName)
        }
        else
        {
            return findColumn(source.value)
        }
    }
}
