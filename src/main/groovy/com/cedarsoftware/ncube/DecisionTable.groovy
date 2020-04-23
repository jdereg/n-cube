package com.cedarsoftware.ncube

import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.LongHashSet
import com.cedarsoftware.util.StringUtilities
import com.google.common.base.Splitter
import groovy.transform.CompileStatic
import it.unimi.dsi.fastutil.ints.IntIterator
import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import it.unimi.dsi.fastutil.ints.IntSet
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import static com.cedarsoftware.ncube.NCubeConstants.DATA_TYPE
import static com.cedarsoftware.ncube.NCubeConstants.DECISION_TABLE
import static com.cedarsoftware.ncube.NCubeConstants.IGNORE
import static com.cedarsoftware.ncube.NCubeConstants.INPUT_HIGH
import static com.cedarsoftware.ncube.NCubeConstants.INPUT_LOW
import static com.cedarsoftware.ncube.NCubeConstants.INPUT_VALUE
import static com.cedarsoftware.ncube.NCubeConstants.OUTPUT_VALUE
import static com.cedarsoftware.ncube.NCubeConstants.PRIORITY
import static com.cedarsoftware.ncube.NCubeConstants.REQUIRED
import static com.cedarsoftware.util.Converter.convertToBigDecimal
import static com.cedarsoftware.util.Converter.convertToBoolean
import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.Converter.convertToDouble
import static com.cedarsoftware.util.Converter.convertToInteger
import static com.cedarsoftware.util.Converter.convertToLong
import static com.cedarsoftware.util.Converter.convertToString
import static com.cedarsoftware.util.StringUtilities.hasContent
import static java.lang.String.CASE_INSENSITIVE_ORDER

/**
 * Decision Table implements a list of rules that filter a variable number of inputs (decision variables) against
 * a list of constraints.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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
class DecisionTable
{
    private NCube decisionTable
    private String fieldAxisName = null
    private String rowAxisName = null
    private Set<String> inputColumns = new CaseInsensitiveSet<>()
    private Set<String> inputKeys = new CaseInsensitiveSet<>()
    private Set<String> outputColumns = new CaseInsensitiveSet<>()
    private Set<String> rangeColumns = new CaseInsensitiveSet<>()
    private Set<String> rangeKeys = new CaseInsensitiveSet<>()
    private Set<String> requiredColumns = new CaseInsensitiveSet<>()
    private Map<String, Range> inputVarNameToRangeColumns = new CaseInsensitiveMap<>()
    private static final String BANG = '!'
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings()

    protected DecisionTable(NCube decisionCube)
    {
        decisionTable = decisionCube
        verifyAndCache()
    }

    private static class RangeSpec
    {
        String inputVarName
        String lowColumnName
        String highColumnName
        String dataType
    }

    /**
     * @return Set<String> All input keys that can be passed to the DecisionTable.getDecision() API.  Extra keys
     * can be passed, but will be ignored.
     */
    Set<String> getInputKeys()
    {
        Set<String> orderedKeys = new CaseInsensitiveSet<>()
        orderedKeys.addAll(inputKeys)
        return orderedKeys
    }

    /**
     * @return Set<String> All required keys (meta-property key REQUIRED value of true on a field column) that
     * must be passed to the DecisionTable.getDecision() API.
     */
    Set<String> getRequiredKeys()
    {
        Set<String> requiredKeys = new CaseInsensitiveSet<>()
        requiredKeys.addAll(requiredColumns)
        return requiredKeys
    }

    /**
     * Main API for querying a Decision Table.
     * @param input Map containing key/value pairs for all the input_value columns
     * @return List<Comparable, List<outputs>>
     */
    Map<Comparable, ?> getDecision(Map<String, ?> input)
    {
        Map<String, Map<String, ?>> ranges = new CaseInsensitiveMap<>()
        Map<String, ?> copyInput = new CaseInsensitiveMap<>(input)
        copyInput.keySet().retainAll(inputKeys)
        copyInput.put(IGNORE, null)
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        ensuredRequiredInputs(copyInput)

        for (String colValue : rangeColumns)
        {
            Column column = fieldAxis.findColumn(colValue)
            Map colMetaProps = column.metaProperties

            [INPUT_LOW, INPUT_HIGH].each { String highLow ->
                if (colMetaProps.containsKey(highLow))
                {   // Allow ranges to be processed in ANY order (even intermixed with other ranges)
                    String inputVarName = colMetaProps.get(highLow)
                    Map<String, ?> spec = getRangeSpec(ranges, inputVarName)
                    spec.put(highLow, column.value)

                    if (!spec.containsKey(INPUT_VALUE))
                    {   // Last range post (high or low) processed, sets the input_value, data_type, and copies the range spec to the input map.
                        // ["date": date instance] becomes ("date": [low:1900, high:2100, input_value: date instance, data_type: DATE])
                        Comparable value = convertDataType((Comparable)copyInput.get(inputVarName), (String)colMetaProps.get(DATA_TYPE))
                        spec.put(INPUT_VALUE, value)
                        spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                        copyInput.put(inputVarName, spec)
                    }
                }
            }
        }

        // Add on the IGNORE column if the field axis had it.
        Set<String> colsToSearch = new CaseInsensitiveSet<>(inputColumns)
        if (fieldAxis.contains(IGNORE))
        {
            colsToSearch.add(IGNORE)
        }

        // Add on the PRIORITY column if the field axis had it.
        Set<String> colsToReturn = new CaseInsensitiveSet<>(outputColumns)
        if (fieldAxis.findColumn(PRIORITY))
        {
            colsToReturn.add(PRIORITY)
        }

        Map<String, ?> closureInput = new CaseInsensitiveMap<>(input)
        closureInput.dvs = copyInput

        Map options = [
                (NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): colsToSearch,
                (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): colsToReturn,
                input: closureInput
        ]

        Map<Comparable, ?> result = decisionTable.mapReduce(fieldAxisName, decisionTableClosure, options)
        result = determinePriority(result)
        return result
    }

    /**
     * @return String name of Axis that is the 'fields' or 'attributes' of the Decision Table (main columns).
     * An IllegalStateException will be thrown if this method is called on a non-Decision Table.
     */
    String getDecisionAxisName()
    {
        return fieldAxisName
    }

    /**
     * @return String name of Axis that is the 'rows' of the Decision Table.
     * An IllegalStateException will be thrown if this method is called on a non-Decision Table.
     */
    String getDecisionRowName()
    {
        return rowAxisName
    }

    /**
     * Validate that the Decision Table has no overlapping rules.  In other words, it must return 0 or 1
     * records, never more.
     * NOTE: Currently, only supports one range variable (low, high) columns.
     * @return Set<Comparable> rowIds of the rows that have duplicate rules.  If the returned Set is empty,
     * then there are no overlapping rules in the DecisionTable.
     */
    Set<Comparable> validateDecisionTable()
    {
        List<Column> rows = decisionTable.getAxis(rowAxisName).columnsWithoutDefault
        NCube blowout = createValidationNCube(rows)
        Set<Comparable> badRows = validateDecisionTableRows(blowout, rows)
        return badRows
    }

    /**
     * @return NCube that sits within the DecisionTable.  
     */
    NCube getUnderlyingNCube()
    {
        return decisionTable
    }

    /**
     * Return the values defined in the decision table for every input_value column. The values represent the possible
     * values of the input_value columns. The values do not account for cells that are left blank which would match on
     * any input value passed.
     * @return Map<String, Set<String> Map with keys representing the input_value columns and values representing
     * all values defined in the cells associated to those columns
     */
    Map<String, Set<String>> getDefinedValues()
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Axis rowAxis = decisionTable.getAxis(rowAxisName)
        List<Column> rows = rowAxis.columnsWithoutDefault
        Map<String, Set<String>> definedValues = [:]
        Map<String, Comparable> coord = [:]
        Set<String> discreteInputCols = inputColumns

        for (String colValue : discreteInputCols)
        {
            Column field = fieldAxis.findColumn(colValue)
            Map colMetaProps = field.metaProperties
            String fieldValue = field.value
            Set<String> values = new TreeSet<>(CASE_INSENSITIVE_ORDER)

            if (colMetaProps.containsKey(INPUT_LOW) || colMetaProps.containsKey(INPUT_HIGH))
            {
                String inputKey = colMetaProps[INPUT_LOW] ?: colMetaProps[INPUT_HIGH]
                definedValues.put(inputKey, values)
                continue
            }

            coord.put(fieldAxisName, fieldValue)
            
            for (Column row : rows)
            {
                String rowValue = row.value
                coord.put(rowAxisName, rowValue)
                Set<Long> idCoord = new LongHashSet(field.id, row.id)
                String cellValue = convertToString(decisionTable.getCellById(idCoord, coord, [:], null, true))
                if (hasContent(cellValue))
                {
                    cellValue -= BANG
                    Iterable<String> cellValues = COMMA_SPLITTER.split(cellValue)
                    values.addAll(cellValues)
                }
            }

            definedValues.put(colValue, values)
        }
        return definedValues
    }

    /**
     * Closure used with mapReduce() on special 2D decision n-cubes.
     */
    private static Closure getDecisionTableClosure()
    {
        return { Map<String, ?> rowValues, Map<String, ?> input ->
            Map<String, ?> inputMap = (Map<String, ?>) input.get('dvs')

            for (Map.Entry<String, ?> entry : inputMap)
            {
                // 1. Check special IGNORE row variable
                String decVarName = entry.key
                Object decVarValue = rowValues.get(decVarName)
                Object inputValue = entry.value

                if (IGNORE == decVarName)
                {
                    if (decVarValue)
                    {
                        return false    // skip row
                    }
                    continue    // check next decision variable
                }

                // 2. Check range variables
                if (inputValue instanceof Map)
                {   // Using [ ] notation for Map access for sake of clarity
                    Comparable low = (Comparable)rowValues.get((String) inputValue[INPUT_LOW])
                    Comparable high = (Comparable)rowValues.get((String) inputValue[INPUT_HIGH])
                    Comparable value = (Comparable)inputValue[INPUT_VALUE]

                    if (value == null || value < low || value >= high)
                    {   // null is never between ranges
                        return false
                    }
                    continue
                }

                // 3. Check discrete decision variables
                String cellValue = convertToString(decVarValue)
                if (StringUtilities.isEmpty(cellValue))
                {   // Empty cells in input columns are treated as "*" (match any).
                    continue
                }
                inputValue = convertToString(inputValue)
                boolean exclude = cellValue.startsWith('!')
                cellValue -= BANG
                Iterable<String> cellValues = COMMA_SPLITTER.split(cellValue)

                if (exclude)
                {
                    if (cellValues.contains(inputValue))
                    {
                        return false
                    }
                }
                else
                {
                    if (!cellValues.contains(inputValue))
                    {
                        return false
                    }
                }
            }
            return true
        }
    }

    /**
     * Perform 'airport pat-down' of 2D NCube being used as a decision table.  This code will ensure that the
     * passed in NCube is 2D.  It will ensure that this operation is only performed once. This method will ensure
     * that both Axis are DISCRETE and that the field 'top axis' is of type CISTRING.  This method will ensure that
     * any range columns have the DATA_TYPE meta-property set.  It will ensure that no "half-ranges" are defined
     * (meaning only a low or high range is specified.)  This method will ensure that no two (or more) ranges
     * specify the same input variable name.  This method will convert all cell data in range columns to the data
     * type of the range columns, so that comparison is faster during validations and searches. Ignore column
     * values are converted to boolean.  Priority column values are converted to int.
     */
    private void verifyAndCache()
    {
        // If already called, ignore
        if (hasContent(fieldAxisName) && hasContent(rowAxisName))
        {
            return
        }

        if (decisionTable.numDimensions != 2)
        {
            throw new IllegalStateException("Decision table: ${decisionTable.name} must have 2 axes.")
        }

        Set<String> decisionMetaPropertyKeys = [INPUT_VALUE, INPUT_LOW, INPUT_HIGH, OUTPUT_VALUE] as Set<String>
        List<Axis> axes = decisionTable.axes
        Axis first = axes.first()
        Axis second = axes.last()

        Column any = first.columnsWithoutDefault.find { Column column ->
            Set<String> keys = new CaseInsensitiveSet<>(column.metaProperties.keySet())
            keys.retainAll(decisionMetaPropertyKeys)
            if (keys.size())
            {
                return column
            }
        }

        if (any)
        {
            fieldAxisName = first.name
            rowAxisName = second.name
        }
        else
        {
            any = second.columnsWithoutDefault.find { Column column ->
                Set<String> keys = new CaseInsensitiveSet<>(column.metaProperties.keySet())
                keys.retainAll(decisionMetaPropertyKeys)
                if (keys.size())
                {
                    return column
                }
            }

            if (any)
            {
                fieldAxisName = second.name
                rowAxisName = first.name
            }
        }

        if (!fieldAxisName || !rowAxisName)
        {
            throw new IllegalStateException("Decision table: ${decisionTable.name} must have one axis with one or more columns with meta-property keys: input_value and/or input_high/input_low.")
        }

        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        if (fieldAxis.type != AxisType.DISCRETE)
        {
            throw new IllegalStateException("Decision table: ${decisionTable.name} field / property axis must be a DISCRETE axis.  It is ${fieldAxis.type}")
        }

        if (fieldAxis.valueType != AxisValueType.CISTRING)
        {
            throw new IllegalStateException("Decision table: ${decisionTable.name} field / property axis must have value type of CISTRING.  It is ${fieldAxis.valueType}")
        }

        Map<String, RangeSpec> rangeSpecs = new CaseInsensitiveMap<>()
        for (Column column : decisionTable.getAxis(fieldAxisName).columnsWithoutDefault)
        {
            Map colMetaProps = column.metaProperties
            String columnValue = column.value

            if (colMetaProps.containsKey(INPUT_VALUE))
            {
                inputColumns.add(columnValue)
                inputKeys.add(columnValue)
            }

            if (colMetaProps.containsKey(INPUT_LOW) || colMetaProps.containsKey(INPUT_HIGH))
            {
                String dataType = colMetaProps.get(DATA_TYPE)
                if (dataType == null || !['LONG', 'DOUBLE', 'DATE', 'BIG_DECIMAL', 'STRING'].contains(dataType.toUpperCase()))
                {
                    throw new IllegalStateException("Range columns must have 'data_type' meta-property set, column: ${columnValue}, ncube: ${decisionTable.name}. Valid values are DATE, LONG, DOUBLE, BIG_DECIMAL, STRING.")
                }
                inputColumns.add(columnValue)
                rangeColumns.add(columnValue)
                RangeSpec rangeSpec
                String inputVarName

                if (colMetaProps.containsKey(INPUT_LOW))
                {
                    Object inputVariableName = colMetaProps.get(INPUT_LOW)
                    if (!(inputVariableName instanceof String))
                    {
                        throw new IllegalStateException("INPUT_LOW meta-property value must be of type String.  Column: ${columnValue}, ncube: ${decisionTable.name}")
                    }
                    inputVarName = colMetaProps.get(INPUT_LOW)
                    inputKeys.add(inputVarName)
                    if (colMetaProps.containsKey(REQUIRED))
                    {
                        requiredColumns.add(inputVarName)
                    }
                    rangeSpec = rangeSpecs.get(inputVarName)
                    if (rangeSpec == null)
                    {
                        rangeSpec = new RangeSpec()
                    }
                    if (rangeSpec.lowColumnName)
                    {
                        throw new IllegalStateException("More than one low range column with same input variable name found: ${INPUT_LOW}, ncube: ${decisionTable.name}. Each range variable should have a unique name.")
                    }
                    rangeSpec.lowColumnName = column.value
                }
                else
                {
                    Object inputVariableName = colMetaProps.get(INPUT_HIGH)
                    if (!(inputVariableName instanceof String))
                    {
                        throw new IllegalStateException("INPUT_HIGH meta-property value must be of type String.  Column: ${columnValue}, ncube: ${decisionTable.name}")
                    }
                    inputVarName = colMetaProps.get(INPUT_HIGH)
                    inputKeys.add(inputVarName)
                    if (colMetaProps.containsKey(REQUIRED))
                    {
                        requiredColumns.add(inputVarName)
                    }
                    rangeSpec = rangeSpecs.get(inputVarName)
                    if (rangeSpec == null)
                    {
                        rangeSpec = new RangeSpec()
                    }
                    if (rangeSpec.highColumnName)
                    {
                        throw new IllegalStateException("More than one high range column with same input variable name found: ${INPUT_LOW}. Each range variable should have a unique name.")
                    }
                    rangeSpec.highColumnName = columnValue
                }
                rangeSpec.dataType = dataType
                rangeSpecs.put(inputVarName, rangeSpec)
            }
            else
            {
                if (colMetaProps.containsKey(REQUIRED))
                {   // REQUIRED on non-input columns will be verified later in the code below.
                    requiredColumns.add(columnValue)
                }
            }
            if (colMetaProps.containsKey(OUTPUT_VALUE))
            {
                outputColumns.add(columnValue)
            }
        }

        Set<String> requiredColumnsCopy = new CaseInsensitiveSet<>(requiredColumns)
        requiredColumnsCopy.removeAll(inputKeys)
        if (!requiredColumnsCopy.empty)
        {
            throw new IllegalStateException("REQUIRED meta-property found on columns that are not input_value, input_low, or input_high. These were: ${requiredColumnsCopy}, ncube: ${decisionTable.name}")
        }

        // Throw error if a range is only half-defined (lower with no upper, upper with no lower).
        for (RangeSpec rangeSpec : rangeSpecs.values())
        {
            String boundName1 = null
            String boundName2 = null
            if (StringUtilities.isEmpty(rangeSpec.lowColumnName))
            {
                boundName1 = 'Upper'
                boundName2 = 'lower'
            }
            else if (StringUtilities.isEmpty(rangeSpec.highColumnName))
            {
                boundName1 = 'Lower'
                boundName2 = 'upper'
            }
            if (boundName1)
            {
                throw new IllegalStateException("${boundName1} range Column defined without matching ${boundName2} range Column. Input variable name: ${rangeSpec.inputVarName}, data type: ${rangeSpec.dataType}, ncube: ${decisionTable.name}")
            }
        }

        rangeKeys = new CaseInsensitiveSet<>(inputKeys)
        rangeKeys.removeAll(inputColumns)

        // Convert all values in the table to the data_type specified on the column meta-property (if there is one)
        Axis rowAxis = decisionTable.getAxis(rowAxisName)
        Map<String, ?> coord = new CaseInsensitiveMap<>()
        List<Column> rowColumns = rowAxis.columnsWithoutDefault
        Set<String> rangePlusOutputCols = new CaseInsensitiveSet<>(rangeColumns)
        rangePlusOutputCols.addAll(outputColumns)

        for (String colName : rangePlusOutputCols)
        {
            Column column = fieldAxis.findColumn(colName)
            String dataType = column.getMetaProperty(DATA_TYPE)
            if (StringUtilities.isEmpty(dataType))
            {   // Only convert cell values on columns that have DATA_TYPE
                continue
            }
            Object columnValue = column.value
            coord.put(fieldAxisName, columnValue)

            for (Column row : rowColumns)
            {
                coord.put(rowAxisName, row.value)
                Set<Long> idCoord = new LongHashSet(column.id, row.id)
                Object value = decisionTable.getCellById(idCoord, coord, [:], null, true)

                if (rangeColumns.contains(columnValue))
                {   // Convert range column values, ensure their cell values are not null
                    if (value == null || !(value instanceof Comparable))
                    {
                        throw new IllegalStateException("Values in range column must be instanceof Comparable, row ${row.value}, field: ${columnValue}, ncube: ${decisionTable.name}")
                    }
                }
                else
                {
                    if (!(value == null || value instanceof Comparable))
                    {
                        throw new IllegalStateException("Values in columns with DATA_TYPE meta-property must be instanceof Comparable, row ${row.value}, field: ${columnValue}, ncube: ${decisionTable.name}")
                    }
                }
                decisionTable.setCellById(convertDataType((Comparable) value, dataType), idCoord, true)
            }
        }
        computeInputVarToRangeColumns()
        convertSpecialColumnsToPrimitive()

        // Place an boolean indicator on the NCube meta-property 'decision_table'.
        // This will allow code that gets the NCube only, access to know if it is inside an DecisionTable.
        // Not sure if this will be useful, as it will not be written out this way.
        decisionTable.setMetaProperty(DECISION_TABLE, Boolean.TRUE)
    }

    /**
     * Convert all IGNORE column values to boolean, and all priority column values to int.
     */
    private void convertSpecialColumnsToPrimitive()
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Axis rowAxis = decisionTable.getAxis(rowAxisName)
        Map<String, ?> coord = new CaseInsensitiveMap<>()

        long ignoreColId = -1
        Column ignoreCol = fieldAxis.findColumn(IGNORE)

        if (ignoreCol != null && !ignoreCol.default)
        {
            ignoreColId = ignoreCol.id
        }

        long priorityColId = -1
        Column priorityCol = fieldAxis.findColumn(PRIORITY)

        if (priorityCol != null && !priorityCol.default)
        {
            priorityColId = priorityCol.id
        }

        if (priorityColId == -1 && ignoreColId == -1)
        {   // Nothing to do here
            return
        }

        for (Column row : rowAxis.columnsWithoutDefault)
        {
            coord.put(rowAxisName, row.value)

            if (ignoreColId != -1)
            {
                Set<Long> idCoord = new LongHashSet(ignoreColId, row.id)
                Object value = decisionTable.getCellById(idCoord, coord, [:])
                decisionTable.setCellById(convertToBoolean(value), idCoord, true)
            }

            if (priorityColId != -1)
            {
                Set<Long> idCoord = new LongHashSet(priorityColId, row.id)
                Integer intValue = convertToInteger(decisionTable.getCellById(idCoord, coord, [:]))
                if (intValue < 1)
                {   // If priority is not specified, then it is the lowest priority of all
                    intValue = Integer.MAX_VALUE
                }
                decisionTable.setCellById(intValue, idCoord, true)
            }
        }
    }

    /**
     * Create Map that maps input variable name to the two columns that are needed to represent it.
     */
    private void computeInputVarToRangeColumns()
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)

        for (String colValue : inputColumns)
        {
            Column field = fieldAxis.findColumn(colValue)
            if (field.metaProperties.containsKey(INPUT_LOW))    // calling field.metaProperties to ensure the field is loaded
            {
                Range pair = inputVarNameToRangeColumns.get(field.metaProps.get(INPUT_LOW))
                if (pair == null)
                {
                    pair = new Range()
                    inputVarNameToRangeColumns.put((String)field.metaProps.get(INPUT_LOW), pair)
                }
                pair.low = field
            }
            else if (field.metaProps.containsKey(INPUT_HIGH))
            {
                Range pair = inputVarNameToRangeColumns.get(field.metaProps.get(INPUT_HIGH))
                if (pair == null)
                {
                    pair = new Range()
                    inputVarNameToRangeColumns.put((String)field.metaProps.get(INPUT_HIGH), pair)
                }
                pair.high = field
            }
        }
    }

    private static Map<Comparable, ?> determinePriority(Map<Comparable, ?> result)
    {
        Map<Comparable, ?> result2 = [:]
        Long highestPriority = null
        for (Map.Entry<Comparable, ?> entry : result.entrySet())
        {
            Map row = (Map) entry.value
            Long currentPriority = convertToLong(row[PRIORITY])
            if (highestPriority == null)
            {
                highestPriority = currentPriority
                result2.put(entry.key, entry.value)
            }
            else if (currentPriority < highestPriority)
            {
                highestPriority = currentPriority
                result2.clear()
                result2.put(entry.key, entry.value)
            }
            else if (currentPriority == highestPriority)
            {
                result2.put(entry.key, entry.value)
            }
            row.remove(PRIORITY)    // Do not return priority field
        }
        return result2
    }

    /**
     * Create an N-Dimensional NCube, where the number of dimensions (N) is equivalent to the number
     * of discrete (non range) columns on the field axis.  When this method returns, it will have no
     * cell contents, however, all Axes will be set up with each axis (dimension) containing all the
     * values that appeared in rows (whether specified negative or positively, as a single value or
     * a comma delimited list.)
     */
    private NCube createValidationNCube(List<Column> rows)
    {
        NCube blowout = new NCube('validation')
        Map<String, Comparable> coord = [:]
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Set<String> colsToProcess = new CaseInsensitiveSet<>(inputColumns)
        colsToProcess.removeAll(rangeColumns)

        for (String colValue : colsToProcess)
        {
            Column field = fieldAxis.findColumn(colValue)
            Axis axis = new Axis(colValue, AxisType.DISCRETE, AxisValueType.STRING, false)
            blowout.addAxis(axis)
            String fieldValue = field.value
            coord.put(fieldAxisName, fieldValue)

            for (Column row : rows)
            {
                String rowValue = row.value
                coord.put(rowAxisName, rowValue)
                Set<Long> idCoord = new LongHashSet(field.id, row.id)
                String cellValue = convertToString(decisionTable.getCellById(idCoord, coord, [:], null, true))
                if (hasContent(cellValue))
                {
                    cellValue -= BANG
                    Iterable<String> values = COMMA_SPLITTER.split(cellValue)

                    for (String value : values)
                    {
                        if (!axis.findColumn(value))
                        {
                            blowout.addColumn(axis.name, value)
                        }
                    }
                }
            }
            blowout.addColumn(axis.name, null)
        }
        return blowout
    }

    private static class BlowoutCell
    {
        private static IntSet blank = new IntOpenHashSet()
        List<List<Range>> ranges = []
        IntSet priorities = blank  // This field is tested (empty), then always overwritten
    }

    /**
     * Validate whether any input rules in the DecisionTable overlap.  An overlap would happen if
     * the same input to the DecisionTable caused two (2) or more rows to be returned.
     * @param blowout NCube that has an Axis per each DISCRETE input_value column in the DecisionTable.
     * @param rows List<Column> all rows (Column instances) from the Decision table row axis.
     * @return Set<Comparable> row pointers (or empty Set in the case of no errors), of rows in a DecisionTable that
     * conflict.
     */
    private Set<Comparable> validateDecisionTableRows(NCube blowout, List<Column> rows)
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Map<String, Comparable> coord = new CaseInsensitiveMap<>(Collections.emptyMap(), new HashMap<>())
        Set<Comparable> badRows = new CaseInsensitiveSet<>()
        Column ignoreColumn = fieldAxis.findColumn(IGNORE)
        Column priorityColumn = fieldAxis.findColumn(PRIORITY)
        int[] startCounters = new int[inputKeys.size() - rangeKeys.size()]
        int[] counters = new int[startCounters.length]
        boolean anyRanges = rangeColumns.size() > 0
        boolean anyDiscretes = inputColumns.size() > rangeColumns.size()
        
        // Caches to dramatically reduce memory footprint while this method is executing
        Map<List<Range>, List<Range>> internedLists = new Object2ObjectOpenHashMap<>()
        Map<Range, Range> internedRanges = new Object2ObjectOpenHashMap<>()
        Map<IntSet, IntSet> internedIntSets = new Object2ObjectOpenHashMap<>()
        Map<Comparable, Comparable> primitives = new Object2ObjectOpenHashMap<>()

        Set<String> colsToProcess = new CaseInsensitiveSet<>(inputColumns)
        colsToProcess.removeAll(rangeColumns)

        Set<String> inputKeysCopy = new CaseInsensitiveSet<>(inputKeys)
        inputKeysCopy.removeAll(rangeKeys)
        String[] axisNames = inputKeysCopy as String[]

        String[] indexToRangeName = new String[rangeKeys.size()]
        int index = 0
        for (String rangeName : rangeKeys)
        {
            indexToRangeName[index++] = rangeName
        }

        for (Column row : rows)
        {
            Comparable rowValue = row.value
            Long rowId = row.id
            coord.put(rowAxisName, rowValue)

            if (ignoreColumn)
            {
                coord.put(fieldAxisName, IGNORE)
                Set<Long> idCoord = new LongHashSet(rowId, ignoreColumn.id)
                if (decisionTable.getCellById(idCoord, coord, [:], null, true))
                {
                    continue
                }
            }

            Map<String, List<Comparable>> bindings = getImpliedCells(blowout, row, fieldAxis, colsToProcess)
            int priority = getPriority(coord, rowId, priorityColumn)
            System.arraycopy(startCounters, 0, counters, 0, startCounters.length)
            Map<String, Range> rowRanges = getRowRanges(coord, rowId, priority, internedRanges, primitives)
            boolean done = false
            Set<Long> ids = new HashSet<>()
            Map<String, ?> coordinate = new HashMap<>()

            // Loop written this way because do-while loops are not in Groovy until version 3
            while (!done)
            {
                ids.clear()

                int idx = 0
                for (String axisName : axisNames)
                {   // this loop skipped if there are no discrete variables (range(s) only)
                    int radix = counters[idx++]
                    Comparable value = bindings.get(axisName).get(radix)
                    coordinate.put(axisName, value)
                    ids.add(blowout.getAxis(axisName).findColumn(value).id)
                }

                Set<Long> cellPtr = new LongHashSet(ids)
                boolean areDiscretesUnique = false
                boolean areRangesGood = false

                // Grab the blowoutCell (List<List<Range>>)
                BlowoutCell cell = blowout.getCellById(cellPtr, coordinate, [:], null, true)
                if (cell == null)
                {
                    cell = new BlowoutCell()
                    blowout.setCellById(cell, cellPtr, true)
                }

                if (anyRanges)
                {   // get all Ranges for row, track them by name
                    areRangesGood = checkRowRangesForOverlap(cell, indexToRangeName, rowRanges, internedLists)
                    done = true
                }

                if (anyDiscretes)
                {
                    areDiscretesUnique = checkDiscretesForOverlap(cell, priority, internedIntSets)
                    done = !incrementVariableRadixCount(counters, bindings, axisNames)
                }

                if (!areDiscretesUnique && !areRangesGood)
                {
                    badRows.add(rowValue)
                }
            }
        }
        return badRows
    }

    /**
     * Check the passed in rowRanges against the 'ranges' passed for complete overlap (for the given cellPtr).
     * If there is complete overlap, return false, otherwise true.
     * Example of List<List<Ranges>>:
     *             age           salary
     *     [0] [Range(0, 16),  Range(0, 40000)]
     *     [1] [Range(16, 35), Range(0, 40000)]
     *     [2] [Range(35, 70), Range(0, 40000)]
     *
     * The outer List is the length of the number of unique ranges encountered per cellPtr (unique coordinate
     * representing all discrete decision variables).  The inner lists are prior row's ranges that have been
     * encountered, that were non-overlapping - meaning they did not overlap this list (unique in at least one
     * range [age or salary, ...]
     *
     * Although this method can indict a row's ranges (saying that they overlap another row elsewhere in the
     * DecisionTable), the associated discrete variables can be unique (unique cellPtr), thereby the row is
     * still good.
     */
    private static boolean checkRowRangesForOverlap(BlowoutCell cell, String[] indexToRangeName, Map<String, Range> rowRanges, Map<List<Range>, List<Range>> internedLists)
    {
        int len = cell.ranges.size()
        List existingRanges = cell.ranges

        for (int i=0; i < len; i++)
        {   // Loop through however many the table has grown too (a function of how many unique ranges appear).
            List<Range> existingRange = existingRanges.get(i)
            int len2 = existingRange.size()
            boolean good = false

            for (int j=0; j < len2; j++)
            {   // Loop through all range variables ranges (age, salary, date)
                String rangeName = indexToRangeName[j]
                Range range = existingRange.get(j)

                if (!range.overlap(rowRanges.get(rangeName)))
                {   // If any one range doesn't overlap, then this List of ranges is OK against another existing List of ranges
                    good = true
                    break
                }
            }

            if (!good)
            {   // Short-circuit - no need to test further, overlap found.
                return false
            }
        }

        List<Range> list = new ArrayList<>(rowRanges.values())
        existingRanges.add(internList(list, internedLists))
        return true
    }

    /**
     * Get all ranges in a given row of the decision table
     * @param Coord Map<String, ?> specifying the n-cube coordinate.  The 'rowAxisName' must already
     * be set before calling this method.
     * @return Map that maps a range name to its ranges on a given row.
     */
    private Map<String, Range> getRowRanges(Map<String, ?> coord, long rowId, int priority, Map<Range, Range> internedRanges, Map<Comparable, Comparable> primitives)
    {
        Map<String, Range> ranges = new CaseInsensitiveMap<>()

        for (String rangeName : rangeKeys)
        {
            Range bounds = inputVarNameToRangeColumns.get(rangeName)
            Column lowColumn = (Column) bounds.low
            coord.put(fieldAxisName, lowColumn.value)
            Set<Long> idCoord = new LongHashSet(lowColumn.id, rowId)
            Range range = new Range()
            range.low = (Comparable) decisionTable.getCellById(idCoord, coord, [:], null, true)

            Column highColumn = (Column) bounds.high
            coord.put(fieldAxisName, highColumn.value)
            idCoord = new LongHashSet(highColumn.id, rowId)
            range.high = (Comparable) decisionTable.getCellById(idCoord, coord, [:], null, true)
            range.priority = priority

            ranges.put(rangeName, internRange(range, internedRanges, primitives))
        }
        return ranges
    }

    /**
     * Get the implied cells in the blowout NCube based on a row in the DecisionTable.
     * @param fieldAxis Axis representing the decision table columns
     * @param row Column from the Row axis in a DecisionTable.
     * @param blowout NCube that has an Axis per each DISCRETE input_value column in the DecisionTable.
     * @return Map<String, List<String>> representing all the discrete input values used for the row, or implied
     * by the row in the case blank (*) or ! (exclusion) is used.
     */
    private Map<String, List<Comparable>> getImpliedCells(NCube blowout, Column row, Axis fieldAxis, Set<String> colsToProcess)
    {
        Map<String, List<Comparable>> bindings = new Object2ObjectOpenHashMap()
        Map<String, ?> coord = new CaseInsensitiveMap<>()
        coord.put(rowAxisName, row.value)

        for (String colValue : colsToProcess)
        {
            List<Comparable> faces = []
            bindings.put(colValue, faces)
            Column field = fieldAxis.findColumn(colValue)
            Set<Long> idCoord = new LongHashSet(field.id, row.id)
            coord.put(fieldAxisName, field.value)
            String cellValue = convertToString(decisionTable.getCellById(idCoord, coord, [:], null, true))

            if (hasContent(cellValue))
            {
                boolean exclude = cellValue.startsWith(BANG)
                cellValue -= BANG
                Iterable<String> values = COMMA_SPLITTER.split(cellValue)

                if (exclude)
                {   // Not the value or list we are looking for (implies all other values)
                    List<Column> columns = blowout.getAxis(colValue).columns
                    for (Column column : columns)
                    {
                        String columnValue = column.value
                        if (!values.contains(columnValue))
                        {
                            faces.add(columnValue)
                        }
                    }
                }
                else
                {   // Value or values to check
                    for (String value : values)
                    {
                        faces.add(value)
                    }
                }
            }
            else
            {   // Empty cell in input_value column implies all possible values
                List<Column> columns = blowout.getAxis(colValue).columns
                for (Column column : columns)
                {
                    String columnValue = column.value
                    faces.add(columnValue)
                }
            }
        }
        return bindings
    }

    /**
     * Fetch the 'int' priority value from the 'Priority' column, if it exists.  If not, then
     * return INTEGER.MAX_VALUE as the priority (lowest).
     */
    private int getPriority(Map<String, ?> coord, long rowId, Column priorityColumn)
    {
        if (priorityColumn)
        {
            coord.put(fieldAxisName, priorityColumn.value)
            Set<Long> idCoord = new LongHashSet(priorityColumn.id, rowId)
            return decisionTable.getCellById(idCoord, coord, [:], null, true)
        }
        else
        {
            return Integer.MAX_VALUE
        }
    }

    /**
     * Fill the passed in cell with a RangeSet containing the passed in 'priority' value.
     * If there is already a RangeSet with priorities there, and it already contains the same priority alue, then
     * return false (we've identified a duplicate rule in the DecisionTable).   If the RangeSet is there, but
     * it does not contain the same priority passed in, add it.
     */
    private static boolean checkDiscretesForOverlap(BlowoutCell cell, int priority, Map<IntSet, IntSet> internedIntSets)
    {
        if (cell.priorities.contains(priority))
        {
            return false
        }
        else
        {
            // "TIntHashSet" pulled from blowout cell duplicated so that the interned version is not modified directly.
            IntSet copy = new IntOpenHashSet()
            IntIterator i = cell.priorities.iterator()
            while (i.hasNext())
            {
                copy.add(i.nextInt())
            }
            copy.add(priority)
            cell.priorities = internSet(copy, internedIntSets)
            return true
        }
    }

    /**
     * Ensure required input key/values are supplied.  If required values are missing, IllegalArgumentException is
     * thrown.
     * @param input Map containing decision variable name/value pairs.
     */
    private void ensuredRequiredInputs(Map<String, ?> input)
    {
        if (!input.keySet().containsAll(requiredColumns))
        {
            Set<String> requiredCopy = new CaseInsensitiveSet<>(requiredColumns)
            requiredCopy.removeAll(input.keySet())
            throw new IllegalArgumentException("Required input keys: ${requiredCopy} not found, decision table: ${decisionTable.name}")
        }
    }

    /**
     * Get the range spec Map out of the 'ranges' map by input variable name.  If not there, create a new
     * empty range spec Map and place it there.
     */
    private static Map<String, ?> getRangeSpec(Map<String, Map<String, ?>> ranges, String inputVarName)
    {
        Map<String, ?> spec
        if (ranges.containsKey(inputVarName))
        {
            spec = (Map<String, ?>) ranges.get(inputVarName)
        }
        else
        {
            spec = new CaseInsensitiveMap<>()
            ranges.put(inputVarName, spec)
        }
        return spec
    }

    /**
     * Convert the passed in value to the data-type specified.
     */
    private Comparable convertDataType(Comparable value, String dataType)
    {
        if (value == null)
        {
            return null
        }
        if (dataType.equalsIgnoreCase('DATE'))
        {
            return convertToDate(value)
        }
        else if (dataType.equalsIgnoreCase('LONG'))
        {
            return convertToLong(value)
        }
        else if (dataType.equalsIgnoreCase('DOUBLE'))
        {
            return convertToDouble(value)
        }
        else if (dataType.equalsIgnoreCase('BIG_DECIMAL'))
        {
            return convertToBigDecimal(value)
        }
        else if (dataType.equalsIgnoreCase('STRING'))
        {
            return convertToString(value)
        }
        else if (dataType.equalsIgnoreCase('BOOLEAN'))
        {
            return convertToBoolean(value)
        }
        throw new IllegalStateException("Data type must be one of: DATE, LONG, DOUBLE, BIG_DECIMAL, STRING, or BOOLEAN. Data type: ${dataType}, value: ${value}, decision table: ${decisionTable.name}")
    }

    /**
     * Re-use Range instances.
     */
    private static Range internRange(Range candidate, Map<Range, Range> internedRanges, Map<Comparable, Comparable> primitives)
    {
        Range internedRange = internedRanges.get(candidate)
        if (internedRange != null)
        {
            return internedRange
        }

        Comparable low = primitives.get(candidate.low)
        if (low != null)
        {
            candidate.low = low
        }
        else
        {
            primitives.put(candidate.low, candidate.low)
        }

        Comparable high = primitives.get(candidate.high)
        if (high != null)
        {
            candidate.high = high
        }
        else
        {
            primitives.put(candidate.high, candidate.high)
        }

        internedRanges.put(candidate, candidate)
        return candidate
    }

    /**
     * Re-use Set<Integer> instances.
     */
    private static IntSet internSet(IntSet candidate, Map<IntSet, IntSet> internedSets)
    {
        IntSet internedSet = internedSets.get(candidate)
        if (internedSet != null)
        {
            return internedSet
        }
        internedSets.put(candidate, candidate)
        return candidate
    }

    /**
     * Re-use List<Range> instances
     */
    private static List<Range> internList(List<Range> candidate, Map<List<Range>, List<Range>> internedLists)
    {
        List<Range> internedList = internedLists.get(candidate)
        if (internedList != null)
        {
            return internedList
        }
        internedLists.put(candidate, candidate)
        return candidate
    }

    /**
     * Increment the variable radix number passed in.  The number is represented by an Object[].
     * @return false if more incrementing can be done, otherwise true.
     */
    private static boolean incrementVariableRadixCount(int[] counters,
                                                       final Map<String, List<Comparable>> bindings,
                                                       final String[] axisNames)
    {
        int digit = axisNames.length - 1

        while (true)
        {
            final String axisName = axisNames[digit]
            final int count = counters[digit]
            final List<Comparable> cols = bindings.get(axisName)

            if (count >= cols.size() - 1)
            {   // Reach max value for given dimension (digit)
                if (digit == 0)
                {   // we have reached the max radix for the most significant digit - we are done
                    return false
                }
                counters[digit--] = 0
            }
            else
            {
                counters[digit] = count + 1
                return true
            }
        }
    }
}
