package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.LongHashSet
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.StringUtilities
import com.google.common.base.Splitter
import groovy.transform.CompileStatic

import static com.cedarsoftware.ncube.NCubeConstants.DATA_TYPE
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
    private Set<String> inputColumns = new HashSet<>()
    private Set<String> inputKeys = new HashSet<>()
    private Set<String> outputColumns = new HashSet<>()
    private Set<String> rangeColumns = new HashSet<>()
    private Set<String> requiredColumns = new HashSet<>()
    private static final String BANG = '!'
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings()

    DecisionTable(NCube decisionCube)
    {
        decisionTable = decisionCube.duplicate("${decisionCube.name}.copy")
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
        Set<String> sortedKeys = new TreeSet<>(CASE_INSENSITIVE_ORDER)
        sortedKeys.addAll(inputKeys)
        return sortedKeys
    }

    /**
     * @return Set<String> All required keys (meta-property key REQUIRED value of true on a field column) that
     * must be passed to the DecisionTable.getDecision() API.
     */
    Set<String> getRequiredKeys()
    {
        Set<String> sortedKeys = new TreeSet<>(CASE_INSENSITIVE_ORDER)
        sortedKeys.addAll(requiredColumns)
        return sortedKeys
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

        Set<String> colsToSearch = new CaseInsensitiveSet<>(inputColumns)
        // OK to add IGNORE column, even if axis does not have it (mapReduce() is now forgiving on this)
        colsToSearch.add(IGNORE)
        Map<String, ?> closureInput = new CaseInsensitiveMap<>(input)
        closureInput.dvs = copyInput

        Map options = [
                (NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): colsToSearch,
                (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): outputColumns,
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
        Set<String> discreteInputCols = inputColumns - rangeColumns
        
        for (String colValue : discreteInputCols)
        {
            Column field = fieldAxis.findColumn(colValue)
            String fieldValue = field.value
            Set<String> values = new TreeSet<>(CASE_INSENSITIVE_ORDER)

            for (Column row : rows)
            {
                String rowValue = row.value
                coord.put(fieldAxisName, fieldValue)
                coord.put(rowAxisName, rowValue)
                Set<Long> idCoord = new LongHashSet([field.id, row.id] as Set)
                String cellValue = convertToString(decisionTable.getCellById(idCoord, coord, [:]))
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
    private Closure getDecisionTableClosure()
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
                if (colMetaProps.containsKey(REQUIRED))
                {
                    requiredColumns.add(columnValue)
                }
            }

            if (colMetaProps.containsKey(INPUT_LOW) || colMetaProps.containsKey(INPUT_HIGH))
            {
                String dataType = colMetaProps.get(DATA_TYPE)
                if (dataType == null || !['LONG', 'DOUBLE', 'DATE', 'BIG_DECIMAL'].contains(dataType.toUpperCase()))
                {
                    throw new IllegalStateException("Range columns must have 'data_type' meta-property set, column: ${columnValue}, ncube: ${decisionTable.name}. Valid values are DATE, LONG, DOUBLE, BIG_DECIMAL.")
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
                {
                    requiredColumns.add(columnValue)
                }
            }
            if (colMetaProps.containsKey(OUTPUT_VALUE))
            {
                outputColumns.add(columnValue)
            }
        }

        Set<String> requiredNonInput = requiredColumns - inputKeys
        if (!requiredNonInput.empty)
        {
            throw new IllegalStateException("REQUIRED meta-property found on columns that are not input_value, input_low, or input_high. These were: ${requiredNonInput}, ncube: ${decisionTable.name}")
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

        // Convert all range values in the table to the data type specified on the column meta-property
        Axis rowAxis = decisionTable.getAxis(rowAxisName)
        Map<String, ?> coord = new CaseInsensitiveMap<>()

        for (String colName : rangeColumns)
        {
            Column limitCol = fieldAxis.findColumn(colName)
            String dataType = limitCol.getMetaProperty(DATA_TYPE)
            coord.put(fieldAxisName, limitCol.value)
            
            for (Column row : rowAxis.columnsWithoutDefault)
            {
                coord.put(rowAxisName, row.value)
                Set<Long> idCoord = new LongHashSet([limitCol.id, row.id] as HashSet)
                Object value = decisionTable.getCellById(idCoord, coord, [:])
                if (value == null || !(value instanceof Comparable))
                {
                    throw new IllegalStateException("Values in range column must be instanceof Comparable, row ${row.value}, field: ${limitCol.value}, ncube: ${decisionTable.name}")
                }
                Comparable limit = (Comparable) value
                decisionTable.setCellById(convertDataType(limit, dataType), idCoord)
            }
        }
        convertSpecialColumnsToPrimitive()
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
                Set<Long> idCoord = new LongHashSet([ignoreColId, row.id] as HashSet)
                Object value = decisionTable.getCellById(idCoord, coord, [:])
                decisionTable.setCellById(convertToBoolean(value), idCoord)
            }

            if (priorityColId != -1)
            {
                Set<Long> idCoord = new LongHashSet([priorityColId, row.id] as HashSet)
                Integer intValue = convertToInteger(decisionTable.getCellById(idCoord, coord, [:]))
                if (intValue < 1)
                {   // If priority is not specified, then it is the lowest priority of all
                    intValue = Integer.MAX_VALUE
                }
                decisionTable.setCellById(intValue, idCoord)
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
        Set<String> discreteCols = inputColumns - rangeColumns

        for (String colValue : discreteCols)
        {
            Column field = fieldAxis.findColumn(colValue)
            String fieldValue = field.value
            Axis axis = new Axis(fieldValue, AxisType.DISCRETE, AxisValueType.CISTRING, false)
            blowout.addAxis(axis)

            for (Column row : rows)
            {
                String rowValue = row.value
                coord.put(fieldAxisName, fieldValue)
                coord.put(rowAxisName, rowValue)
                Set<Long> idCoord = new LongHashSet([field.id, row.id] as Set)
                String cellValue = convertToString(decisionTable.getCellById(idCoord, coord, [:]))
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

    /**
     * @param blowout NCube that has an Axis per each DISCRETE input_value column in the DecisionTable.
     * @param rows List<Column> all rows (Column instances) from the Decision table row axis.
     * @return Set<Comparable> row pointers (or empty Set in the case of no errors), of rows in a DecisionTable that
     * conflict.
     */
    private Set<Comparable> validateDecisionTableRows(NCube blowout, List<Column> rows)
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Map<String, Comparable> coord = [:]
        Set<Comparable> badRows = []
        Column ignoreColumn = fieldAxis.findColumn(IGNORE)
        Column priorityColumn = fieldAxis.findColumn(PRIORITY)
        Set<String> colsToProcess = inputColumns - rangeColumns
        Map<String, Integer> startCounters = new HashMap<>()
        
        for (String colValue : colsToProcess)
        {
            startCounters.put(colValue, 1)
        }

        for (Column row : rows)
        {
            Comparable rowValue = row.value
            coord.put(rowAxisName, rowValue)

            if (ignoreColumn)
            {
                coord.put(fieldAxisName, IGNORE)
                Set<Long> idCoord = new LongHashSet([row.id, ignoreColumn.id] as Set)
                if (decisionTable.getCellById(idCoord, coord, [:]))
                {
                    continue
                }
            }

            Map<String, List<String>> bindings = getImpliedCells(colsToProcess, fieldAxis, row, blowout)
            Range range = buildRange(row.id, rowValue, priorityColumn)
            String[] axisNames = bindings.keySet() as String[]
            Map<String, Integer> counters = new HashMap<>(startCounters)

            if (axisNames.length > 0)
            {   // No discrete variables, only ranges.
                boolean done = false
                Set<Long> ids = new HashSet<>()
                Map<String, ?> coordinate = new HashMap<>()

                // Loop written this way because do-while loops are not in Groovy until version 3
                while (!done)
                {
                    ids.clear()

                    for (String key : axisNames)
                    {
                        int radix = counters.get(key)
                        String value = bindings.get(key).get(radix - 1)
                        coordinate.put(key, value)
                        ids.add(blowout.getAxis(key).findColumn(value).id)
                    }

                    if (!populateRangeTableCell(blowout, new LongHashSet(ids), coordinate, range))
                    {
                        badRows.add(rowValue)
                    }
                    done = !incrementVariableRadixCount(counters, bindings, axisNames)
                }
            }
        }
        return badRows
    }

    /**
     * Get the implied cells in the blowout NCube based on a row in the DecisionTable.
     * @param colsToProcess Set<String> of DISCRETE input_value column names
     * @param fieldAxis Axis representing the decision table columns
     * @param row Column from the Row axis in a DecisionTable.
     * @param blowout NCube that has an Axis per each DISCRETE input_value column in the DecisionTable.
     * @return Map<String, List<String>> representing all the discrete input values used for the row, or implied
     * by the row in the case blank (*) or ! (exclusion) is used.
     */
    private Map<String, List<String>> getImpliedCells(Set<String> colsToProcess, Axis fieldAxis, Column row, NCube blowout)
    {
        Map<String, List<String>> bindings = [:]
        Map<String, ?> coord = new CaseInsensitiveMap<>()

        for (String colValue : colsToProcess)
        {
            Column field = fieldAxis.findColumn(colValue)
            bindings.put(colValue, [])
            Set<Long> idCoord = new LongHashSet([row.id, field.id] as Set)
            coord.put(fieldAxisName, field.value)
            String cellValue = convertToString(decisionTable.getCellById(idCoord, coord, [:]))
            if (hasContent(cellValue))
            {
                boolean exclude = cellValue.startsWith(BANG)
                cellValue -= BANG
                Iterable<String> values = COMMA_SPLITTER.split(cellValue)

                if (exclude)
                {
                    List<Column> columns = blowout.getAxis(colValue).columns
                    for (Column column : columns)
                    {
                        String columnValue = column.value
                        if (!values.contains(columnValue))
                        {
                            bindings.get(colValue).add(columnValue)
                        }
                    }
                }
                else
                {
                    for (String value : values)
                    {
                        bindings.get(colValue).add(value)
                    }
                }
            }
            else
            {
                List<Column> columns = blowout.getAxis(colValue).columns
                for (Column column : columns)
                {
                    String columnValue = column.value
                    bindings.get(colValue).add(columnValue)
                }
            }
        }
        return bindings
    }

    private Range buildRange(long rowId, Comparable rowValue, Column priorityColumn)
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Map<String, Comparable> coord = [(rowAxisName): rowValue]

        // TODO - add support for validating multiple ranges
        if (rangeColumns.size() > 2)
        {
            throw new IllegalStateException("Multiple ranges not yet supported, ncube: ${decisionTable.name}")
        }
        Range range = new Range(0, 1)

        for (String colValue : rangeColumns)
        {
            Column column = fieldAxis.findColumn(colValue)
            Map colMetaProps = column.metaProperties
            Set<Long> idCoord = new LongHashSet([column.id, rowId] as Set)
            coord.put(fieldAxisName, colValue)

            if (colMetaProps.containsKey(INPUT_LOW))
            {
                range.low = (Comparable)decisionTable.getCellById(idCoord, coord, [:])
            }
            else if (colMetaProps.containsKey(INPUT_HIGH))
            {
                range.high = (Comparable)decisionTable.getCellById(idCoord, coord, [:])
            }
        }

        if (priorityColumn)
        {
            coord.put(fieldAxisName, priorityColumn.value)
            Set<Long> idCoord = new LongHashSet([rowId, priorityColumn.id] as Set)
            range.priority = decisionTable.getCellById(idCoord, coord, [:])
        }

        return range
    }

    private static boolean populateRangeTableCell(NCube blowout, Set<Long> idCoord, Map<String, ?> coordinate, Range candidate)
    {
        RangeList ranges = blowout.getCellById(idCoord, coordinate, [:])
        if (ranges == null)
        {
            ranges = new RangeList()
            ranges.addRange(candidate)
            blowout.setCellById(ranges, idCoord)
            return true
        }
        else if (ranges.overlaps(candidate))
        {
            return false
        }
        else
        {
            // "Ranges" pulled from cell duplicated so that the interned version is not modified directly.
            // setCellById() below will intern if possible, otherwise this new instance will exist.
            RangeList copy = ranges.duplicate()
            copy.addRange(candidate)
            blowout.setCellById(copy, idCoord)
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
            Set<String> missingKeys = requiredColumns - inputKeys
            throw new IllegalArgumentException("Required input keys: ${missingKeys} not found, decision table: ${decisionTable.name}")
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
        throw new IllegalStateException("Data type must be one of: DATE, LONG, DOUBLE, BIG_DECIMAL. Data type: ${dataType}, value: ${value}, decision table: ${decisionTable.name}")
    }

    /**
     * Increment the variable radix number passed in.  The number is represented by a Map, where the keys are the
     * digit names (axis names), and the values are the associated values for the number.
     * @return false if more incrementing can be done, otherwise true.
     */
    private static boolean incrementVariableRadixCount(final Map<String, Integer> counters,
                                                       final Map<String, List<String>> bindings,
                                                       final String[] axisNames)
    {
        int digit = axisNames.length - 1

        while (true)
        {
            final String axisName = axisNames[digit]
            final int count = counters.get(axisName)
            final List<String> cols = bindings.get(axisName)

            if (count >= cols.size())
            {   // Reach max value for given dimension (digit)
                if (digit == 0)
                {   // we have reached the max radix for the most significant digit - we are done
                    return false
                }
                counters.put(axisNames[digit--], 1)
            }
            else
            {
                counters.put(axisName, count + 1)  // increment counter
                return true
            }
        }
    }
}
