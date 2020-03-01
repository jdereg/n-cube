package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.LongHashSet
import com.cedarsoftware.ncube.util.RangeSpec
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
    private Set<String> outputColumns = new HashSet<>()
    private Set<String> rangeColumns = new HashSet<>()
    private static final String BANG = '!'
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings()
    private static final Splitter BAR_SPLITTER = Splitter.on('|').trimResults().omitEmptyStrings()

    DecisionTable(NCube decisionCube)
    {
        decisionTable = decisionCube.duplicate("${decisionCube.name}.copy")
        verifyAndCache()
    }
    
    /**
     * Main API for querying a Decision Table.
     * @param input Map containing key/value pairs for all the input_value columns
     * @return List<Comparable, List<outputs>>
     */
    Map<Comparable, ?> getDecision(Map<String, ?> input)
    {
        Map<String, Map<String, ?>> ranges = new HashMap<>()
        Map<String, ?> copyInput = new CaseInsensitiveMap<>(input)
        copyInput.put(IGNORE, null)
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        ensuredRequiredInputs(copyInput, fieldAxis)

        for (String colValue : rangeColumns)
        {
            Column column = fieldAxis.findColumn(colValue)
            Map colMetaProps = column.metaProperties

            if (colMetaProps.containsKey(INPUT_LOW))
            {   // Allow ranges to be processed in ANY order (even intermixed with other ranges)
                String inputVarName = colMetaProps.get(INPUT_LOW)
                Map<String, ?> spec = getRangeSpec(ranges, inputVarName)
                spec.put(INPUT_LOW, column.value)

                if (!spec.containsKey(INPUT_VALUE))
                {   // Last range post (high or low) processed, sets the input_value, data_type, and copies the range spec to the input map.
                    // ["date": date instance] becomes ("date": [low:1900, high:2100, input_value: date instance, data_type: DATE])
                    Comparable value = convertDataType((Comparable)copyInput.get(inputVarName), (String)colMetaProps.get(DATA_TYPE))
                    spec.put(INPUT_VALUE, value)
                    spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                    copyInput.put(inputVarName, spec)
                }
            }

            if (colMetaProps.containsKey(INPUT_HIGH))
            {   // Allow ranges to be processed in ANY order (even intermixed with other ranges)
                String inputVarName = colMetaProps.get(INPUT_HIGH)
                Map<String, ?> spec = getRangeSpec(ranges, inputVarName)
                spec.put(INPUT_HIGH, column.value)
                if (!spec.containsKey(INPUT_VALUE))
                {
                    Comparable value = convertDataType((Comparable)copyInput.get(inputVarName), (String)colMetaProps.get(DATA_TYPE))
                    spec.put(INPUT_VALUE, value)
                    spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                    copyInput.put(inputVarName, spec)
                }
            }
        }
        
        Map options = [
                (NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): inputColumns,
                (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): outputColumns,
                input: [dvs:copyInput]
        ]

        Map<Comparable, ?> result = decisionTable.mapReduce(fieldAxisName, decisionTableClosure, options)
        result = determinePriority(result)
        println result
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
     * then there are no overlapping rules.
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
     * andy input value passed.
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
        
        for (String colValue : inputColumns)
        {
            if (rangeColumns.contains(colValue))
            {
                continue
            }

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
                    if (convertToBoolean(decVarValue))
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

                    if (value == null)
                    {   // null is never between ranges
                        return false
                    }
                    if (value.compareTo(low) < 0)
                    {
                        return false
                    }
                    else if (value.compareTo(high) >= 0)
                    {
                        return false
                    }
                    continue
                }

                // 3. Check discrete decision variables
                String cellValue = convertToString(decVarValue)
                if (cellValue == null)
                {
                    cellValue = ''
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
            Set<String> keys = new HashSet<>(column.metaProperties.keySet())
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
                Set<String> keys = new HashSet<>(column.metaProperties.keySet())
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
            throw new IllegalStateException("Decision table: ${decisionTable.name} must have one axis with one or more columns with meta-property key: input_value.")
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
            if (colMetaProps.containsKey(INPUT_VALUE))
            {
                inputColumns.add((String) column.value)
            }

            if (colMetaProps.containsKey(INPUT_LOW) || colMetaProps.containsKey(INPUT_HIGH))
            {
                String dataType = colMetaProps.get(DATA_TYPE)
                if (dataType == null || !['LONG', 'DOUBLE', 'DATE', 'BIG_DECIMAL'].contains(dataType.toUpperCase()))
                {
                    throw new IllegalStateException("Range columns must have 'data_type' meta-property set, ncube: ${decisionTable.name}, column: ${column.value}. Valid values are DATE, LONG, DOUBLE, BIG_DECIMAL.")
                }
                inputColumns.add((String) column.value)
                rangeColumns.add((String) column.value)
                RangeSpec rangeSpec
                String inputVarName
                
                if (colMetaProps.containsKey(INPUT_LOW))
                {
                    inputVarName = colMetaProps.get(INPUT_LOW)
                    rangeSpec = rangeSpecs.get(inputVarName)
                    if (rangeSpec == null)
                    {
                        rangeSpec = new RangeSpec()
                    }
                    if (rangeSpec.lowColumnName)
                    {
                        throw new IllegalStateException("More than one low range column with same input variable name found: ${INPUT_LOW}. Each range variable should have a unique name.")
                    }
                    rangeSpec.lowColumnName = column.value
                }
                else
                {
                    inputVarName = colMetaProps.get(INPUT_HIGH)
                    rangeSpec = rangeSpecs.get(inputVarName)
                    if (rangeSpec == null)
                    {
                        rangeSpec = new RangeSpec()
                    }
                    if (rangeSpec.highColumnName)
                    {
                        throw new IllegalStateException("More than one high range column with same input variable name found: ${INPUT_LOW}. Each range variable should have a unique name.")
                    }
                    rangeSpec.highColumnName = column.value
                }
                rangeSpec.dataType = dataType
                rangeSpecs.put(inputVarName, rangeSpec)
            }

            if (colMetaProps.containsKey(INPUT_HIGH))
            {
                if (!colMetaProps.containsKey(DATA_TYPE))
                {
                    throw new IllegalStateException("Range columns must have 'data_type' meta-property set, ncube: ${decisionTable.name}, column: ${column.value}. Valid values are DATE, LONG, DOUBLE, BIG_DECIMAL.")
                }
                inputColumns.add((String) column.value)
                rangeColumns.add((String) column.value)
            }

            if (colMetaProps.containsKey(OUTPUT_VALUE))
            {
                outputColumns.add((String) column.value)
            }
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

        for (String colValue : inputColumns)
        {
            if (rangeColumns.contains(colValue))
            {
                continue
            }

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

    private Set<Comparable> validateDecisionTableRows(NCube blowout, List<Column> rows)
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Map<String, Comparable> coord = [:]
        Set<Comparable> badRows = []
        Column ignoreColumn = fieldAxis.findColumn(IGNORE)
        Column priorityColumn = fieldAxis.findColumn(PRIORITY)
        Set<String> colsToProcess = new CaseInsensitiveSet<>(inputColumns)
        colsToProcess.removeAll(rangeColumns)
        
        for (Column row : rows)
        {
            Comparable rowValue = row.value
            coord.put(rowAxisName, rowValue)

            if (ignoreColumn)
            {
                coord.put(fieldAxisName, IGNORE)
                Set<Long> idCoord = new LongHashSet([row.id, ignoreColumn.id] as Set)
                if (convertToBoolean(decisionTable.getCellById(idCoord, coord, [:])))
                {
                    continue
                }
            }

            Map<String, Integer> counters = [:]
            Map<String, List<String>> bindings = [:]

            for (String colValue : colsToProcess)
            {
                coord.put(fieldAxisName, colValue)
                Column field = fieldAxis.findColumn(colValue)
                counters[colValue] = 1
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

            Range range = buildRange(row.id, rowValue, priorityColumn)
            String[] axisNames = bindings.keySet() as String[]

            populateCachedNCube(badRows, blowout, counters, bindings, axisNames, range, rowValue) // call this method once before the counter gets turned over
            while (incrementVariableRadixCount(counters, bindings, axisNames))
            {
                populateCachedNCube(badRows, blowout, counters, bindings, axisNames, range, rowValue)
            }
        }
        return badRows
    }

    private Range buildRange(long rowId, Comparable rowValue, Column priorityColumn)
    {
        Range range = new Range()
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Map<String, Comparable> coord = [(rowAxisName): rowValue]

        // TODO - add support for validating multiple ranges
        if (rangeColumns.size() > 2)
        {
            throw new IllegalStateException("Multiple ranges not yet supported, ncube: ${decisionTable.name}")
        }

        for (String colValue : rangeColumns)
        {
            Column column = fieldAxis.findColumn(colValue)
            Map colMetaProps = column.metaProperties
            String dataType = colMetaProps.get(DATA_TYPE)
            Set<Long> idCoord = new LongHashSet([column.id, rowId] as Set)
            coord.put(fieldAxisName, colValue)

            if (colMetaProps.containsKey(INPUT_LOW))
            {
                range.low = getRangeValue(dataType, coord, idCoord)
            }
            else if (colMetaProps.containsKey(INPUT_HIGH))
            {
                range.high = getRangeValue(dataType, coord, idCoord)
            }
        }

        if (priorityColumn)
        {
            coord.put(fieldAxisName, priorityColumn.value)
            Set<Long> idCoord = new LongHashSet([rowId, priorityColumn.id] as Set)
            Object cellValue = decisionTable.getCellById(idCoord, coord, [:])
            range.priority = convertToInteger(cellValue)
        }

        return range
    }

    private Comparable getRangeValue(String dataType, Map coord, Set<Long> idCoord)
    {
        Object cellValue = decisionTable.getCellById(idCoord, coord, [:])
        Comparable rangeValue

        if (dataType.equalsIgnoreCase('DATE'))
        {
            rangeValue = convertToDate(cellValue)
        }
        else if (dataType.equalsIgnoreCase('LONG'))
        {
            rangeValue = convertToLong(cellValue)
        }
        else if (dataType.equalsIgnoreCase('DOUBLE'))
        {
            rangeValue = convertToDouble(cellValue)
        }
        else if (dataType.equalsIgnoreCase('BIG_DECIMAL'))
        {
            rangeValue = convertToBigDecimal(cellValue)
        }
        else
        {
            throw new IllegalStateException("Range data type must be one of: DATE, LONG, DOUBLE, BIG_DECIMAL, ncube: ${decisionTable.name}")
        }

        return rangeValue
    }

    private static void populateCachedNCube(Set<Comparable> badRows, NCube blowout, Map<String, Integer> counters, Map<String, List<String>> bindings, String[] axisNames, Range candidate, Comparable rowId)
    {
        String candidateOut = candidate.out()
        Map<String, Object> coordinate = [:]
        Set<Long> ids = new HashSet<>()
        
        for (String key : axisNames)
        {
            int radix = counters[key]
            String value = bindings.get(key)[radix - 1]
            coordinate.put(key, value)
            ids.add(blowout.getAxis(key).findColumn(value).id)
        }

        boolean goodCoordinate = true
        Set<Long> idCoord = new LongHashSet(ids)
        String existingValue = blowout.getCellById(idCoord, coordinate, [:])
        
        if (existingValue != null)
        {
            Iterable<String> entryStrings = BAR_SPLITTER.split(existingValue)
            for (String entryString : entryStrings)
            {
                Range existingEntry = new Range(entryString)
                if (candidateOut == existingEntry.out())
                {
                    goodCoordinate = false
                }
                else
                {
                    boolean nullRange = existingEntry.low == null && existingEntry.high == null && candidate.low == null && candidate.high == null
                    boolean equalPriorities = existingEntry.priority == candidate.priority
                    if ((nullRange || existingEntry.overlap(candidate)) && equalPriorities)
                    {
                        goodCoordinate = false
                    }
                }

                if (!goodCoordinate)
                {
                    badRows.add(rowId)
                    break
                }
            }
        }
        else
        {
            existingValue = ''
        }

        if (goodCoordinate)
        {
            String newValue = "${candidateOut}|${existingValue}"
            blowout.setCellById(newValue, idCoord)
        }
    }

    /**
     * Ensure required input key/values are supplied.  If required values are missing, IllegalArgumentException is
     * thrown.
     * @param input Map containing decision variable name/value pairs.
     */
    private void ensuredRequiredInputs(Map<String, ?> input, Axis fieldAxis)
    {
        for (Column column : fieldAxis.columnsWithoutDefault)
        {
            if (column.metaProperties.containsKey(REQUIRED) && convertToBoolean(column.getMetaProperty(REQUIRED)))
            {
                if (column.getMetaProperty(INPUT_VALUE))
                {
                    if (!input.containsKey(column.value))
                    {
                        throw new IllegalArgumentException("Required input: ${column.value} not found, decision table: ${decisionTable.name}")
                    }
                }
                else
                {
                    String colName = column.getMetaProperty(INPUT_LOW)
                    if (!colName)
                    {
                        colName = column.getMetaProperty(INPUT_HIGH)
                    }
                    if (colName)
                    {
                        if (!input.containsKey(colName))
                        {
                            throw new IllegalArgumentException("Required range input: ${colName} not found on input, decision table: ${decisionTable.name}")
                        }
                    }
                }
            }
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
            spec = new HashMap<>()
            ranges.put(inputVarName, spec)
        }
        return spec
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
