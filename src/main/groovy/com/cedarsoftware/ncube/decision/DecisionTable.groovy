package com.cedarsoftware.ncube.decision

import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.AxisType
import com.cedarsoftware.ncube.AxisValueType
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.Range
import com.cedarsoftware.util.CaseInsensitiveMap
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

/**
 * Decision Table implements a list of rules that filter a variable number of inputs (decision variables) against
 * a list of constraints.
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
        decisionTable = decisionCube
        verifyAndCache()
    }

    /**
     * Main API for querying a Decision Table.
     * @param input Map containing key/value pairs for all the input_value columns
     * @return List<Comparable, List<outputs>>
     */
    Map<Comparable, ?> getDecisionx(Map<String, ?> input)
    {
        Map<String, ?> ranges = new HashMap<>()
        Map<String, ?> copyInput = new CaseInsensitiveMap<>(input)
        copyInput.put(IGNORE, null)
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)

        for (String colValue : rangeColumns)
        {
            Column column = fieldAxis.findColumn(colValue)
            Map colMetaProps = column.metaProperties

            if (colMetaProps.containsKey(INPUT_LOW))
            {
                String assocInputVarName = colMetaProps.get(INPUT_LOW)
                Map<String, ?> spec = new HashMap<>()
                spec.put('low', column.value)
                spec.put('value', copyInput.get(assocInputVarName))
                spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                ranges.put(assocInputVarName, spec)
            }

            if (colMetaProps.containsKey(INPUT_HIGH))
            {
                String assocInputVarName = colMetaProps.get(INPUT_HIGH)
                Map<String, ?> spec = (Map)ranges.get(assocInputVarName)
                spec.put('high', column.value)
                spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                copyInput.put(assocInputVarName, spec)
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

    void ensuredRequiredInputs(Map<String, ?> input)
    {
        for (Column column : decisionTable.getAxis(fieldAxisName).columnsWithoutDefault)
        {
            if (column.metaProperties.containsKey(REQUIRED) && convertToBoolean(column.getMetaProperty(REQUIRED)))
            {
                if (!input.containsKey(column.value))
                {
                    throw new IllegalArgumentException("Required field: ${column.value} not found on input, decision table: ${decisionTable.name}")
                }
            }
        }
    }

    /**
     * Main API for querying a Decision Table.
     * @param input Map containing key/value pairs for all the input_value columns
     * @return List<Comparable, List<outputs>>
     */
    Map<Comparable, ?> getDecision(Map<String, ?> input)
    {
        ensuredRequiredInputs(input)
        Map<String, ?> ranges = new HashMap<>()
        Map<String, ?> copyInput = new CaseInsensitiveMap<>(input)
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        boolean hasIgnoreColumn = fieldAxis.findColumns([(IGNORE): true] as Map).size() > 0
        Map<String, Comparable> coord = [:]
        Map<Comparable, ?> result = new CaseInsensitiveMap<>()

        for (Column row : decisionTable.getAxis(rowAxisName).columnsWithoutDefault)
        {
            Comparable rowValue = row.value
            coord.put(rowAxisName, rowValue)

            if (hasIgnoreColumn)
            {
                coord.put(fieldAxisName, IGNORE)
                if (convertToBoolean(decisionTable.getCell(coord)))
                {   // Skip rows when value in cell inside ignore column evaluates to boolean true.
                    continue
                }
            }

            Map<?, ?> stripe = decisionTable.getMap([row: row.id])
            for (String colName : inputColumns)
            {
                Column column = fieldAxis.findColumn(colName)
                Map colMetaProps = column.metaProperties

                if (colMetaProps.containsKey(INPUT_LOW))
                {
                    String assocInputVarName = colMetaProps.get(INPUT_LOW)
                    Map<String, ?> spec = new HashMap<>()
                    spec.put('low', column.value)
                    spec.put('value', copyInput.get(assocInputVarName))
                    spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                    ranges.put(assocInputVarName, spec)
                }

                if (colMetaProps.containsKey(INPUT_HIGH))
                {
                    String assocInputVarName = colMetaProps.get(INPUT_HIGH)
                    Map<String, ?> spec = (Map) ranges.get(assocInputVarName)
                    spec.put('high', column.value)
                    spec.put(DATA_TYPE, colMetaProps.get(DATA_TYPE))
                    copyInput.put(assocInputVarName, spec)
                }

                String decVarName = colName
                Object decVarValue = stripe.get(decVarName)
                Object inputValue = coord.get(colName)


                // Check range variables
                if (inputValue instanceof Map)
                {
                    Map<String, ?> rangeInfo = (Map<String, ?>) inputValue
                    Comparable low = (Comparable) stripe.get((String) rangeInfo.get('low'))
                    Comparable high = (Comparable) stripe.get((String) rangeInfo.get('high'))
                    Comparable value = (Comparable) rangeInfo.get('value')
                    String dataType = rangeInfo.get(DATA_TYPE)
                    if (StringUtilities.isEmpty(dataType))
                    {
                        throw new IllegalStateException("Range columns must have 'data_type' meta-property set, ncube: ${decisionTable.name}, input variable: ${decVarName}")
                    }

                    if (dataType.equalsIgnoreCase('DATE'))
                    {
                        low = convertToDate(low)
                        high = convertToDate(high)
                        value = convertToDate(value)
                    }
                    else if (dataType.equalsIgnoreCase('LONG'))
                    {
                        low = convertToLong(low)
                        high = convertToLong(high)
                        value = convertToLong(value)
                    }
                    else if (dataType.equalsIgnoreCase('DOUBLE'))
                    {
                        low = convertToDouble(low)
                        high = convertToDouble(high)
                        value = convertToDouble(value)
                    }
                    else if (dataType.equalsIgnoreCase('BIG_DECIMAL'))
                    {
                        low = convertToBigDecimal(low)
                        high = convertToBigDecimal(high)
                        value = convertToBigDecimal(value)
                    }

                    Range range = new Range(low, high)
                    if (!(range.isWithin(value) == 0))
                    {
                        break    // skip row: does not fit within a range
                    }
                    continue    // check next decision variable
                }

                // Check discrete decision variables
                String cellValue = convertToString(decVarValue)
                if (cellValue == null)
                {
                    cellValue = ''
                }
                inputValue = convertToString(inputValue)
                boolean exclude = cellValue.startsWith('!')

                if (exclude)
                {
                    cellValue = cellValue.substring(1)
                }

                List<String> tokens = cellValue.tokenize(', ')

                if (exclude)
                {
                    if (tokens.contains(inputValue))
                    {
                        break   // skip row
                    }
                }
                else
                {
                    if (!tokens.contains(inputValue))
                    {
                        break // skip row
                    }
                }
                // Row matches.  Add it to output.
                result.put(row.id, [:])
            }
        }
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

    Set<Comparable> validateDecisionTable()
    {
        List<Column> rows = decisionTable.getAxis(rowAxisName).columnsWithoutDefault
        NCube blowout = createValidationNCube(rows)
        Set<Comparable> badRows = validateDecisionTableRow(blowout, rows)
        return badRows
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
                // Check special IGNORE row variable
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

                // Check range variables
                if (inputValue instanceof Map)
                {
                    Map<String, ?> rangeInfo = (Map<String, ?>) inputValue
                    Comparable low = (Comparable) rowValues.get((String) rangeInfo.get('low'))
                    Comparable high = (Comparable) rowValues.get((String) rangeInfo.get('high'))
                    Comparable value = (Comparable) rangeInfo.get('value')
                    String dataType = rangeInfo.get(DATA_TYPE)
                    if (StringUtilities.isEmpty(dataType))
                    {
                        throw new IllegalStateException("Range columns must have 'data_type' meta-property set, ncube: ${decisionTable.name}, input variable: ${decVarName}")
                    }

                    if (dataType.equalsIgnoreCase('DATE'))
                    {
                        low = convertToDate(low)
                        high = convertToDate(high)
                        value = convertToDate(value)
                    }
                    else if (dataType.equalsIgnoreCase('LONG'))
                    {
                        low = convertToLong(low)
                        high = convertToLong(high)
                        value = convertToLong(value)
                    }
                    else if (dataType.equalsIgnoreCase('DOUBLE'))
                    {
                        low = convertToDouble(low)
                        high = convertToDouble(high)
                        value = convertToDouble(value)
                    }
                    else if (dataType.equalsIgnoreCase('BIG_DECIMAL'))
                    {
                        low = convertToBigDecimal(low)
                        high = convertToBigDecimal(high)
                        value = convertToBigDecimal(value)
                    }

                    Range range = new Range(low, high)
                    if (!(range.isWithin(value) == 0))
                    {
                        return false    // skip row: does not fit within a range
                    }
                    continue    // check next decision variable
                }

                // Check discrete decision variables
                String cellValue = convertToString(decVarValue)
                if (cellValue == null)
                {
                    cellValue = ''
                }
                inputValue = convertToString(inputValue)
                boolean exclude = cellValue.startsWith('!')

                if (exclude)
                {
                    cellValue = cellValue.substring(1)
                }

                List<String> tokens = cellValue.tokenize(', ')

                if (exclude)
                {
                    if (tokens.contains(inputValue))
                    {
                        return false
                    }
                }
                else
                {
                    if (!tokens.contains(inputValue))
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
        if (hasContent(fieldAxisName) && hasContent(rowAxisName))
        {
            return
        }

        if (decisionTable.numDimensions != 2)
        {
            throw new IllegalStateException("Decision table: ${decisionTable.name} must have 2 axes.")
        }

        Set<String> decisionMetaPropertyKeys = [INPUT_VALUE, INPUT_LOW, INPUT_HIGH, OUTPUT_VALUE, IGNORE, PRIORITY] as Set<String>
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

        for (Column column : decisionTable.getAxis(fieldAxisName).columnsWithoutDefault)
        {
            Map colMetaProps = column.metaProperties
            if (colMetaProps.containsKey(INPUT_VALUE))
            {
                inputColumns.add((String) column.value)
            }

            if (colMetaProps.containsKey(INPUT_LOW))
            {
                if (!colMetaProps.containsKey(DATA_TYPE))
                {
                    throw new IllegalStateException("Range columns must have 'data_type' meta-property set, ncube: ${decisionTable.name}, column: ${column.value}. Valid values are DATE, LONG, DOUBLE, BIG_DECIMAL.")
                }
                inputColumns.add((String) column.value)
                rangeColumns.add((String) column.value)
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

    private NCube createValidationNCube(List<Column> rows)
    {
        NCube blowout = new NCube('validation')
        Map<String, Comparable> coord = [:]

        for (String colValue : inputColumns)
        {
            if (rangeColumns.contains(colValue))
            {
                continue
            }

            Column field = decisionTable.getAxis(fieldAxisName).findColumn(colValue)
            String fieldValue = field.value

            Axis axis = new Axis(fieldValue, AxisType.DISCRETE, AxisValueType.CISTRING, false)
            blowout.addAxis(axis)

            for (Column row : rows)
            {
                String rowValue = row.value
                coord.put(fieldAxisName, fieldValue)
                coord.put(rowAxisName, rowValue)
                String cellValue = convertToString(decisionTable.getCellNoExecute(coord))
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

    private Set<Comparable> validateDecisionTableRow(NCube blowout, List<Column> rows)
    {
        Axis fieldAxis = decisionTable.getAxis(fieldAxisName)
        Map<String, Comparable> coord = [:]
        Set<Comparable> badRows = []
        boolean hasIgnoreColumn = fieldAxis.findColumns([(IGNORE): true] as Map).size() > 0

        for (Column row : rows)
        {
            Comparable rowValue = row.value
            coord.put(rowAxisName, rowValue)

            if (hasIgnoreColumn)
            {
                coord.put(fieldAxisName, IGNORE)
                if (convertToBoolean(decisionTable.getCell(coord)))
                {
                    continue
                }
            }

            Map<String, Integer> counters = [:]
            Map<String, Set<String>> bindings = [:]

            for (String colValue : inputColumns)
            {
                if (rangeColumns.contains(colValue))
                {
                    continue
                }

                coord.put(fieldAxisName, colValue)

                counters[colValue] = 1
                bindings[colValue] = new HashSet()

                String cellValue = convertToString(decisionTable.getCellNoExecute(coord))
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

            Range range = buildRange(rowValue)
            String[] axisNames = bindings.keySet() as String[]

            populateCachedNCube(badRows, blowout, counters, bindings, axisNames, range, rowValue) // call this method once before the counter gets turned over
            while (incrementVariableRadixCount(counters, bindings, axisNames))
            {
                populateCachedNCube(badRows, blowout, counters, bindings, axisNames, range, rowValue)
            }
        }
        return badRows
    }

    private Range buildRange(Comparable rowValue)
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

            if (colMetaProps.containsKey(INPUT_LOW))
            {
                coord.put(fieldAxisName, colValue)
                range.low = getRangeValue(dataType, coord)
            }
            else if (colMetaProps.containsKey(INPUT_HIGH))
            {
                coord.put(fieldAxisName, colValue)
                range.high = getRangeValue(dataType, coord)
            }
        }

        List<Column> priorityColumns = fieldAxis.findColumns([(PRIORITY): true] as Map)
        if (priorityColumns)
        {
            Column priority = priorityColumns.first()
            coord.put(fieldAxisName, priority.value)
            Object cellValue = decisionTable.getCell(coord)
            range.priority = convertToInteger(cellValue)
        }

        return range
    }

    private Comparable getRangeValue(String dataType, Map coord)
    {
        Object cellValue = decisionTable.getCell(coord)
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

    private static void populateCachedNCube(Set<Comparable> badRows, NCube blowout, Map<String, Integer> counters, Map<String, Set<String>> bindings, String[] axisNames, Range candidate, Comparable rowId)
    {
        Map<String, Object> coordinate = [:]
        for (String key : axisNames)
        {
            int radix = counters[key]
            String value = bindings.get(key)[radix - 1]
            coordinate.put(key, value)
        }

        boolean goodCoordinate = true
        String existingValue = blowout.getCell(coordinate)
        if (existingValue != null)
        {
            Iterable<String> entryStrings = BAR_SPLITTER.split(existingValue)
            for (String entryString : entryStrings)
            {
                Range existingEntry = new Range(entryString)
                if (candidate.out() == existingEntry.out())
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
            existingValue = ""
        }

        if (goodCoordinate)
        {
            String newValue = "${candidate.out()}|${existingValue}"
            blowout.setCell(newValue, coordinate)
        }
    }

    /**
     * Increment the variable radix number passed in.  The number is represented by a Map, where the keys are the
     * digit names (axis names), and the values are the associated values for the number.
     * @return false if more incrementing can be done, otherwise true.
     */
    private static boolean incrementVariableRadixCount(final Map<String, Integer> counters,
                                                       final Map<String, Set<String>> bindings,
                                                       final String[] axisNames)
    {
        int digit = axisNames.length - 1

        while (true)
        {
            final String axisName = axisNames[digit]
            final int count = counters[axisName]
            final Set<String> cols = bindings[axisName]

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
}
