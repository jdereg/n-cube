package com.cedarsoftware.ncube.decision

import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.Range
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.StringUtilities
import groovy.transform.CompileStatic

import static com.cedarsoftware.ncube.NCubeConstants.*
import static com.cedarsoftware.util.Converter.*

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
    Map<Comparable, ?> getDecision(Map<String, ?> input)
    {
        Map<String, ?> ranges = new HashMap<>()
        Map<String, ?> copyInput = new CaseInsensitiveMap<>(input)
        copyInput.put('ignore', null)
        Axis fieldAxis = decisionTable.getAxis(decisionAxisName)

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

        Map<Comparable, ?> result = decisionTable.mapReduce(decisionAxisName, decisionTableClosure, options)
        // TODO: Ensure determinePriority() allows for multiple rows to be returned.
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

                if ('ignore' == decVarName)
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

                    if (dataType.equals('DATE'))
                    {
                        low = convertToDate(low)
                        high = convertToDate(high)
                        value = convertToDate(value)
                    }
                    else if (dataType.equals('LONG'))
                    {
                        low = convertToLong(low)
                        high = convertToLong(high)
                        value = convertToLong(value)
                    }
                    else if (dataType.equals('DOUBLE'))
                    {
                        low = convertToDouble(low)
                        high = convertToDouble(high)
                        value = convertToDouble(value)
                    }
                    else if (dataType.equals('BIG_DECIMAL'))
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
        if (StringUtilities.hasContent(fieldAxisName) && StringUtilities.hasContent(rowAxisName))
        {
            return
        }

        if (decisionTable.numDimensions != 2)
        {
            return
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
            return
        }

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
    
    // TODO: Review.  Need to allow for multiple rows to return
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
}
