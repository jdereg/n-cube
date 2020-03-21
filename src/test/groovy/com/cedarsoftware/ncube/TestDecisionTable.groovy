package com.cedarsoftware.ncube


import groovy.transform.CompileStatic
import org.junit.Ignore
import org.junit.Test

import java.security.SecureRandom

import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.fail

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
class TestDecisionTable extends NCubeBaseTest
{
    @Ignore
    @Test
    void testGetDecision()
    {
        Map input = [profitCenter: '2967',
                     producerCode: '50',
                     date: new Date(),
                     symbol: 'CAP']
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'decision-tables/commission.json')
        DecisionTable decisionTable = new DecisionTable(ncube)

        for (int i=0; i < 10; i++)
        {
            long start = System.nanoTime()
            println decisionTable.getDecision(input)
            long end = System.nanoTime()
            println "took ${(end - start) / 1000000} ms"
        }
    }

    @Ignore
    @Test
    void testStuff()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/commission.json')

        for (int i = 0; i < 10; i ++)
        {
            long start = System.nanoTime()
            Set<Comparable> badRows = dt.validateDecisionTable()
            long end = System.nanoTime()
            println "took ${(end - start) / 1000000} ms"
        }
    }

    @Test
    void testGetDefinedValues()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv.json')
        dt.validateDecisionTable()
        Map<String, Set<String>> definedValues = dt.definedValues
        assert 2 == definedValues.size()
        Set<String> states = definedValues['state']
        assert 5 == states.size()
        assert states.contains('OH')
        assert states.contains('IN')
        assert states.contains('KY')
        assert states.contains('MI')
        assert states.contains('CA')
        assert states.contains('CA')
        Set<String> pets = definedValues['pet']
        assert 4 == pets.size()
        assert pets.contains('dog')
        assert pets.contains('cat')
        assert pets.contains('horse')
        assert pets.contains('lizard')
    }

    @Test
    void testOneAxis()
    {
        try
        {
            getDecisionTableFromJson('testCube1.json')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'table', '2 axes')
        }
    }

    @Test
    void test_1dv_pos_1row()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_pos.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
        assert dt.requiredKeys.size() == 1
        Map result = dt.getDecision([state:'OH'])
        Map row = result[1L] as Map
        assert row.output == '15'
        result = dt.getDecision([state:'KY'])
        assert result.isEmpty()
    }

    @Test
    void test_1dv_pos_star()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_pos_star.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
        Map out = dt.getDecision([state:'KY'])
        assert out.size() == 0
        out = dt.getDecision([state:'KY', sku: 1])
        Map row = out[1L] as Map
        assert row.output == '15'

        out = dt.getDecision([state:'OH', sku: 2])
        assert out.size() == 1
        row = out[2L] as Map
        assert row.output == '20'

        out = dt.getDecision([:])
        assert out.size() == 0
    }

    @Test
    void test_1dv_pos_star_noDiscrete()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/1dv_pos_star_noDiscrete.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'field', 'axis', 'must', 'DISCRETE')
        }
    }

    @Test
    void test_1dv_pos_star_noCIString()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/1dv_pos_star_noCISTRING.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'field', 'axis', 'must', 'CISTRING')
        }
    }

    @Test
    void test_1dv_neg_1row()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_neg.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
        assert dt.getDecision([state:'OH']).isEmpty()
        Map<?, Map<String, ?>> result = dt.getDecision([state:'TX']) as Map
        assert result.size() == 1
        Map<String, ?> out = result[1L]
        assert out.output == '15'
    }

    @Test
    void test_1dv_both()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_both.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
    }

    @Test
    void test_1dv_bad()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_bad.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert 1 == badRows.size()
        Comparable badRow = badRows.first()
        assert 2L == badRow
    }

    @Test
    void test_2dv_priority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_priority.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty

        Map<String, Set<String>> map = dt.definedValues
        assert map.size() == 2
        Set pets = map['pet']
        assert pets.size() == 1
        Set states = map['state']
        assert states.size() == 1
    }

    @Test
    void test_2dv_priority_bad()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_priority_bad.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert 1 == badRows.size()
        Comparable badRow = badRows.first()
        assert 2L == badRow
    }

    @Test
    void test_2dv_range()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty

        Map<String, Set<String>> map = dt.definedValues
        assert map.size() == 2
        Set pets = map['pet']
        Set number = map['number']
        assert pets.empty
        assert number.empty
    }

    @Test
    void test_2dv_range_bad()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_bad.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert 1 == badRows.size()
        Comparable badRow = badRows.first()
        assert 2L == badRow
    }

    @Test
    void test_2dv_range_priority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_priority.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
        
        Map<String, Set<String>> map = dt.definedValues
        assert map.size() == 2
        Set pets = map['pet']
        Set number = map['number']
        assert pets.size() == 1
        assert pets.contains('cat')
        assert number.empty
    }

    @Test
    void test_2dv_range_priority_bad()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_priority_bad.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert 1 == badRows.size()
        Comparable badRow = badRows.first()
        assert 3L == badRow
    }

    @Test
    void testRangeDeclaredHighThenLowWithNoIgnorePriorityAndOutput()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/high-low.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert 0 == badRows.size()
        Map map10 = dt.getDecision([age:10])
        assert map10.size() == 1
        assert map10[1L]['upper'] == 16
        assert map10[1L]['lower'] == 0
        Map map20 = dt.getDecision([AGE:20])
        assert map20.size() == 1
        assert map20[2L]['UPPER'] == 22
        assert map20[2L]['LOWER'] == 16
        assert map10 != map20

        assert dt.inputKeys.contains('age')
        assert dt.inputKeys.size() == 1
        assert dt.requiredKeys.contains('age')
        assert dt.requiredKeys.size() == 1
    }

    @Test
    void testRangeDeclaredHighThenLowWithNoIgnorePriority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/high-low-output.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert 0 == badRows.size()
        Map map10 = dt.getDecision([age:10])
        assert map10.size() == 1
        assert map10[1L]['rate'] == 0.0
        Map map20 = dt.getDecision([AGE:20])
        assert map20.size() == 1
        assert map20[2L]['RaTe'] == 2.0
    }

    @Test
    void testGetDecision_Simple()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv.json')
        dt.validateDecisionTable()
        Map input = [state: 'OH', pet: 'dog']
        Map decision = dt.getDecision(input)
        assert 1 == decision.size()
        Map output = (Map) decision.values().first()
        assert '15' == output['output']
    }

    @Test
    void testGetDecision_Ignore()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_ignore.json')
        dt.validateDecisionTable()
        Map input = [state: 'OH', pet: 'dog']
        Map decision = dt.getDecision(input)
        assert 0 == decision.size()     // no matches, this row is ignored
        input = [state: 'KY', pet: 'dog']
        decision = dt.getDecision(input)
        assert 0 == decision.size()     // no matches, this row is ignored
    }

    @Test
    void testGetDecision_ExtraInput()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv.json')
        dt.validateDecisionTable()
        Map input = [state: 'OH', pet: 'dog', foo: 'bar']
        Map decision = dt.getDecision(input)
        assert 1 == decision.size()
        Map output = (Map) decision.values().first()
        assert '15' == output['output']
    }

    @Test
    void testGetDecision_EmptyColumn()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv.json')
        dt.validateDecisionTable()
        Map input = [state: 'VA', pet: 'horse']
        Map decision = dt.getDecision(input)
        assert 1 == decision.size()
        Map output = (Map) decision.values().first()
        assert '50' == output['output']

        assert dt.inputKeys.containsAll(['state', 'pet'])
        assert dt.requiredKeys.size() == 1
    }
    
    @Test
    void testGetDeterminedAxisNames()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/high-low-output.json')
        dt.validateDecisionTable()
        assert dt.decisionAxisName == 'Column'
        assert dt.decisionRowName == 'Row'

        assert dt.inputKeys.contains('age')
        assert dt.inputKeys.size() == 1
        assert dt.requiredKeys.contains('age')
        assert dt.requiredKeys.size() == 1
    }

    @Test
    void testRangeBelowAboveAndNull()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
        assert dt.getDecision([number:-1d]).isEmpty()
        assert dt.getDecision([number:101d]).isEmpty()
        assert dt.getDecision([number:null]).isEmpty()
        assert dt.requiredKeys.size() == 1
    }

    @Test
    void test2d_notDecTable()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2d_notDecTable.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'must have', 'one axis', 'meta', 'property')
        }
    }

    @Test
    void testRangeWithBadDataType()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2dv_range_bad_datatype.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'range columns', 'must', 'type', 'meta', 'prop')
        }
    }

    @Test
    void testRangeWithBadMetaPropType()
    {
        String json = NCubeRuntime.getResourceAsString('decision-tables/2dv_range_bad_meta_prop_type.json')
        NCube ncube = NCube.fromSimpleJson(json)

        try
        {
            new DecisionTable(ncube)
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'input_low', 'meta', 'prop', 'type', 'string')
        }

        Axis field = ncube.getAxis('field')
        Column col = field.findColumn('low')
        col.setMetaProperty('input_low', 'number')
        new DecisionTable(ncube)

        col = field.findColumn('high')
        col.setMetaProperty('input_high', 10i)

        try
        {
            new DecisionTable(ncube)
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'input_high', 'meta', 'prop', 'type', 'string')
        }
    }
    
    @Test
    void testRangeWithTwoSameLowLimits()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2dv_bad_low_low.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'more than', 'low', 'same input', 'name')
        }
    }

    @Test
    void testRangeWithTwoSameHighLimits()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2dv_bad_high_high.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'more than', 'high', 'same input', 'name')
        }
    }

    @Test
    void testWithRequiredOnNonInputField()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/1dv_bad_required.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'required','meta','property','found','not','input')
        }
    }

    @Test
    void testWithLowRangeOnly()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2dv_range_bad_low_only.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'lower', 'range', 'defined', 'without', 'upper','range')
        }
    }

    @Test
    void testWithHighRangeOnly()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2dv_range_bad_high_only.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'upper', 'range', 'defined', 'without', 'lower','range')
        }
    }

    @Test
    void testWithNullPriority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_null_priority.json')
        NCube ncube  = dt.underlyingNCube
        def x = ncube.getCell([field:'priority', row:1L])
        assert Integer.MAX_VALUE == x
    }

    @Test
    void testWithMultiPriority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_multi_priority.json')
        Map<Long, Map<String, ?>> out = dt.getDecision([state:'OH']) as Map
        Map<String, ?> output = out[3L]
        assert output['output'] == '25'
    }

    @Test
    void testWithMultiSamePriority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_multi_same_priority.json')
        Map<Long, Map<String, ?>> out = dt.getDecision([state:'OH']) as Map
        assert out.size() == 3
        assert out[1L]['output'] == '15'
        assert out[2L]['output'] == '20'
        assert out[3L]['output'] == '25'

        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.size() == 2
    }

    @Test
    void testRangeWithNullInLimitColumm()
    {
        try
        {
            getDecisionTableFromJson('decision-tables/2dv_range_null_in_col.json')
            fail()
        }
        catch(IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'values', 'range', 'must', 'instanceof', 'comparable')
        }
    }

    @Test
    void testRangeWithDoubleDataTypeAndOutputDataType()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_double.json')
        Map<Long, Map<String, ?>> out = dt.getDecision([number: 0.0d]) as Map
        assert out[1L]['output'] == 15.0d
        out = dt.getDecision([number: 6.0d]) as Map
        assert out[2L]['output'] instanceof Double
        assert out[2L]['output'] == 20.0d
        out = dt.getDecision([number: -1.0d]) as Map
        assert out.isEmpty()
        out = dt.getDecision([number: 100.0d]) as Map
        assert out.isEmpty()

        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.size() == 0
    }

    @Test
    void testRangeWithBigDecimalDataTypeAndOutputDataType()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_big_dec.json')
        Map<Long, Map<String, ?>> out = dt.getDecision([number: 0.0d]) as Map
        assert out[1L]['output'] == 15.0
        out = dt.getDecision([number: 6.0]) as Map
        assert out[2L]['output'] instanceof BigDecimal
        assert out[2L]['output'] == 20.0
        out = dt.getDecision([number: -1.0]) as Map
        assert out.isEmpty()
        out = dt.getDecision([number: 100.0]) as Map
        assert out.isEmpty()

        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.size() == 0
    }

    @Test
    void testRangeWithDateDataTypeAndOutputDataType()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_date.json')
        Map<Long, Map<String, ?>> out = dt.getDecision([date: convertToDate('2020/01/01')]) as Map
        assert out[1L]['output'] == convertToDate('2021/01/15')
        out = dt.getDecision([date: convertToDate('2020/07/01')]) as Map
        assert out[2L]['output'] instanceof Date
        assert out[2L]['output'] == convertToDate('2021/01/20')
        out = dt.getDecision([date: convertToDate('1900/01/01')]) as Map
        assert out.isEmpty()
        out = dt.getDecision([date: convertToDate('2100/01/01')]) as Map
        assert out.isEmpty()

        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.size() == 0
    }

    @Test
    void testRequiredInputNotSuppliedForRange()
    {
        try
        {
            DecisionTable dt = getDecisionTableFromJson('decision-tables/high-low.json')
            dt.getDecision([foo: 'bar']) as Map
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'required', 'input', '[age]', 'not', 'found')
        }
    }

    @Test
    void testRequiredInputNotSuppliedForInput()
    {
        try
        {
            DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_pos.json')
            dt.getDecision([foo: 'bar']) as Map
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'required', 'input', '[state]', 'not', 'found')
        }
    }

    @Test
    void testOutputDatatypeSupport()
    {
        try
        {
            DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_pos_data_type_bad.json')
            dt.getDecision([state: 'OH']) as Map
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'data', 'type', 'must', 'be')
        }
    }

    @Test
    void testRangeOnly()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_only.json')
        Map<Long, ?> map = dt.getDecision([number: 10L]) as Map
        Map<String, ?> row = map[2L] as Map
        assert row.output == '20'

        Set set = dt.validateDecisionTable()
        assert set.empty
    }

    @Test
    void test1RangeGood()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/2dv_range_only.json')
        Map<Long, ?> map = dt.getDecision([number: 10L]) as Map
        Map<String, ?> row = map[2L] as Map
        assert row.output == '20'

        Set set = dt.validateDecisionTable()
        assert set.empty
    }

    @Test
    void test2RangesGood()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/multi_range_good.json')
        Set set = dt.validateDecisionTable()
        assert set.empty
        Map<Long, ?> map = dt.getDecision([age:70,salary:30000]) as Map
        assertContainsIgnoreCase(map.toString(), '5','factor', '5.0')
    }

    @Test
    void test2RangesBad()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/multi_range_bad.json')
        Set set = dt.validateDecisionTable()
        assert set.size() == 1
        assert set.contains(10L)
    }

    @Test
    void test2RangesGoodPriority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/multi_range_good_priority.json')
        Set set = dt.validateDecisionTable()
        assert set.empty
        Map<Long, ?> map = dt.getDecision([age:70,salary:30000]) as Map
        assertContainsIgnoreCase(map.toString(), '10','factor', '10.0')
    }

    @Test
    void test2RangesGoodIgnore()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/multi_range_good_ignore.json')
        Set set = dt.validateDecisionTable()
        assert set.empty
        Map<Long, ?> map = dt.getDecision([age:70,salary:30000]) as Map
        assertContainsIgnoreCase(map.toString(), '5','factor', '5.0')
    }

    @Test
    void test2Ranges2DiscretesGood()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/multi_range_multi_discrete_good.json')
        Set set = dt.validateDecisionTable()
        assert set.empty

        Map map = dt.getDecision(['PROFIT center':2700,symbol:'c', age:40, salary: 25000])
        assertContainsIgnoreCase(map.toString(), '4','factor', '4.0')

        map = dt.getDecision(['PROFIT center':2800,symbol:'z', age:54, salary: 39000])
        assertContainsIgnoreCase(map.toString(), '9','factor', '9.0')
    }

    @Test
    void testOptionalInputNotSupplied()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/optional_input.json')
        Set set = dt.validateDecisionTable()
        assert set.empty

        Map decision = dt.getDecision([profitCenter: 23, date: new Date()])
        assert decision.size() == 1
        assert decision.containsKey('1581788642877000415')
    }

    @Test
    void testDiscreteRangeRepeatedPriority()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/discrete_range_repeated_priority.json')
        Set set = dt.validateDecisionTable()
        assert set.empty

        Map decision = dt.getDecision([profit: 1, loc: 'a', date: 2000])
        assert decision.size() == 1
        assert decision.containsKey(3L)

        decision = dt.getDecision([profit: 1, loc: 'a', date: 900])
        assert decision.size() == 1
        assert decision.containsKey(7L)
    }

    @Test
    void testBigDecisionTablePerformance()
    {
        int numInputs = 10
        int numRows = 1000
        int span = 1000

        NCube ncube = new NCube('BigDecisionTable')
        Axis fields = new Axis('field', AxisType.DISCRETE, AxisValueType.CISTRING, false, Axis.DISPLAY)
        for (int i = 0; i < numInputs; i++)
        {
            Column column = fields.addColumn("i${i}".toString())
            column.setMetaProperty(INPUT_VALUE, Boolean.TRUE)
        }
        Column price = fields.addColumn('price')
        price.setMetaProperty(OUTPUT_VALUE, Boolean.TRUE)

        Axis rows = new Axis('row', AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY)
        for (int i = 0; i < numRows; i++)
        {
            rows.addColumn(i)
        }
        ncube.addAxis(fields)
        ncube.addAxis(rows)

        Random random = new SecureRandom()
        Map<String, ?> coord = new HashMap<>()

        for (int i = 0; i < numInputs; i++)
        {
            coord.put('field', "i${i}".toString())
            for (int j = 0; j < numRows; j++)
            {
                coord.put('row', j)
                ncube.setCell(random.nextInt(span), coord)
            }
        }

        coord.put('field', 'price')
        for (int j = 0; j < numRows; j++)
        {
            coord.put('row', j)
            ncube.setCell(j, coord)
        }

        DecisionTable dt = new DecisionTable(ncube)
        long start = System.nanoTime()
        println dt.validateDecisionTable()
        long stop = System.nanoTime()

        println "${numInputs} decision variables by ${numRows} rules validation time: ${(stop - start) / 1000000}"
    }

    private static DecisionTable getDecisionTableFromJson(String file)
    {
        String json = NCubeRuntime.getResourceAsString(file)
        return new DecisionTable(NCube.fromSimpleJson(json))
    }
}