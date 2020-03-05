package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Ignore
import org.junit.Test

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

        for (int i = 0; i < 1; i ++)
        {
            long start = System.nanoTime()
            Set<Comparable> badRows = dt.validateDecisionTable()
            long end = System.nanoTime()
            println "took ${(end - start) / 1000000} ms"
        }

//        assert badRows.empty
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
        assert dt.requiredKeys.size() == 0
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
        assert out.size() == 1
        Map row = out[1L] as Map
        assert row.output == '15'

        out = dt.getDecision([state:'OH'])
        assert out.size() == 2
        row = out[1L] as Map
        assert row.output == '15'
        row = out[2L] as Map
        assert row.output == '20'
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

        println out
        // TODO: Bug - validateDecisionTable() should throw exception as there is more than one output
        dt.validateDecisionTable()
    }

    // TODO: write test that finds a cube with a null range value (in the table definition)
    // TODO: write test that has ranges of DOUBLE and BIG_DECIMAL
    // TODO: write test that doesn't provide required input on call to getDecision()
    // TODO: write test to verify that output values are converted to the meta-property data_type
    // TODO: Cut a bunch of rows off of commission.json and add that to the tests.

    private static DecisionTable getDecisionTableFromJson(String file)
    {
        String json = NCubeRuntime.getResourceAsString(file)
        return new DecisionTable(NCube.fromSimpleJson(json))
    }
}
