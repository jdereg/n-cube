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

        for (int i = 0; i < 3; i ++)
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
        Map<String, Set<String>> definedValues = dt.definedValues
        assert 2 == definedValues.size()
        Set<String> states = definedValues['state']
        assert 5 == states.size()
        assert states.contains('OH')
        assert states.contains('IN')
        assert states.contains('KY')
        assert states.contains('MI')
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
    }

    @Test
    void test_1dv_neg_1row()
    {
        DecisionTable dt = getDecisionTableFromJson('decision-tables/1dv_neg.json')
        Set<Comparable> badRows = dt.validateDecisionTable()
        assert badRows.empty
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

    private static DecisionTable getDecisionTableFromJson(String file)
    {
        String json = NCubeRuntime.getResourceAsString(file)
        return new DecisionTable(NCube.fromSimpleJson(json))
    }
}
