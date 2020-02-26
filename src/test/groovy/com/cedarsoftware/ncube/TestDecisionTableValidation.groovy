package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.decision.DecisionTable
import groovy.transform.CompileStatic
import org.junit.Test

import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.fail

/**
 * @author Josh Snyder
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
class TestDecisionTableValidation extends NCubeBaseTest
{
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
        
//        assert badRows.empty
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

    private static DecisionTable getDecisionTableFromJson(String file)
    {
        String json = NCubeRuntime.getResourceAsString(file)
        return new DecisionTable(NCube.fromSimpleJson(json))
    }
}
