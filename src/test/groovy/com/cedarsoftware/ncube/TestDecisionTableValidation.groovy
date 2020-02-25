package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.decision.DecisionTable
import groovy.transform.CompileStatic
import org.junit.Test

import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.fail

@CompileStatic
class TestDecisionTableValidation
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
