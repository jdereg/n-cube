package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CommandCellException
import groovy.transform.CompileStatic
import org.junit.Test

import static com.cedarsoftware.ncube.ApplicationID.testAppId
import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.util.ExceptionUtilities.getDeepestException
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase

@CompileStatic
class TestNCubeGroovyExpression extends NCubeBaseTest
{
    @Test
    void testGetDecisionValue()
    {
        String json = NCubeRuntime.getResourceAsString('decision-tables/2dv.json')
        NCube dtCube = NCube.fromSimpleJson(json)
        dtCube.applicationID = testAppId
        ncubeRuntime.addCube(dtCube)

        NCube caller = new NCube('test')
        caller.applicationID = testAppId
        String cmd = "decisionValue('output', [state: 'OH', pet: 'dog'], '2dv')"
        caller.defaultCellValue = new GroovyExpression(cmd)

        assert '15' == caller.getCell([:])
    }

    @Test
    void testGetDecisionValue_NoResult()
    {
        String json = NCubeRuntime.getResourceAsString('decision-tables/2dv.json')
        NCube dtCube = NCube.fromSimpleJson(json)
        dtCube.applicationID = testAppId
        ncubeRuntime.addCube(dtCube)

        NCube caller = new NCube('test')
        caller.applicationID = testAppId
        String cmd = "decisionValue('output', [state: 'AZ', pet: 'dog'], '2dv')"
        caller.defaultCellValue = new GroovyExpression(cmd)

        assert null == caller.getCell([:])
    }

    @Test
    void testGetDecisionValue_NoNCube()
    {
        NCube caller = new NCube('test')
        caller.applicationID = testAppId
        String cmd = "decisionValue('output', [state: 'OH', pet: 'dog'], '2dv')"
        caller.defaultCellValue = new GroovyExpression(cmd)

        try
        {
            caller.getCell([:])
        }
        catch (CommandCellException e)
        {
            Throwable t = getDeepestException(e)
            assertContainsIgnoreCase(t.message, 'decisionvalue', 'n-cube', 'not found')
        }
    }

    @Test
    void testGetDecisionValue_MultipleResults()
    {
        String json = NCubeRuntime.getResourceAsString('decision-tables/2dv.json')
        NCube dtCube = NCube.fromSimpleJson(json)
        dtCube.applicationID = testAppId
        ncubeRuntime.addCube(dtCube)

        NCube caller = new NCube('test')
        caller.applicationID = testAppId
        String cmd = "decisionValue('output', [state: 'OH'], '2dv')"
        caller.defaultCellValue = new GroovyExpression(cmd)

        try
        {
            caller.getCell([:])
        }
        catch (CommandCellException e)
        {
            Throwable t = getDeepestException(e)
            assertContainsIgnoreCase(t.message, 'decisionvalue', 'more than 1')
        }
    }

    @Test
    void testGetDecisionValue_InvalidColumnName()
    {
        String json = NCubeRuntime.getResourceAsString('decision-tables/2dv.json')
        NCube dtCube = NCube.fromSimpleJson(json)
        dtCube.applicationID = testAppId
        ncubeRuntime.addCube(dtCube)

        NCube caller = new NCube('test')
        caller.applicationID = testAppId
        String cmd = "decisionValue('outputx', [state: 'OH', pet: 'dog'], '2dv')"
        caller.defaultCellValue = new GroovyExpression(cmd)

        try
        {
            caller.getCell([:])
        }
        catch (CommandCellException e)
        {
            Throwable t = getDeepestException(e)
            assertContainsIgnoreCase(t.message, 'decisionvalue', 'not', 'output')
        }
    }
}
