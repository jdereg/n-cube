package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CommandCellException
import groovy.transform.CompileStatic
import org.junit.Test

import static com.cedarsoftware.ncube.ApplicationID.testAppId
import static com.cedarsoftware.util.ExceptionUtilities.getDeepestException
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase

@CompileStatic
class TestNCubeGroovyExpression extends NCubeBaseTest
{
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
}
