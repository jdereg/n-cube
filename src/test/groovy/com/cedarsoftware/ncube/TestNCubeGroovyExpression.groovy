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
    void testDt_NoNCube()
    {
        NCube caller = new NCube('test')
        caller.applicationID = testAppId
        String cmd = "dt('2dv').val('output', [state: 'OH', pet: 'dog'])"
        caller.defaultCellValue = new GroovyExpression(cmd)

        try
        {
            caller.getCell([:])
        }
        catch (CommandCellException e)
        {
            Throwable t = getDeepestException(e)
            assertContainsIgnoreCase(t.message, 'dt()', 'n-cube', 'not found')
        }
    }
}
