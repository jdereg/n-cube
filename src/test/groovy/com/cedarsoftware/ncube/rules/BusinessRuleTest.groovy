package com.cedarsoftware.ncube.rules

import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeBaseTest
import groovy.transform.CompileStatic
import org.junit.Test
import org.slf4j.Logger

import static com.cedarsoftware.ncube.ApplicationID.testAppId
import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.fail

@CompileStatic
class BusinessRuleTest extends NCubeBaseTest
{
    @Test
    void testConstructor_Map()
    {
        Map map = [foo: 'bar']
        BusinessRule br = new BusinessRule(map)
        assert map.equals(br.root)
    }

    @Test
    void testConstructor_Null()
    {
        BusinessRule br = new BusinessRule(null)
        assert null == br.root
    }

    @Test
    void testLogger()
    {
        BusinessRule br = new BusinessRule(null)
        assert br.log != null
        assert br.getLog() != null
        assert br.getLog() instanceof Logger
    }

    @Test
    void testGetMapOrList_Map()
    {
        Map object = [field: [:]]
        List<Map> list = new BusinessRule(null).getMapOrList(object, 'field')
        assert 1 == list.size()
    }

    @Test
    void testGetMapOrList_List()
    {
        Map object = [field: [[:]]]
        List<Map> list = new BusinessRule(null).getMapOrList(object, 'field')
        assert 1 == list.size()
    }

    @Test
    void testGetMapOrList_Other()
    {
        BusinessRule rule = new BusinessRule(null)
        Map object = [field: 'foo']
        rule.getMapOrList(object, 'field')
        assert 1 == rule.errors.size()
        RulesError exception = rule.errors[0]
        assertContainsIgnoreCase((String)exception.message, 'field', 'not', 'list', 'map', 'map')
    }

    @Test
    void testGetMapOrList_MissingField()
    {
        BusinessRule rule = new BusinessRule(null)
        Map object = [:]
        rule.getMapOrList(object, 'field')
        assert 1 == rule.errors.size()
        RulesError exception = rule.errors[0]
        assertContainsIgnoreCase((String)exception.message, 'field', 'not exist', 'map')
    }

    @Test
    void testGetMapOrList_Null()
    {
        BusinessRule rule = new BusinessRule(null)
        Map object = [field: null]
        rule.getMapOrList(object, 'field')
        assert 1 == rule.errors.size()
        RulesError exception = rule.errors[0]
        assertContainsIgnoreCase((String)exception.message, 'field', 'not', 'list', 'map', 'map')
    }

    @Test
    void testAddError()
    {
        BusinessRule rule = new BusinessRule(null)
        rule.addError('foo', 'bar', 'baz')
        List<RulesError> errors = rule.errors
        assert 1 == errors.size()
        RulesError error = errors[0]
        assert 'foo' == error.category
        assert 'bar' == error.code
        assert 'baz' == error.message
    }

    @Test
    void testSafeParseDate()
    {
        BusinessRule rule = new BusinessRule(null)
        Date date = rule.safeParseDate('2019-10-14', 'foo', 'bar')
        assert rule.errors.empty
        Calendar calendar = Calendar.instance
        calendar.time = date
        assert 2019 == calendar.get(Calendar.YEAR)
        assert 9 == calendar.get(Calendar.MONTH)
        assert 14 == calendar.get(Calendar.DAY_OF_MONTH)

    }

    @Test
    void testSafeParseDate_InvalidDate()
    {
        BusinessRule rule = new BusinessRule(null)
        rule.safeParseDate('baz', 'foo', 'bar')
        assert 1 == rule.errors.size()
        RulesError error = rule.errors[0]
        assert 'Invalid Request' == error.category
        assert 'foo' == error.code
        assertContainsIgnoreCase(error.message, 'bar')

    }

    @Test
    void testGetConstant()
    {
        NCube mockConstants = new NCube('lookup.constants')
        mockConstants.applicationID = testAppId
        mockConstants.defaultCellValue = 'foo'
        ncubeRuntime.addCube(mockConstants)

        BusinessRule rule = new BusinessRule(null)
        rule.appId = testAppId
        assert 'foo' == rule.getConstant('FOO')

        ncubeRuntime.clearCache(testAppId)
    }

    @Test
    void testGetConstant_NoAppId()
    {
        BusinessRule rule = new BusinessRule(null)
        try
        {
            rule.getConstant('FOO')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'applicationid', 'null')
        }

    }

    @Test
    void testGetConstant_NoNCube()
    {
        BusinessRule rule = new BusinessRule(null)
        rule.appId = testAppId
        try
        {
            rule.getConstant('FOO')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'ncube', 'lookup.constants', 'not', 'appid')
        }

    }

    @Test
    void testTraverse_NoSource()
    {
        BusinessRule rule = new BusinessRule(null)
        try
        {
            rule.runTargetRules('foo', 'field1', 'bar', 'rule.baz')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'class', 'does not', 'property')
        }
    }

    @Test
    void testTraverse_NoField()
    {
        BusinessRule rule = new BusinessRule([foo: []])
        rule.runTargetRules('root', 'field1', 'bar', 'rule.baz')
        assert 1 == rule.errors.size()
        RulesError exception = rule.errors[0]
        assertContainsIgnoreCase((String)exception.message, 'field', 'not exist', 'map')
    }

    @Test
    void testTraverse_NoTarget()
    {
        BusinessRule rule = new BusinessRule([field1: [:]])
        try
        {
            rule.runTargetRules('root', 'field1', 'bar', 'rule.baz')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'field name', 'does not', 'exist')
        }
    }

    @Test
    void testTraverse_NoNCube()
    {
        BusinessRule rule = new TestRule([field1: [:]])
        rule.appId = testAppId
        rule.input = [:]
        rule.output = [:]
        try
        {
            rule.runTargetRules('root', 'field1', 'foo', 'rule.baz')
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'ncube', 'does not', 'exist', 'appId')
        }
    }

    @Test
    void testTraverse()
    {
        NCube mockConstants = new NCube('rule.baz')
        mockConstants.applicationID = testAppId
        ncubeRuntime.addCube(mockConstants)

        BusinessRule rule = new TestRule([field1: [:]])
        rule.appId = testAppId
        rule.input = [:]
        rule.output = [:]
        rule.runTargetRules('root', 'field1', 'foo', 'rule.baz')
        assert rule.errors.empty

        ncubeRuntime.clearCache(testAppId)
    }
}
