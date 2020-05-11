package com.cedarsoftware.ncube.rules

import com.cedarsoftware.ncube.NCubeBaseTest
import com.cedarsoftware.ncube.NCubeRuntime
import com.cedarsoftware.util.DeepEquals
import com.cedarsoftware.util.io.JsonReader
import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Test

import static TestApplication.createAndCacheNCube
import static com.cedarsoftware.ncube.ApplicationID.testAppId
import static com.cedarsoftware.ncube.AxisType.RULE
import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.ncube.rules.RulesEngine.AXIS_ATTRIBUTE
import static com.cedarsoftware.ncube.rules.RulesEngine.AXIS_RULE
import static com.cedarsoftware.ncube.rules.RulesEngine.AXIS_RULE_GROUP
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.fail

@CompileStatic
class RulesEngineTest extends NCubeBaseTest
{
    private static final String NCUBE_RULES = 'app.rules'
    private static final String NCUBE_CATEGORIES = 'app.rules.categories'

    RulesEngine rulesEngine = new RulesEngine('testEngine', testAppId, NCUBE_RULES, NCUBE_CATEGORIES)

    @Before
    void setupNCubes()
    {
        createAndCacheNCube('rules/ncubes/lookup.something.json')
        createAndCacheNCube('rules/ncubes/app.rules.json')
        createAndCacheNCube('rules/ncubes/app.rules.categories.json')
        createAndCacheNCube('rules/ncubes/rule.group1.type1.object1.json')
        createAndCacheNCube('rules/ncubes/rule.group1.type1.object2.json')
        createAndCacheNCube('rules/ncubes/rule.group2.type1.object1.json')
        createAndCacheNCube('rules/ncubes/rule.group1.type2.object1.json')
        createAndCacheNCube('rules/ncubes/rule.group4.object1.json')
    }

    @After
    void clearNCubes()
    {
        ncubeRuntime.clearCache(testAppId)
    }

    @Test
    void testConstructor()
    {
        RulesEngine engine = new RulesEngine('foo', testAppId, 'rules')
        assert 'foo' == engine.name
        assert testAppId == engine.appId
    }

    @Test
    void testVerifySetup_NoRules()
    {
        ncubeRuntime.clearCache(testAppId, [NCUBE_RULES])
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'requires', 'ncube', 'appid')
        }
    }

    @Test
    void testVerifySetup_NoCategories()
    {
        ncubeRuntime.clearCache(testAppId, [NCUBE_CATEGORIES])
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'requires', 'ncube', 'appid')
        }
    }

    @Test
    void testVerifySetup_NoRuleGroupAxis()
    {
        createAndCacheNCube('rules/ncubes/malformed/app.rules-no-rule-group.json')
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'must have', 'axis', AXIS_RULE_GROUP)
        }
    }

    @Test
    void testVerifySetup_RuleGroupNotDiscrete()
    {
        createAndCacheNCube('rules/ncubes/malformed/app.rules-rule-group-not-discrete.json')
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'axis', 'type', 'discrete')
        }
    }

    @Test
    void testVerifySetup_RuleGroupNotString()
    {
        createAndCacheNCube('rules/ncubes/malformed/app.rules-rule-group-not-string.json')
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'axis', 'value type', 'string')
        }
    }

    @Test
    void testVerifySetup_NoClassNameColumn()
    {
        createAndCacheNCube('rules/ncubes/malformed/app.rules-no-className.json')
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'axis', 'must', 'column')
        }
    }

    @Test
    void testVerifySetup_NoNCubeColumn()
    {
        createAndCacheNCube('rules/ncubes/malformed/app.rules-no-ncube.json')
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'axis', 'must', 'column')
        }
    }

    @Test
    void testVerifySetup_NoAttributeAxis()
    {
        createAndCacheNCube('rules/ncubes/malformed/app.rules-no-attribute.json')
        try
        {
            rulesEngine.generateDocumentation('')
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'must have', 'axis', AXIS_ATTRIBUTE)
        }
    }

    @Test
    void testVerificationComplete()
    {
        assert !rulesEngine.verificationComplete
        rulesEngine.generateDocumentation('')
        assert rulesEngine.verificationComplete
        rulesEngine.generateDocumentation('')
        assert rulesEngine.verificationComplete
    }

    @Test
    void testExecute_NullGroup()
    {
        try
        {
            rulesEngine.execute((String) null, [:])
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'rule group', 'not', 'null')
        }
    }

    @Test
    void testExecuteGroups_NullGroups()
    {
        try
        {
            rulesEngine.executeGroups((List) null, [:])
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'rule groups', 'not', 'null')
        }
    }

    @Test
    void testExecuteGroups_EmptyGroups()
    {
        Map root = [:]
        rulesEngine.executeGroups([], root)
        assert root.isEmpty()
    }

    @Test
    void testExecuteGroups_InvalidGroup()
    {
        Map root = [:]
        rulesEngine.executeGroups(['group5'], root)
        assert root.isEmpty()
    }

    @Test
    void testExecuteGroups_GroupWithNonExistantNCube()
    {
        Map root = [:]
        try
        {
            rulesEngine.executeGroups(['invalidGroup'], root)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.localizedMessage, "rulesengine", "NCube", "defined", "does not exist.")
        }
        assert root.isEmpty()
    }

    @Test
    void testExecuteGroups_Group1()
    {
        Map root = [:]
        rulesEngine.executeGroups(['group1'], root)
        assert 3 == root.keySet().size()
        assert root['rule1']
        assert root['rule2']
        assert root['rule3']
    }

    @Test
    void testExecuteGroups_Group2()
    {
        Map root = [:]
        rulesEngine.executeGroups(['group2'], root)
        assert 1 == root.keySet().size()
        assert root['rule5']
    }

    @Test
    void testExecuteGroups_Group3()
    {
        Map root = [:]
        rulesEngine.executeGroups(['group3'], root)
        assert 1 == root.keySet().size()
        assert root['rule4']
    }

    @Test
    void testExecute_AllGroups()
    {
        Map root = [:]
        rulesEngine.executeGroups(['group1', 'group2', 'group3'], root)
        assert 5 == root.keySet().size()
        assert root['rule1']
        assert root['rule2']
        assert root['rule3']
        assert root['rule4']
        assert root['rule5']
    }

    @Test
    void testExecute_Closure_Null()
    {
        try
        {
            rulesEngine.execute((Closure) null, [:])
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'where', 'not', 'null')
        }
    }

    @Test
    void testExecute_Closure_SingleCategory()
    {
        Map root = [:]
        Closure where = {Map input -> input['category1'] == 'foo'}
        rulesEngine.execute(where, root)
        assert 4 == root.keySet().size()
        assert root['rule1']
        assert root['rule2']
        assert root['rule3']
        assert root['rule5']
    }

    @Test
    void testExecute_Closure_MultipleCategories()
    {
        Map root = [:]
        Closure where = {Map input -> input['category1'] == 'foo' && input['category3'] == 'tiger'}
        rulesEngine.execute(where, root)
        assert 3 == root.keySet().size()
        assert root['rule1']
        assert root['rule2']
        assert root['rule3']
    }

    @Test
    void testExecute_Map_SingleCategory()
    {
        Map root = [:]
        Map categories = [category1: 'foo'] as Map
        rulesEngine.execute(categories, root)
        assert 4 == root.keySet().size()
        assert root['rule1']
        assert root['rule2']
        assert root['rule3']
        assert root['rule5']
    }

    @Test
    void testExecute_Map_ListValue()
    {
        Map root = [:]
        Map categories = [category2: ['apple', 'orange']] as Map
        rulesEngine.execute(categories, root)
        assert 4 == root.keySet().size()
        assert root['rule1']
        assert root['rule2']
        assert root['rule3']
        assert root['rule5']
    }

    @Test
    void testExecute_List()
    {
        Map root = [:]
        Map input = [:]
        Map output = [count: 0]
        rulesEngine.execute([[category1: 'foo'] as Map, [category2: 'apple'] as Map], root, input, output)
        assert 4 == root.keySet().size()
        assert 2 == output['count']
    }

    @Test
    void testGenerateDocumentation_Group1And3()
    {
        String json = NCubeRuntime.getResourceAsString('rules/group1and3.json')
        Map expected = (Map) JsonReader.jsonToJava(json)
        Map actual = rulesEngine.generateDocumentationForGroups(['group1', 'group3'])
        assert DeepEquals.deepEquals(expected, actual)
    }

    @Test
    void testGenerateDocumentation_Group2()
    {
        String json = NCubeRuntime.getResourceAsString('rules/group2.json')
        Map expected = (Map) JsonReader.jsonToJava(json)
        Map actual = rulesEngine.generateDocumentationForGroups(['group2'])
        assert DeepEquals.deepEquals(expected, actual)
    }

    @Test
    void testGenerateDocumentation_InvalidGroups()
    {
        Map rules = rulesEngine.generateDocumentationForGroups(['group5'])
        assert rules.isEmpty()
    }

    @Test
    void testGenerateDocumentation_EmptyGroups()
    {
        Map rules = rulesEngine.generateDocumentationForGroups([])
        assert rules.isEmpty()
    }

    @Test
    void testGenerateDocumentation_NullGroups()
    {
        try
        {
            rulesEngine.generateDocumentationForGroups((List) null)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'rule groups', 'not', 'null')
        }
    }

    @Test
    void testGenerateDocumentation_NullGroup()
    {
        try
        {
            rulesEngine.generateDocumentation((String) null)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'rule group', 'not', 'null')
        }
    }

    @Test
    void testGenerateDocumentation_Closure()
    {
        String json = NCubeRuntime.getResourceAsString('rules/group2.json')
        Map expected = (Map) JsonReader.jsonToJava(json)
        Closure where = {Map input -> input['category2'] == 'orange'}
        Map actual = rulesEngine.generateDocumentation(where)
        assert DeepEquals.deepEquals(expected, actual)
    }

    @Test
    void testGenerateDocumentation_Map()
    {
        String json = NCubeRuntime.getResourceAsString('rules/group1and3.json')
        Map expected = (Map) JsonReader.jsonToJava(json)
        Map categories = [category3: 'tiger'] as Map
        Map actual = rulesEngine.generateDocumentation(categories)
        assert DeepEquals.deepEquals(expected, actual)
    }

    @Test
    void testGenerateDocumentation_List()
    {
        String json = NCubeRuntime.getResourceAsString('rules/group1and3.json')
        Map expected = (Map) JsonReader.jsonToJava(json)
        List categories = [[category1: 'foo', category2: 'apple'] as Map, [category1: 'bar'] as Map]
        Map actual = rulesEngine.generateDocumentation(categories)
        assert DeepEquals.deepEquals(expected, actual)
    }

    @Test
    void testGetUiInfo()
    {
        String json = NCubeRuntime.getResourceAsString('rules/info.json')
        Map expected = (Map) JsonReader.jsonToJava(json)
        Map actual = rulesEngine.info
        assert DeepEquals.deepEquals(expected, actual)
    }

    @Test
    void testExecute()
    {
        Map root = [:]
        rulesEngine.execute('group2', root)
        assert 1 == root.keySet().size()
        assert root['rule5']
    }

    @Test
    void testExecute_OrchestrationMissingRuleAxis()
    {
        createAndCacheNCube('rules/ncubes/malformed/rule-axis-name.json')

        try
        {
            rulesEngine.execute('group1', [:])
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'axis', AXIS_RULE)
        }
    }

    @Test
    void testExecute_OrchestrationWrongAxisType()
    {
        createAndCacheNCube('rules/ncubes/malformed/rule-wrong-axis-type.json')

        try
        {
            rulesEngine.execute('group1', [:])
            fail()
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'rulesengine', 'appid', 'ncube', 'axis', 'type', RULE.name())
        }
    }

    @Test
    void testExecute_ThrowException()
    {
        Map root = [:]
        try
        {
            rulesEngine.execute('group4', root)
            fail()
        }
        catch (RulesException e)
        {
            assert 1 == e.errors.size()
        }

    }

    @Test
    void testExecute_NoException()
    {
        Map root = [:]
        List<RulesError> errors = rulesEngine.execute('group3', root)
        assert 1 == errors.size()
    }

}
