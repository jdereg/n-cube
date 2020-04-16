package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CommandCellException
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.TestUtil
import groovy.transform.CompileStatic
import org.junit.Test

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.fail

/**
 * NCube RuleEngine Tests
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
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
class TestRuleEngine extends NCubeBaseTest
{
    @Test
    void testRuleAtJumpNullingOutInputCoord()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleUsingAtandUse.json')
        Map input = [type:'USE']
        Map output = [:]
        output.steps = []
        ncube.getCell(input, output)
        output.remove('_rule')
        output.remove('return')
        List steps = (List)output.steps
        assert 'use Test1' == steps[0]
        assert 'default Test1' == steps[1]
        assert 'use Test2' == steps[2]
        assert 'default Test2' == steps[3]

        input = [type:'AT']
        output.steps = []
        ncube.getCell(input, output)
        output.remove('_rule')
        output.remove('return')
        steps = (List)output.steps
        assert 'at Test1' == steps[0]
        assert 'default Test1' == steps[1]
        assert 'default Test2' == steps[2]
        assert 'at Test2' == steps[3]
        assert 'default Test1' == steps[4]
        assert 'default Test2' == steps[5]

        input = [type:'GO']
        output.steps = []
        ncube.getCell(input, output)
        output.remove('_rule')
        output.remove('return')
        steps = (List)output.steps
        assert 'go Test1' == steps[0]
        assert 'default Test1' == steps[1]
        assert 'default Test2' == steps[2]
        assert 'go Test2' == steps[3]
        assert 'default Test1' == steps[4]
        assert 'default Test2' == steps[5]
    }

    // This test also tests ID-based ncube's specified in simple JSON format
    @Test
    void testRuleCube()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'expressionAxis.json')
        Axis cond = ncube.getAxis 'condition'
        assert cond.columns[0].id != 1
        Axis state = ncube.getAxis 'state'
        assert state.columns[0].id != 10

        Map<String, Object> input = [vehiclePrice: 5000.0, driveAge: 22, gender: 'male', vehicleCylinders: 8, state: 'TX'] as Map
        Map output = [:]
        Object out = ncube.getCell(input, output)
        assert out == 10
        assert output.premium == 119.0

        try
        {
            input['state'] = "BOGUS"
            ncube.getCell(input, output)
            fail("should not make it here")
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains('not found on axis')
            assert ncube.name == e.cubeName
            assert input == e.coordinate
            assert 'state' == e.axisName
            assert 'BOGUS' == e.value
        }

    }

    // This test also tests ID-based ncube's specified in simple JSON format
    @Test
    void testExpressionValue()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'expressionAxis.json')
        Axis cond = ncube.getAxis 'condition'
        assert cond.columns[0].id != 1
        Axis state = ncube.getAxis 'state'
        assert state.columns[0].id != 10
        assert 'foo' == state.standardizeColumnValue('foo')
    }

    // This test ensures that identical expressions result in a single dynamic Groovy class being generated for them.
    @Test
    void testDuplicateExpression()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId,  'duplicateExpression.json')
        Map output = [:]
        def out = ncube.getCell([vehiclePrice: 5000.0, driveAge: 22, gender: 'male', vehicleCylinders: 8], output)
        assert out == 10
        assert output.premium == 119.0
    }

    @Test
    void testRequiredScopeRuleAxis()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'expressionAxis.json')

        Set<String> reqScope = ncube.getRequiredScope([:], [:])
        assert 1 == reqScope.size()
        assert reqScope.contains('state')

        Set<String> optScope = ncube.getOptionalScope([:], [:])
        assert 1 == optScope.size()
    }

    @Test
    void testCubeRefFromRuleAxis()
    {
        NCube ncube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube5.json')
        Set reqScope = ncube1.getRequiredScope([:], [:])
        Set optScope = ncube1.getOptionalScope([:], [:])
        assert optScope.size() == 1
        assert optScope.contains('Age')
        assert 0 == reqScope.size()

        NCube ncube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId,  'expressionAxis2.json')
        reqScope = ncube2.getRequiredScope([:], [:])
        assert reqScope.size() == 1
        assert reqScope.contains('state')
        optScope = ncube2.getOptionalScope([:], [:])
        assert 1 == optScope.size()

        def coord = [age: 18, state: 'OH'] as Map
        Map output = [:]
        ncube2.getCell(coord, output)
        assert 5.0 == output.premium

        coord.state = 'TX'
        output.clear()
        ncube2.getCell(coord, output)
        assert -5.0 == output.premium

        output.clear()
        ncube2.getCell([state: 'OH', Age: 23], output)
        assert 1.0 == output.premium
    }

    @Test
    void testMultipleRuleAxisBindings()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId,  'multiRule.json')
        Map output = [:]
        ncube.getCell([age: 10, weight: 50], output)

        assert output.weight == 'medium-weight'
        assert output.age == 'adult'
        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 4L == ruleInfo.getNumberOfRulesExecuted()

        output.clear()
        ncube.getCell([age: 10, weight: 150], output)
        assert output.weight == 'medium-weight'
        assert output.age == 'adult'
        ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 2L == ruleInfo.getNumberOfRulesExecuted()

        output.clear()
        ncube.getCell([age: 35, weight: 150], output)
        assert output.weight == 'medium-weight'
        assert output.age == 'adult'
        ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 1L == ruleInfo.getNumberOfRulesExecuted()

        output.clear()
        ncube.getCell([age: 42, weight: 205], output)
        assert output.weight == 'heavy-weight'
        assert output.age == 'middle-aged'
        ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 1L == ruleInfo.getNumberOfRulesExecuted()
    }

    @Test
    void testMultipleRuleAxisBindingsOKInMultiDim()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId,  'multiRule2.json')
        Map output = [:]
        ncube.getCell([age: 10, weight: 60], output)
        assert output.weight == 'light-weight'

        // The age is 'adult' because two rules are matching on the age axis (intentional rule error)
        // This test illustrates that I can match 2 or more rules on one rule axis, 1 on a 2nd rule
        // axis.
        assert output.age == 'adult'
    }

    @Test
    void testRuleStopCondition()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId,  'multiRuleHalt.json')
        Map output = [:]
        ncube.getCell([age: 10, weight: 60], output)
        assert output.age == 'young'
        assert output.weight == 'light-weight'
        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 1L == ruleInfo.getNumberOfRulesExecuted()
        assertFalse ruleInfo.wasRuleStopThrown()

        output.clear()
        ncube.getCell([age: 25, weight: 60], output)
        ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 0L == ruleInfo.getNumberOfRulesExecuted()
        assert ruleInfo.wasRuleStopThrown()

        output.clear()
        ncube.getCell([age: 45, weight: 60], output)
        assert output.age == 'middle-aged'
        assert output.weight == 'light-weight'
        ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 1L == ruleInfo.getNumberOfRulesExecuted()
        assertFalse ruleInfo.wasRuleStopThrown()
    }

    @Test
    void testDefaultColumnOnRuleAxis()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleWithDefault.json')

        Map output = [:]
        def coord = [:]

        coord.state = 'OH'
        coord.age = 18
        ncube.getCell coord, output
        assert output.text == 'OH 18'
        assert ncube.containsCell(coord)

        coord.state = 'TX'
        ncube.getCell coord, output
        assert output.text == 'TX 18'
        assert ncube.containsCell(coord)

        coord.state = 'GA'
        ncube.getCell coord, output
        assert output.text == 'GA 18'
        assert ncube.containsCell(coord)

        coord.state = 'AZ'
        ncube.getCell coord, output
        assert output.text == 'default 18'
        assert ncube.containsCell(coord)

        coord.state = 'OH'
        coord.age = 50
        ncube.getCell coord, output
        assert output.text == 'OH 50'
        assert ncube.containsCell(coord)

        coord.state = 'TX'
        ncube.getCell coord, output
        assert output.text == 'TX 50'
        assert ncube.containsCell(coord)

        coord.state = 'GA'
        ncube.getCell coord, output
        assert output.text == 'GA 50'
        assert ncube.containsCell(coord)

        coord.state = 'AZ'
        ncube.getCell coord, output
        assert output.text == 'default 50'
        assert ncube.containsCell(coord)

        coord.state = 'OH'
        coord.age = 85
        ncube.getCell coord, output
        assert output.text == 'OH 85'
        assert ncube.containsCell(coord)

        coord.state = 'TX'
        ncube.getCell coord, output
        assert output.text == 'TX 85'
        assert ncube.containsCell(coord)

        coord.state = 'GA'
        ncube.getCell coord, output
        assert output.text == 'GA 85'
        assert ncube.containsCell(coord)

        coord.state = 'AZ'
        ncube.getCell coord, output
        assert output.text == 'default 85'
        assert ncube.containsCell(coord)

        coord.state = 'OH'
        coord.age = 100
        ncube.getCell coord, output
        assert output.text == 'OH default'
        assert ncube.containsCell(coord)

        coord.state = 'TX'
        ncube.getCell coord, output
        assert output.text == 'TX default'
        assert ncube.containsCell(coord)

        coord.state = 'GA'
        ncube.getCell coord, output
        assert output.text == 'GA default'
        assert ncube.containsCell(coord)

        coord.state = 'AZ'
        ncube.getCell coord, output
        assert output.text == 'default default'
        assert ncube.containsCell(coord)
    }

    @Test
    void testRuleAxisWithNoMatchAndNoDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleNoMatch.json')

        def coord = [age: 85]
        Map output = [:]
        try
        {
            ncube.getCell(coord, output)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            ruleAxisDidNotBind(e, ncube.name, coord)
        }
        assert 1 == output.size()
        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 0L == ruleInfo.getNumberOfRulesExecuted()

        coord.age = 22
        ncube.getCell(coord, output)
        assert output.containsKey('adult')
        assert output.containsKey('old')
        ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 2L == ruleInfo.getNumberOfRulesExecuted()
    }

    private static void ruleAxisDidNotBind(CoordinateNotFoundException e, String cubeName, Map coordinate)
    {
        assert e.message.toLowerCase().contains("no condition")
        assert e.message.toLowerCase().contains("fired")
        assert e.message.toLowerCase().contains("no default")
        assert e.message.toLowerCase().contains("rule axis")
        assert cubeName == e.cubeName
        assert coordinate == e.coordinate
        assert e.axisName
        assert e.value == null
    }

    @Test
    void testContainsCellValueRule()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'containsCellRule.json')

        def coord = [condition: 'Male']
        assert ncube.containsCell(coord, true)
        try
        {
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            ruleAxisDidNotBind(e, ncube.name, coord)
        }

        coord.condition = 'Female'
        assert ncube.containsCell(coord)
        try
        {
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            ruleAxisDidNotBind(e, ncube.name, coord)
        }
        coord.gender = 'Female'
        assert 'bar' == ncube.getCell(coord)

        try
        {
            coord.condition = 'GI Joe'
            ncube.containsCell(coord)
            fail('should not make it here')
        }
        catch (CoordinateNotFoundException e)
        {
            e.printStackTrace()
            TestUtil.assertContainsIgnoreCase(e.message, 'GI Joe', 'not', 'found', 'condition', 'containsCellRule')
        }

        ncube.defaultCellValue = null

        coord.condition = 'Male'
        assertFalse ncube.containsCell(coord)
        coord.condition = 'Female'
        assert ncube.containsCell(coord)
    }

    @Test
    void testOneRuleSetCallsAnotherRuleSet()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleSet2.json')
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleSet1.json')
        Map input = [age: 10]
        Map output = [:]
        ncube.getCell input, output
        assert 1.0 == output.total

        input.age = 48
        ncube.getCell input, output
        assert 8.560 == output.total

        input.age = 84
        ncube.getCell input, output
        assert 5.150 == output.total
    }

    @Test
    void testBasicJump()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'basicJump.json')
        Map input = [age: 10]
        Map output = [:]
        ncube.getCell(input, output)

        assert 'child' == output.group
        assert 'thang' == output.thing

        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 3L == ruleInfo.getNumberOfRulesExecuted()
//        System.out.println('ruleInfo.getRuleExecutionTrace() = ' + ruleInfo.getRuleExecutionTrace())

        input.age = 48
        ncube.getCell(input, output)
        assert 'adult' == output.group

        input.age = 84
        ncube.getCell(input, output)
        assert 'geezer' == output.group
    }

    @Test
    void testMultipleRuleAxesWithMoreThanOneRuleFiring()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'multiRule.json')
        assert 'medium-weight' == ncube.getCell([age: 35, weight: 99])
    }

    @Test
    void testRuleFalseValues()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleFalseValues.json')
        Map input = [state: 'OH']
        Map output = [:]
        ncube.getCell input, output
        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 1L == ruleInfo.getNumberOfRulesExecuted()
    }

    @Test
    void testJumpStart()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'basicJumpStart.json')
        Map input = [letter: 'e']
        Map output = [:] as CaseInsensitiveMap
        ncube.getCell input, output

        assert output.containsKey('A')           // condition ran (condition axis was told to start at beginning - null)
        assert output.containsKey('B')
        assert output.containsKey('C')
        assert output.containsKey('D')
        assert output.containsKey('E')
        assert output.containsKey('F')
        assert output.containsKey('G')
        assert 'echo' == output.word

        input.condition = 'e'
        output.clear()
        ncube.getCell input, output

        assertFalse output.containsKey('A')           // condition never ran (condition axis was told to start at a)
        assertFalse output.containsKey('B')           // condition never ran (condition axis was told to start at a)
        assertFalse output.containsKey('C')           // condition never ran (condition axis was told to start at a)
        assertFalse output.containsKey('D')           // condition never ran (condition axis was told to start at a)
        assert output.containsKey('E')            // condition ran
        assert output.containsKey('F')            // condition ran
        assert output.containsKey('G')            // condition ran
        assert 'echo' == output.word
    }

    @Test
    void testJumpStart2D()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'basicJumpStart2D.json')
        Map input = [letter: 'f', column: 'y', condition: 'f', condition2: 'y']
        Map output = [:] as CaseInsensitiveMap
        ncube.getCell input, output

        assertFalse output.containsKey('A')
        // skipped - condition never ran (condition axis was told to start at f)
        assertFalse output.containsKey('B')       // skipped
        assertFalse output.containsKey('C')       // skipped
        assertFalse output.containsKey('D')       // skipped
        assertFalse output.containsKey('E')       // skipped
        assert output.containsKey('F')        // condition ran
        assert output.containsKey('G')        // condition ran
        assert 'foxtrot' == output.word

        assertFalse output.containsKey('W')
        // skipped - condition never ran (condition2 axis was told to start at y)
        assertFalse output.containsKey('X')       // skipped
        assert output.containsKey('Y')        // condition ran
        assert output.containsKey('Z')        // condition ran
        assert 'y' == output.col
    }

    @Test
    void testJumpRestart()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'basicJumpRestart.json')
        Map input = [letter: 'e']
        Map output = [:] as CaseInsensitiveMap
        output.a = 0
        output.b = 0
        output.c = 0
        output.d = 0
        output.e = 0
        output.f = 0
        output.g = 0
        output.word = ''
        ncube.getCell(input, output)

        assert output.containsKey('A')
        // condition ran (condition axis was told to start at beginning - null)
        assert output.containsKey('B')
        assert output.containsKey('C')
        assert output.containsKey('D')
        assert output.containsKey('E')
        assert output.containsKey('F')
        assert output.containsKey('G')
        assert 'echoecho' == output.word

        assert 1 == output.a
        assert 1 == output.b
        assert 1 == output.c
        assert 1 == output.d
        assert 2 == output.e       // This step is run twice.
        assert 1 == output.f
        // This step is run once (skipped the first time, then after 'e' runs a 2nd time)
        assert 1 == output.g
        // This step is run once (skipped the first time, then after 'e' runs a 2nd time)
    }

    @Test
    void testNoRuleBinding()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleSet2.json')
        Map output = [:]

        try
        {
            ncube.getCell([:], output)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains("no condition")
            assert e.message.toLowerCase().contains("rule axis")
            assert e.message.toLowerCase().contains("fired")
            assert e.message.toLowerCase().contains("no default column")
        }
        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        assert 0L == ruleInfo.getNumberOfRulesExecuted()
    }

    @Test
    void testRuleStop()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleStop.json')
        Map output = [:]
        ncube.getCell([:], output)
        assert 200 == output.price
    }

    @Test
    void testRuleInfo()
    {
        RuleInfo ruleInfo = new RuleInfo()
        assert '' == ruleInfo.getSystemOut()
        ruleInfo.setSystemOut('the quick brown fox')
        assert 'the quick brown fox' == ruleInfo.getSystemOut()
        assert '' == ruleInfo.getSystemErr()
        ruleInfo.setSystemErr('the quick brown dog')
        assert 'the quick brown dog' == ruleInfo.getSystemErr()
        assertNull ruleInfo.getLastExecutedStatementValue()
    }

    @Test
    void testRuleBinding()
    {
        Binding binding = new Binding('fancy', 2)
        assert 'fancy' == binding.cubeName

        String html = binding.toHtml()
        assert html.contains(' fancy')
        assert html.contains('value')
        assert html.contains('null')
        assertNotNull binding.toString()
    }

    @Test
    void testRuleInfoRuleName()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'multiRule.json')
        Map input = [age: 18, weight: 125]
        Map output = [:]
        def ret = ncube.getCell input, output
        assert 'medium-weight' == ret
        RuleInfo ruleInfo = (RuleInfo) output[NCube.RULE_EXEC_INFO]
        List<Binding> bindings = ruleInfo.getAxisBindings()
        for (Binding binding : bindings)
        {
            String html = binding.toHtml()
            assert html.contains('medium /')
            assert binding.value != null
            assert binding.coordinate.size() > 0
        }
    }

    @Test
    void testRuleInfoUnboundAxes_ruleCube_noUnboundAxes()
    {
        NCube cube = NCubeBuilder.ruleCubeWithDefaultColumn
        cube.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(cube)

        //No unbound axes
        Map input = [RuleAxis1: "${'(Condition1): true'}",
                     Axis2    : 'Axis2Col2',
                     foo      : true]
        Map output = [:]
        cube.getCell(input, output)
        RuleInfo ruleInfo = cube.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 0
    }

    @Test
    void testRuleInfoUnboundAxes_ruleCube_nonRuleAxisIsUnbound()
    {
        NCube cube = NCubeBuilder.ruleCubeWithDefaultColumn
        cube.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(cube)

        //Non-rule axis is unbound
        Map input = [RuleAxis1: "${'(Condition1): true'}",
                     Axis2: 'bogus',
                     foo: true]
        Map output = [:]
        cube.getCell(input, output)
        RuleInfo ruleInfo = cube.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 1
        Map unboundAxes = ruleInfo.getUnboundAxesMap()
        assert unboundAxes.size() == 1
        Map unboundAxesForCube = unboundAxes[cube.name]
        assert unboundAxesForCube.size() == 1
        Set values = unboundAxesForCube['Axis2']
        assert values.size() == 1
        assert values.contains('bogus')
    }

    @Test
    void testRuleInfoUnboundAxes_ruleCube_ruleAxisIsUnbound()
    {
        NCube cube = NCubeBuilder.ruleCubeWithDefaultColumn
        cube.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(cube)

        //Rule axis is unbound
        Map input = [Axis2 : 'Axis2Col2']
        Map output = [:]
        cube.getCell(input, output)
        RuleInfo ruleInfo = cube.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 1
        Map unboundAxes = ruleInfo.getUnboundAxesMap()
        assert unboundAxes.size() == 1
        Map unboundAxesForCube = unboundAxes[cube.name]
        assert unboundAxesForCube.size() == 1
        Set values = unboundAxesForCube['RuleAxis1']
        assert values.size() == 1
        assert values.contains(null)
    }

    @Test
    void testRuleInfoUnboundAxes_noUnboundAxes()
    {
        NCube primary = NCubeBuilder.cubeCallingCubeWithDefaultColumn
        NCube secondary = NCubeBuilder.cubeWithDefaultColumn
        primary.applicationID = ApplicationID.testAppId
        secondary.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(primary)
        ncubeRuntime.addCube(secondary)

        //Primary cube calls secondary cube.
        //No unbound axes
        Map input = [Axis1Primary  : 'Axis1Col1',
                     Axis2Primary  : 'Axis2Col1',
                     Axis1Secondary: 'Axis1Col1',
                     Axis2Secondary: 'Axis2Col1',
                     Axis3Secondary: 'Axis3Col1']
        Map output = [:]
        primary.getCell(input, output)
        RuleInfo ruleInfo = primary.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 0

    }

    @Test
    void testRuleInfoUnboundAxes_valueNotFound()
    {
        NCube primary = NCubeBuilder.cubeCallingCubeWithDefaultColumn
        NCube secondary = NCubeBuilder.cubeWithDefaultColumn
        primary.applicationID = ApplicationID.testAppId
        secondary.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(primary)
        ncubeRuntime.addCube(secondary)

        //Primary cube calls secondary cube.
        //One unbound column with a value provided, but not found.
        Map input = [Axis1Primary  : 'Axis1Col1',
                     Axis2Primary  : 'Axis2Col1',
                     Axis1Secondary: 'bogus',
                     Axis2Secondary: 'Axis2Col1',
                     Axis3Secondary: 'Axis3Col1']
        Map output = [:]
        primary.getCell(input, output)
        RuleInfo ruleInfo = primary.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 1
        Map unboundAxes = ruleInfo.getUnboundAxesMap()
        assert unboundAxes.size() == 1
        Map unboundAxesForCube = unboundAxes[secondary.name]
        assert unboundAxesForCube.size() == 1
        Set values = unboundAxesForCube['Axis1Secondary']
        assert values.size() == 1
        assert values.contains('bogus')
    }

    @Test
    void testRuleInfoUnboundAxes_noValueProvided()
    {
        NCube primary = NCubeBuilder.cubeCallingCubeWithDefaultColumn
        NCube secondary = NCubeBuilder.cubeWithDefaultColumn
        primary.applicationID = ApplicationID.testAppId
        secondary.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(primary)
        ncubeRuntime.addCube(secondary)

        //Primary cube calls secondary cube.
        //One unbound column with a value provided, but not found.
        //One unbound column, no value was provided.
        Map input = [Axis1Primary  : 'Axis1Col1',
                     Axis2Primary  : 'Axis2Col1',
                     Axis1Secondary: 'bogus',
                     Axis2Secondary: 'Axis2Col2']
        Map output = [:]
        primary.getCell(input, output)
        RuleInfo ruleInfo = primary.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 1
        Map unboundAxes = ruleInfo.getUnboundAxesMap()
        assert unboundAxes.size() == 1
        Map unboundAxesForCube = unboundAxes[secondary.name]
        assert unboundAxesForCube.size() == 2
        Set values = unboundAxesForCube['Axis1Secondary']
        assert values.size() == 1
        assert values.contains('bogus')
        values = unboundAxesForCube['Axis3Secondary']
        assert values.size() == 1
        assert values.contains(null)
    }

    @Test
    void testRuleInfoUnboundAxes_unboundAxesOnTwoCubes()
    {
        NCube primary = NCubeBuilder.cubeCallingCubeWithDefaultColumn
        NCube secondary = NCubeBuilder.cubeWithDefaultColumn
        primary.applicationID = ApplicationID.testAppId
        secondary.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(primary)
        ncubeRuntime.addCube(secondary)

        //Primary cube calls secondary cube and secondary cube calls back to different cell on primary.
        Map input = [Axis1Primary  : 'Axis1Col2',
                     Axis2Primary  : 'Axis2Col2',
                     Axis1Secondary: 'bogus2',
                     Axis2Secondary: 'Axis2Col1',
                     Axis3Secondary: 'bogus3']
        Map output = [:]
        primary.getCell(input, output)
        RuleInfo ruleInfo = primary.getRuleInfo(output)
        assert ruleInfo.getUnboundAxesMap().size() == 2
        Map unboundAxes =  ruleInfo.getUnboundAxesMap()
        assert unboundAxes.size() == 2
        Map unboundAxesForCube = unboundAxes[secondary.name]
        assert unboundAxesForCube.size() == 2
        Set values = unboundAxesForCube['Axis1Secondary']
        assert values.size() == 1
        assert values.contains('bogus2')
        values = unboundAxesForCube['Axis3Secondary']
        assert values.size() == 1
        assert values.contains('bogus3')
        unboundAxesForCube = unboundAxes[primary.name]
        assert unboundAxesForCube.size() == 1
        values = unboundAxesForCube['Axis1Primary']
        assert values.size() == 1
        assert values.contains(null)
    }

    @Test
    void testRuleSimpleWithDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleSimpleWithDefault.json')
        Map input = [state: 'OH']
        Map output = [:]
        def ret = ncube.getCell(input, output)
        assert ret == 'Ohio'
        assert output.text == 'Ohio'

        input = [state: 'OH', rule: 'OhioRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'Ohio'
        assert output.text == 'Ohio'

        input = [state: 'O', rule: 'OhioRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'nope'
        assert output.text == 'nope'

        input = [state: 'O']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'nope'
        assert output.text == 'nope'

        input = [state: 'TX']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'Texas'
        assert output.text == 'Texas'

        input = [state: 'TX', rule: 'TexasRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'Texas'
        assert output.text == 'Texas'

        input = [state: 'O', rule: 'TexasRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'nope'
        assert output.text == 'nope'

        // Starting at 'OhioRule' but input value is TX so we should get Texas
        input = [state: 'TX', rule: 'OhioRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'Texas'
        assert output.text == 'Texas'

        // Starting at 'TexasRule' but input value is OH so we should get 'no state' (because of rule order)
        input = [state: 'OH', rule: 'TexasRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'nope'
        assert output.text == 'nope'

        input = [state: 'OH', rule: 'MatchesNoRuleName']
        output = [:]
        ncube.getCell(input, output)
        assert ret == 'nope'
        assert output.text == 'nope'
    }

    @Test
    void testRuleSimpleWithNoDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleSimpleWithNoDefault.json')
        Map input = [state: 'OH']
        Map output = [:]
        def ret = ncube.getCell(input, output)
        assert ret == 'Ohio'
        assert output.text == 'Ohio'

        input = [state: 'OH', rule: 'OhioRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'Ohio'
        assert output.text == 'Ohio'

        input = [state: 'O', rule: 'OhioRule']
        output = [:]
        try
        {
            ncube.getCell(input, output)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            ruleAxisDidNotBind(e, ncube.name, input)
        }

        input = [state: 'TX', rule: 'OhioRule']
        output = [:]
        ret = ncube.getCell(input, output)
        assert ret == 'Texas'
        assert output.text == 'Texas'

        input = [state: 'OH', rule: 'TexasRule']
        output = [:]
        try
        {
            ncube.getCell(input, output)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            ruleAxisDidNotBind(e, ncube.name, input)
        }

        input = [state: 'OH', rule: 'MatchesNoRuleName']
        output = [:]
        try
        {
            ncube.getCell(input, output)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains('rule named')
            assert e.message.toLowerCase().contains('matches no column')
            assert e.message.toLowerCase().contains('no default column')
        }
    }

    @Test
    void testFireOne()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleFireOneVer1D.json')

        // Start at 1st rule
        Map input = [:]
        Map output = [:]
        def ret = ncube.getCell(input, output)
        assert output.A == 'A fired'
        assert ret == 'A fired'
        assert output.size() == 3   // A, _rule, return

        // Start on 2nd rule
        input = [rule: 'SecondRule']
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.B == 'B fired'
        assert ret == 'B fired'
        assert output.size() == 3   // B, _rule, return

        Axis axis = ncube.getAxis('rule')
        axis.fireAll = true
        input.clear()
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.A == 'A fired'
        assert output.B == 'B fired'
        assert ret == 'B fired'
        assert output.size() == 4   // A, B, _rule, return
    }

    @Test
    void testFireOne2D()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleFireOneVer2D.json')
        Map input = [:]
        Map output = [:]
        def ret = ncube.getCell(input, output)
        assert output.A1 == 'A1 fired'
        assert ret == 'A1 fired'
        assert output.size() == 3   // B, _rule, return

        input = [ruleLetter: 'BRule']
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.B1 == 'B1 fired'
        assert ret == 'B1 fired'
        assert output.size() == 3   // B, _rule, return

        input = [ruleNumber: '2Rule']
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.A2 == 'A2 fired'
        assert ret == 'A2 fired'
        assert output.size() == 3   // B, _rule, return

        input = [ruleNumber: '2Rule', ruleLetter: 'BRule']
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.B2 == 'B2 fired'
        assert ret == 'B2 fired'
        assert output.size() == 3   // B, _rule, return

        // Switch RuleNumber axis back to fireAll=true
        Axis axis = ncube.getAxis('ruleNumber')
        axis.fireAll = true
        input.clear()
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.size() == 4
        assert output.A1 == 'A1 fired'
        assert output.A2 == 'A2 fired'
        assert ret == 'A2 fired'

        axis.fireAll = false
        axis = ncube.getAxis('ruleLetter')
        axis.fireAll = true
        input.clear()
        output.clear()
        ret = ncube.getCell(input, output)
        assert output.size() == 4
        assert output.A1 == 'A1 fired'
        assert output.B1 == 'B1 fired'
        assert ret == 'B1 fired'
    }

    @Test
    void testRuleFire()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'rule-test-1.json')
        Map input = [bu: 'R', item: new Date()]
        Map output = [:]
        ncube.getCell(input, output)
        assert output.initialize == true
        assert output.active == true
        assertFalse output.containsValue('defaultExecuted')
    }

    @Test
    void testNamedOrchestration()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleOrchestration.json')
        Map input = [rule: ['init', 'add1', 'add5', 'add10', 'add1']]
        Map output = [:]
        ncube.getCell(input, output)
        assert output.total == 12.3

        input = [rule: ['init', 'add1', 'add5', 'add10', 'add1', 'minus5'], state: 'OH']
        ncube.getCell(input, output)
        assert output.total == 12.0

        // Ignores add11
        input = [rule: ['init', 'add1', 'add1', 'add11', 'add1'], state: 'OH']
        ncube.getCell(input, output)
        assert output.total == 3.0

        // Match none (and no default, so blow up)
        try
        {
            input = [rule: ['initd', 'add2'], state: 'OH']
            ncube.getCell(input, output)
            fail()
        }
        catch (CoordinateNotFoundException ignored)
        {
        }

        // run rules backward (except init)
        input = [rule: ['init', 'add100', 'add10', 'add1'], state: 'OH']
        ncube.getCell(input, output)
        assert output.total == 111.0
    }

    @Test
    void testNamedOrchestrationWithDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleOrchestration.json')
        ncube.addColumn('rule', null)
        Map input = [rule: ['none-ya']]
        Map output = [total: 'qux']
        ncube.getCell(input, output)    // does not blow up because there is a no-op default column
        assert output.total == 'qux'     // no rules called so .total is never initialized
    }

    @Test
    void testOrchestrationMap()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleOrchestration.json')
        Map required = [foo: true]
        Map input = [rule: required]
        Map output = [:]
        ncube.getCell(input, output)
        assert output.total == 90.0

        input = [rule: required, state: 'OH']
        ncube.getCell(input, output)
        assert output.total == 90.0

        required = [foo: true, bar: 'baz']
        input = [rule: required, state: 'OH']
        ncube.getCell(input, output)
        assert output.total == 180.0    // skipped init (did not have 'bar/baz' key-value pair)

        try
        {
            output = [:]
            required = [foo: true, bar: 'baz']
            input = [rule: required, state: 'OH']
            ncube.getCell(input, output)
            fail()
        }
        catch (CommandCellException ignored)
        {
        }    // .total field never established on output

        output = [total: 10.0]
        required = [foo: true, bar: 'baz']
        input = [rule: required, state: 'OH']
        ncube.getCell(input, output)
        assert output.total == 100.0
    }

    @Test
    void testOrchestrationMapWithDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleOrchestration.json')
        ncube.addColumn('rule', null)
        Map required = [foo: false]
        Map input = [rule: required]
        Map output = [total: 'foo']
        ncube.getCell(input, output)
        assert output.total == 'foo'
    }

    @Test
    void testOrchestrationClosure()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleOrchestration.json')
        Map input = [state: 'OH', rule: { Axis axis ->
            List<Column> columns = []
            axis.columns.each { Column column ->
                String name = column.columnName
                if (name.startsWith('add'))
                {
                    columns.add(column)
                    columns.add(column)
                }
            }
            columns.add(axis.findColumnByName('minus10'))
            return columns
        }]
        Map output = [total: 0.0]
        ncube.getCell(input, output)
        assert output.total == 212.0
    }

    @Test
    void testOrchestrationClosureWithDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleOrchestration.json')
        ncube.addColumn('rule', null)
        Map input = [state: 'OH', rule: { Axis axis -> return [] }]
        Map output = [total: 'garply']
        ncube.getCell(input, output)
        assert output.total == 'garply'
    }

    @Test
    void testUseAndAtWithinRuleCube()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'useWithinRules.json')
        Map input = [businessDivisionCode:'USE']
        Map output = [:]
        def x = ncube.getCell(input, output)
        println x
        println "------- output -------"
        println output
    }

    @Test
    void testRuleNameDuringExecution()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'ruleNameDuringExecution.json')
        Map input = [attribute:'command']
        Map output = [:]
        ncube.getCell(input, output)
        assert output.out.toString() == '[1, 3]'
        assert output.desc.toString() == '[foo, baz]'
    }
}