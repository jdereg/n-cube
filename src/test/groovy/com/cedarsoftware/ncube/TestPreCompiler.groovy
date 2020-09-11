package com.cedarsoftware.ncube

import com.cedarsoftware.config.NCubeConfiguration
import com.cedarsoftware.ncube.rules.RulesConfiguration
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test
import org.springframework.beans.factory.annotation.Autowired

import static com.cedarsoftware.ncube.ApplicationID.testAppId
import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime

@CompileStatic
class TestPreCompiler extends NCubeCleanupBaseTest
{
    @Autowired private NCubeConfiguration nCubeConfiguration

    @Before
    void setupNCubes()
    {
        preloadCubes(testAppId,
        'rules/ncubes/lookup.something.json',
        'rules/ncubes/app.rules.json',
        'rules/ncubes/rule.group1.type1.object1.json',
        'rules/ncubes/rule.group1.type1.object2.json',
        'rules/ncubes/rule.group2.type1.object1.json',
        'rules/ncubes/rule.group1.type2.object1.json',
        'rules/ncubes/rule.group4.object1.json')
    }

    @Test
    void testPreCompile()
    {
        Map<String, Object> searchOptions = [(SEARCH_ACTIVE_RECORDS_ONLY): true, (SEARCH_INCLUDE_CUBE_DATA): true] as Map
        NCubeInfoDto dto = ncubeRuntime.search(testAppId, 'rule.group1.type1.object1', null, searchOptions).first()
        NCube ncube = NCube.createCubeFromBytes(dto.bytes)
        GroovyExpression cell = (GroovyExpression) ncube.getCellNoExecute([rules: 'Rule 1'])
        assert null == cell.runnableCode

        RulesConfiguration rulesConfiguration = new RulesConfiguration()
        rulesConfiguration.addRulesEngine('foo', testAppId, 'app.rules')
        PreCompiler preCompiler = new PreCompiler(rulesConfiguration, [])
        preCompiler.precompile()

        ncube = ncubeRuntime.getCube(testAppId, 'rule.group1.type1.object1')
        cell = (GroovyExpression) ncube.getCellNoExecute([rules: 'Rule 1'])
        assert null != cell.runnableCode
    }
}
