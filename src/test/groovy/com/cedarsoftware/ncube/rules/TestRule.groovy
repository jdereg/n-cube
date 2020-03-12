package com.cedarsoftware.ncube.rules

import groovy.transform.CompileStatic

@SuppressWarnings('unused')
@Documentation('Test Rule')
@CompileStatic
class TestRule extends BusinessRule
{
    Map foo

    TestRule(Map root)
    {
        super(root)
    }

    @Documentation(value = 'Rule 1 description', ncubes = 'lookup.something')
    void rule1()
    {
        root['rule1'] = true
        if (output['count'] instanceof Integer)
        {
            output['count'] = (Integer)output['count'] + 1
        }
    }

    @Documentation('Rule 2 description')
    void rule2()
    {
        root['rule2'] = true
    }

    @Documentation(value = 'Rule 3 description', ncubes = 'lookup.something', appId = 'NONE/DEFAULT_APP/999.99.9/SNAPSHOT/TEST')
    void rule3()
    {
        root['rule3'] = true
    }

    void rule4()
    {
        root['rule4'] = true
        addError('category', 'code', 'message')
    }

    void rule10()
    {
        root['rule10'] = true
        addError('category', 'code', 'message')
    }

}
