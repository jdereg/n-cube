package com.cedarsoftware.ncube.rules.examples;

import com.cedarsoftware.ncube.rules.RulesConfiguration;
import com.cedarsoftware.ncube.rules.RulesEngine;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
@Component
public class Demo
{
    private RulesEngine policyRulesEngine;

    Demo(RulesConfiguration rulesConfiguration)
    {
        policyRulesEngine = rulesConfiguration.getRulesEngine("PolicyEngine");
    }

    public void runSingleRuleGroup(Map policy)
    {
        policyRulesEngine.execute("TransformationRules", policy);
    }

    public void runMultipleRuleGroups(Map policy)
    {
        List<String> ruleGroups = new ArrayList<>();
        ruleGroups.add("TransformationRules");
        ruleGroups.add("ValidationRules");
        policyRulesEngine.executeGroups(ruleGroups, policy);
    }

    public void runRulesSelectedByMap(Map policy)
    {
        Map<String, Object> categories = new LinkedHashMap<>();
        categories.put("mutable", true);
        policyRulesEngine.execute(categories, policy);
    }

    public void runRulesSelectedByListOfMaps(Map policy)
    {
        List<Map<String, Object>> categories = new ArrayList<>();
        Map<String, Object> category1 = new LinkedHashMap<>();
        category1.put("color", "red");
        categories.add(category1);
        Map<String, Object> category2 = new LinkedHashMap<>();
        category2.put("direction", "north");
        categories.add(category2);
        policyRulesEngine.execute(categories, policy);
    }
}
