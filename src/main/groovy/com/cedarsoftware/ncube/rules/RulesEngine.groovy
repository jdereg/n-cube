package com.cedarsoftware.ncube.rules

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.GroovyExpression
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.util.ReflectionUtils
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.AxisType.RULE
import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.ncube.NCubeConstants.INPUT_VALUE
import static com.cedarsoftware.ncube.NCubeConstants.OUTPUT_VALUE
import static com.cedarsoftware.ncube.NCubeConstants._OR_
import static com.cedarsoftware.util.StringUtilities.hasContent

@Slf4j
@CompileStatic
class RulesEngine
{
    static final String AXIS_RULE = 'rules'
    static final String COL_CLASS = 'className'
    static final String COL_NCUBE = 'ncube'
    static final String COL_RULE_GROUP = 'ruleGroup'
    static final String COL_EXCEPTION = 'throwException'

    private static final Set IGNORED_METHODS = ['equals', 'toString', 'hashCode', 'annotationType'] as Set
    private static final Pattern PATTERN_METHOD_NAME = Pattern.compile(".*input\\.rule\\.((?:[^(]+))\\(.*")
    private static final Pattern PATTERN_NCUBE_NAME = Pattern.compile(".*'(rule.(?:[^']+))'.*", Pattern.DOTALL)
    protected volatile boolean verificationComplete = false
    private ConcurrentMap<String, Boolean> verifiedOrchestrations = new ConcurrentHashMap<>()

    private String name
    private ApplicationID appId
    private String rules
    private NCube ncubeRules

    RulesEngine(String name, ApplicationID appId, String rules)
    {
        this.name = name
        this.appId = appId
        this.rules = rules
    }

    /**
     * Name of the RuleEngine
     * @return String
     */
    String getName()
    {
        return name
    }

    /**
     * ApplicationID associated with the RuleEngine
     * @return ApplicationID
     */
    ApplicationID getAppId()
    {
        return appId
    }

    /**
     * Low level execution. Execute rules, in order, for a list of named groups on a given root object.
     * Each group will be executed in order. If any errors are recorded during execution for a given group, execution
     * will not proceed to the next group.
     * @param ruleGroups List<String>
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> executeGroups(List<String> ruleGroups, Object root, Map input = [:], Map output = [:])
    {
        // if you make a change here, also make a change in generateDocumentationForGroups()
        verifyNCubeSetup()
        if (ruleGroups == null)
        {
            throw new IllegalArgumentException("Rule groups to execute must not be null")
        }

        List<RulesError> errors = []
        if (ruleGroups.empty)
        {
            return errors
        }

        List<String> ruleGroupsForDecision = []
        ruleGroupsForDecision.add(_OR_)
        ruleGroupsForDecision.addAll(ruleGroups)

        Map<Comparable, Object> decision = (Map<Comparable, Object>) ncubeRules.decisionTable.getDecision([(COL_RULE_GROUP): ruleGroupsForDecision])
        Map<String, Map<String, Object>> ruleGroupIndex = [:]

        for (String key : decision.keySet())
        {
            Map<String, Object> row = (Map<String, Object>) decision[key]
            String ruleGroup = row[COL_RULE_GROUP]
            ruleGroupIndex[ruleGroup] = row
        }

        for (String ruleGroup : ruleGroups)
        {
            Map ruleInfo = ruleGroupIndex[ruleGroup]
            if (!ruleInfo)
            {
                log.info("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, rule group ${ruleGroup} is not defined.")
                continue
            }

            String className = ruleInfo[COL_CLASS]
            String ncubeName = ruleInfo[COL_NCUBE]
            Boolean throwException = ruleInfo[COL_EXCEPTION]
            if (!hasContent(className) || !hasContent(ncubeName))
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, rule group: ${ruleGroup} must have className and ncube name defined.")
            }

            BusinessRule rule = (BusinessRule) Class.forName(className).newInstance(root)
            rule.init(appId, input, output)

            NCube ncube = ncubeRuntime.getCube(appId, ncubeName)
            if (!ncube)
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube defined in ${COL_NCUBE} column of ${ncubeRules.name} does not exist.")
            }
            verifyOrchestration(ncube)
            ncube.getCell(input, output)
            errors.addAll(rule.errors)
            
            if (throwException && !errors.empty)
            {
                throw new RulesException(errors)
            }
        }
        return errors
    }

    /**
     * Execute rules for a named group on a given root object.
     * @param ruleGroups String
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> execute(String ruleGroup, Object root, Map input = [:], Map output = [:])
    {
        if (ruleGroup == null)
        {
            throw new IllegalArgumentException("Rule group must not be null.")
        }
        verifyNCubeSetup()
        return executeGroups([ruleGroup], root, input, output)
    }

    /**
     * Execute rules by defined by categories. Use the categories Map to define which categories apply for rule execution.
     * Example Map: [product: 'workerscompensation', type: 'validation']
     * The value for a given key can also be a List which will act like a logic OR for selection.
     * Example Map: [product: 'workerscompensation', type: ['composition', 'validation']]
     * @param categories Map see DecisionTable
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> execute(Map<String, Object> categories, Object root, Map input = [:], Map output = [:])
    {
        verifyNCubeSetup()
        List<String> ruleGroups = getRuleGroupsFromDecisionTable(categories)
        return executeGroups(ruleGroups, root, input, output)
    }

    /**
     * Execute rules by defined by categories. Use the categories List to define which categories apply for rule execution.
     * Similar to executeGroups() which takes a Map, but provides an additional way to specify multiple groups.
     * @param categories Iterable see DecisionTable
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> execute(Iterable<Map<String, Object>> iterable, Object root, Map input = [:], Map output = [:])
    {
        verifyNCubeSetup()
        List ruleGroups = getRuleGroupsFromDecisionTable(iterable)
        return executeGroups(ruleGroups, root, input, output)
    }

    /**
     * Low level generation. Generate a data structure that represents rule definitions for a List of rule groups.
     * @param ruleGroups List<String>
     * @return Map representing rule definitions
     */
    Map generateDocumentationForGroups(List<String> ruleGroups)
    {
        // if you make a change here, also make the same change in executeGroups()
        verifyNCubeSetup()
        if (ruleGroups == null)
        {
            throw new IllegalArgumentException("Rule groups for documentation must not be null.")
        }

        if (ruleGroups.empty)
        {
            return [:]
        }

        List<String> ruleGroupsForDecision = []
        ruleGroupsForDecision.add(_OR_)
        ruleGroupsForDecision.addAll(ruleGroups)

        Map<Comparable, ?> decision = ncubeRules.decisionTable.getDecision([(COL_RULE_GROUP): ruleGroupsForDecision])
        Map<String, Map<String, Object>> ruleGroupIndex = [:]

        for (String key : decision.keySet())
        {
            Map<String, Object> row = (Map<String, Object>) decision[key]
            String ruleGroup = row[COL_RULE_GROUP]
            ruleGroupIndex[ruleGroup] = row
        }

        Map info = [:]

        for (String ruleGroup : ruleGroups)
        {
            Map ruleInfo = ruleGroupIndex[ruleGroup]
            if (!ruleInfo)
            {
                log.info("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, rule group ${ruleGroup} is not defined.")
                continue
            }

            String className = ruleInfo[COL_CLASS]
            String ncubeName = ruleInfo[COL_NCUBE]
            if (!hasContent(className) || !hasContent(ncubeName))
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, rule group: ${ruleGroup} must have className and ncube name defined.")
            }
            Class ruleClass = Class.forName(className)

            Map typeMap = [(COL_CLASS): ruleClass.name] as Map

            Documentation documentation = (Documentation) ReflectionUtils.getClassAnnotation(ruleClass, Documentation)
            if (documentation)
            {
                typeMap['value'] = documentation.value()
            }

            Map map = [:]
            generateObjectDocumentation(map, ruleGroup, ruleClass, ncubeName)
            Map orderedMap = [:]
            map.reverseEach { orderedMap[it.key] = it.value }

            typeMap['objects'] = orderedMap
            info[ruleGroup] = typeMap
        }
        return info
    }

    /**
     * Generate a data structure that represents rule definitions for a single rule group
     * @param ruleGroup String
     * @return Map representing rule definitions
     */
    Map generateDocumentation(String ruleGroup)
    {
        if (ruleGroup == null)
        {
            throw new IllegalArgumentException("Rule group must not be null.")
        }
        verifyNCubeSetup()
        return generateDocumentationForGroups([ruleGroup])
    }

    /**
     * Generate a data structure that represents rule definitions from a Map
     * @param categories Map defining which rule groups to generate
     * @return Map representing rule definitions
     */
    Map generateDocumentation(Map<String, Object> categories)
    {
        verifyNCubeSetup()
        List<String> ruleGroups = getRuleGroupsFromDecisionTable(categories)
        return generateDocumentationForGroups(ruleGroups)
    }

    /**
     * Generate a data structure that represents rule definitions from a Map
     * @param categories Iterable defining which rule groups to generate
     * @return Map representing rule definitions
     */
    Map generateDocumentation(Iterable<Map<String, Object>> iterable)
    {
        verifyNCubeSetup()
        List ruleGroups = getRuleGroupsFromDecisionTable(iterable)
        generateDocumentationForGroups(ruleGroups)
    }

    /**
     * Generate data structure for UI component. The Map returned contains the following keys:
     *   groups: List of rule groups
     *   categories: Map with keys of categories and values of valid values for each category.
     * This data structure may be useful in places other than the UI.
     * @return Map
     */
    Map getInfo()
    {
        verifyNCubeSetup()
        Set<String> groups = ncubeRules.decisionTable.definedValues[COL_RULE_GROUP]
        Map<String, Set<String>> definedValues = ncubeRules.decisionTable.definedValues
        definedValues.remove(COL_RULE_GROUP)
        Map info = [groups: groups, categories: definedValues]
        return info
    }

    private void generateObjectDocumentation(Map map, String ruleGroup, Class rule, String ncubeName, List<String> visitedNCubes = [])
    {
        if (visitedNCubes.contains(ncubeName))
            return

        visitedNCubes.add(ncubeName)

        String entityName = ncubeName.split("rule.${ruleGroup}.".toString()).last()
        List methods = []
        NCube rulesNCube = ncubeRuntime.getCube(appId, ncubeName)
        Axis ruleAxis = rulesNCube.getAxis(AXIS_RULE)
        List<Column> columns = ruleAxis.columns
        for (Column column : columns)
        {
            // TODO - enhance this part of the code if rule orchestration gets scoped (for example, by clientName)
            String ruleName = column.metaProperties.name?: column.toString()
            GroovyExpression expression = (GroovyExpression) rulesNCube.getCellNoExecute([(AXIS_RULE): column.columnName])
            String conditionExprString = ((GroovyExpression) column.value)?.cmd
            String condition = conditionExprString ?: true // for Default column

            if (expression != null)
            {
                String cmd = expression.cmd
                if (cmd.startsWith('input.rule.runTargetRules('))
                {
                    traverseRules(map, ruleGroup, rule, cmd, visitedNCubes)
                    methods.add([name: ruleName, condition: condition, code: escapeCode(cmd)])
                }
                else if (cmd.startsWith('input.rule.'))
                {
                    // Expression references a rule
                    List<String> cmdLines = cmd.tokenize('\n')
                    Map methodInfo = generateMethodDocumentation(rule, cmdLines.first(), condition, ruleName)

                    if (cmdLines.size() > 1)
                    {
                        // If the expression does something after the rule method, include the code as well.
                        methodInfo['code'] = escapeCode(cmd)
                    }
                    methods.add(methodInfo)
                }
                else if (cmd.contains("rule.${ruleGroup}."))
                {
                    traverseRules(map, ruleGroup, rule, cmd, visitedNCubes)
                    methods.add([name: ruleName, condition: condition, code: escapeCode(cmd)])
                }
                else
                {
                    // Expression just contains code
                    methods.add([name: ruleName, condition: condition, code: escapeCode(cmd)])
                }
            }
            else
            {
                // Rule expression is empty
                methods.add([name: ruleName, condition: condition, noContent: true])
            }
        }
        if (methods)
        {
            map[entityName] = [rules: methods] as Map
        }
    }

    private void traverseRules(Map map, String ruleGroup, Class rule, String cmd, List<String> visitedNCubes = [])
    {
        // Expression contains code referencing another ruleGroup
        String ncubeNameNext = findStringAgainstPattern(PATTERN_NCUBE_NAME, escapeCode(cmd))
        generateObjectDocumentation(map, ruleGroup, rule, ncubeNameNext, visitedNCubes)
    }

    /**
     * Escape the < and > so that it can render in the HTML.
     * @param cmd
     * @return
     */
    private static String escapeCode(String cmd)
    {
        String escapedCmd = cmd.replaceAll('<', '&lt;').replaceAll('>', '&gt;')
        return escapedCmd
    }

    private Map generateMethodDocumentation(Class rule, String cmd, String condition, String ruleName)
    {
        String methodName = findStringAgainstPattern(PATTERN_METHOD_NAME, cmd)
        Map methodInfo = [name: ruleName, condition: condition, methodName: methodName] as Map
        Method method = ReflectionUtils.getNonOverloadedMethod(rule, methodName)
        if (!method)
        {
            throw new IllegalStateException("Method: ${methodName} does not exist on class: ${rule.name}")
        }
        Documentation documentation = (Documentation) ReflectionUtils.getMethodAnnotation(method, Documentation)
        if (documentation)
        {
            Method[] declaredMethods = documentation.class.declaredMethods
            for (Method declaredMethod : declaredMethods)
            {
                String declaredName = declaredMethod.name
                if (!IGNORED_METHODS.contains(declaredName))
                {
                    def value = documentation.invokeMethod(declaredName, null)
                    if (value)
                    {
                        if (declaredName == 'value')
                        {
                            methodInfo['documentation'] = value
                        } else if (declaredName == 'ncubes')
                        {
                            methodInfo['ncubes'] = value
                            addDefaultAppId(documentation, methodInfo)
                        }
                        else if (declaredName == 'appId')
                        {
                            methodInfo['appId'] = value
                        }
                    }
                }
            }
        }
        else
        {
            // If it calls a method and it doesn't have the annotation, return the code block.
            methodInfo['code'] = cmd
        }
        return methodInfo
    }

    /**
     * If the annotation did not include the appId, update the value in methodInfo with the default appId.
     * @param documentation
     * @param methodInfo
     */
    private void addDefaultAppId(Documentation documentation, Map methodInfo)
    {
        if (!hasContent(documentation.appId()))
        {
            methodInfo['appId'] = appId.toString()
        }
    }

    private static String findStringAgainstPattern(Pattern pattern, String cmd)
    {
        Matcher matcher = pattern.matcher(cmd)
        if (matcher.matches())
        {
            return matcher.group(1)
        }
        return ''
    }

    private List<String> getRuleGroupsFromDecisionTable(Map<String, Object> input)
    {
        Map<Comparable, ?> decision = ncubeRules.decisionTable.getDecision(input)
        return decision.values()[COL_RULE_GROUP]
    }

    private List<String> getRuleGroupsFromDecisionTable(Iterable<Map<String, Object>> iterable)
    {
        Map<Comparable, ?> decision = ncubeRules.decisionTable.getDecision(iterable)
        return decision.values()[COL_RULE_GROUP]
    }

    private void verifyNCubeSetup()
    {
        if (verificationComplete)
        {
            return
        }
        ncubeRules = ncubeRuntime.getCube(appId, rules)
        if (!ncubeRules)
        {
            throw new IllegalStateException("RulesEngine: ${name} requires an NCube named ${rules} in appId: ${this.appId}.")
        }
        String decisionAxisName = ncubeRules.decisionTable.decisionAxisName
        Axis decisionAxis = ncubeRules.getAxis(decisionAxisName)
        checkColumn(decisionAxis, COL_RULE_GROUP, true, true, true)
        checkColumn(decisionAxis, COL_CLASS, true, false, true)
        checkColumn(decisionAxis, COL_NCUBE, true, false, true)
        checkColumn(decisionAxis, COL_EXCEPTION, false, false, true)
        verificationComplete = true
    }

    private void checkColumn(Axis decisionAxis, String columnName, boolean required, boolean inputValue, boolean outputValue)
    {
        Column column = decisionAxis.findColumn(columnName)
        if (required && !column)
        {
            throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, Axis: ${decisionAxis.name} must have Column: ${columnName}.")
        }

        if (!required && !column)
        {
            return
        }

        if (inputValue && !column.metaProperties[INPUT_VALUE])
        {
            throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, Axis: ${decisionAxis.name}, Column: ${columnName} must have meta-property: ${INPUT_VALUE} set to true.")
        }

        if (outputValue && !column.metaProperties[OUTPUT_VALUE])
        {
            throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, Axis: ${decisionAxis.name}, Column: ${columnName} must have meta-property: ${OUTPUT_VALUE} set to true.")
        }
    }

    private void verifyOrchestration(NCube ncube)
    {
        if (verifiedOrchestrations[ncube.name])
        {
            return
        }

        Axis axis = ncube.getAxis(AXIS_RULE)
        if (axis)
        {
            if (axis.type != RULE)
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name}, Axis: ${axis.name} must be type ${RULE.name()}, but was ${axis.type}.")
            }
        }
        else
        {
            throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name} must have Axis: rules.")
        }

        verifiedOrchestrations[ncube.name] = true
    }
}
