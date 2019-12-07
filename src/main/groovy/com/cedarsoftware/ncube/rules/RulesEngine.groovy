package com.cedarsoftware.ncube.rules

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.GroovyExpression
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.util.ReflectionUtils
import com.google.common.base.Splitter
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.AxisType.DISCRETE
import static com.cedarsoftware.ncube.AxisType.RULE
import static com.cedarsoftware.ncube.AxisValueType.CISTRING
import static com.cedarsoftware.ncube.AxisValueType.STRING
import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static com.cedarsoftware.util.StringUtilities.hasContent

@Slf4j
@CompileStatic
class RulesEngine
{
    static final String AXIS_RULE_GROUP = 'ruleGroup'
    static final String AXIS_CATEGORY = 'category'
    static final String AXIS_ATTRIBUTE = 'attribute'
    static final String AXIS_RULE = 'rule'
    static final String COL_CLASS = 'className'
    static final String COL_NCUBE = 'ncube'
    static final String COL_EXCEPTION = 'throwException'

    private static final Set IGNORED_METHODS = ['equals', 'toString', 'hashCode', 'annotationType'] as Set
    private static final Pattern PATTERN_METHOD_NAME = Pattern.compile(".*input.rule.((?:[^(]+))\\(.*")
    private static final Pattern PATTERN_NCUBE_NAME = Pattern.compile(".*'(rule.(?:[^']+))'.*", Pattern.DOTALL)
    protected volatile boolean verificationComplete = false
    private Set<String> columnsToReturn
    private ConcurrentMap<String, Boolean> verifiedOrchestrations = new ConcurrentHashMap<>()

    private String name
    private ApplicationID appId
    private String rules
    private String categories
    private NCube ncubeRules
    private NCube ncubeCategories

    RulesEngine(String name, ApplicationID appId, String rules, String categories = null)
    {
        this.name = name
        this.appId = appId
        this.rules = rules
        this.categories = categories
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
        verifyNCubeSetup()

        if (ruleGroups == null)
        {
            throw new IllegalArgumentException("Rule groups to execute must not be null")
        }

        List<RulesError> errors = []

        Axis ruleGroupAxis = ncubeRules.getAxis(AXIS_RULE_GROUP)
        for (String ruleGroup : ruleGroups)
        {
            // if you make a change here, also make a change in generateDocumentationForGroups
            if (!ruleGroupAxis.findColumn(ruleGroup))
            {
                log.info("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, rule group ${ruleGroup} is not defined.")
                continue
            }
            Map ruleInfo = ncubeRules.getMap([(AXIS_RULE_GROUP): ruleGroup, (AXIS_ATTRIBUTE): [] as Set])
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
        executeGroups([ruleGroup], root, input, output)
    }

    /**
     * Execute rules by defined by categories. Use the where Closure to define which categories apply for rule execution.
     * Example Closure: {Map input -> input['product'] == 'workerscompensation' && input['type'] == 'validation'}
     * @param where
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> execute(Closure where, Object root, Map input = [:], Map output = [:])
    {
        verifyNCubeSetup()
        if (!ncubeCategories)
        {
            throw new IllegalStateException("Categories ncube not setup in app: ${appId}.")
        }
        List<String> ruleGroups = getRuleGroupsFromClosure(where)
        executeGroups(ruleGroups, root, input, output)
    }

    /**
     * Execute rules by defined by categories. Use the categories Map to define which categories apply for rule execution.
     * Example Map: [product: 'workerscompensation', type: 'validation']
     * The value for a given key can also be a List which will act like a logic OR for selection.
     * Example Map: [product: 'workerscompensation', type: ['composition', 'validation']]
     * @param categories Map
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> execute(Map<String, Object> categories, Object root, Map input = [:], Map output = [:])
    {
        verifyNCubeSetup()
        if (!ncubeCategories)
        {
            throw new IllegalStateException("Categories ncube not setup in app: ${appId}.")
        }
        List<String> ruleGroups = getRuleGroupsFromMap(categories)
        executeGroups(ruleGroups, root, input, output)
    }

    /**
     * Execute rules by defined by categories. Use the categories List to define which categories apply for rule execution.
     * Similar to executeGroups() which takes a Map, but provides an additional way to specify multiple groups.
     * @param categories List
     * @param root Object
     * @param input Map (optional)
     * @param output Map (optional)
     * @return List<RulesError>
     * @throws RulesException if any errors are recorded during execution
     */
    List<RulesError> execute(List<Map<String, Object>> categoryList, Object root, Map input = [:], Map output = [:])
    {
        verifyNCubeSetup()
        if (!ncubeCategories)
        {
            throw new IllegalStateException("Categories ncube not setup in app: ${appId}.")
        }
        List ruleGroups = []
        for (Map categories : categoryList)
        {
            ruleGroups.addAll(getRuleGroupsFromMap(categories))
        }
        executeGroups(ruleGroups, root, input, output)
    }

    /**
     * Low level generation. Generate a data structure that represents rule definitions for a List of rule groups.
     * @param ruleGroups List<String>
     * @return Map representing rule definitions
     */
    Map generateDocumentationForGroups(List<String> ruleGroups)
    {
        verifyNCubeSetup()
        if (ruleGroups == null)
        {
            throw new IllegalArgumentException("Rule groups for documentation must not be null.")
        }

        Map info = [:]
        Axis ruleGroupAxis = ncubeRules.getAxis(AXIS_RULE_GROUP)
        for (String ruleGroup : ruleGroups)
        {
            // if you make a change here, also make the same change in executeGroups()
            if (!ruleGroupAxis.findColumn(ruleGroup))
            {
                log.info("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncubeRules.name}, rule group ${ruleGroup} is not defined.")
                continue
            }
            Map ruleInfo = ncubeRules.getMap([(AXIS_RULE_GROUP): ruleGroup, (AXIS_ATTRIBUTE): [] as Set])
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
     * Generate a data structure that represents rule definitions from a Closure
     * @param where Closure defining which rule groups to generate
     * @return Map representing rule definitions
     */
    Map generateDocumentation(Closure where)
    {
        verifyNCubeSetup()
        if (!ncubeCategories)
        {
            throw new IllegalStateException("Categories ncube not setup in app: ${appId}.")
        }
        List<String> ruleGroups = getRuleGroupsFromClosure(where)
        return generateDocumentationForGroups(ruleGroups)
    }

    /**
     * Generate a data structure that represents rule definitions from a Map
     * @param categories Map defining which rule groups to generate
     * @return Map representing rule definitions
     */
    Map generateDocumentation(Map<String, Object> categories)
    {
        verifyNCubeSetup()
        if (!ncubeCategories)
        {
            throw new IllegalStateException("Categories ncube not setup in app: ${appId}.")
        }
        List<String> ruleGroups = getRuleGroupsFromMap(categories)
        return generateDocumentationForGroups(ruleGroups)
    }

    /**
     * Generate a data structure that represents rule definitions from a Map
     * @param categories List defining which rule groups to generate
     * @return Map representing rule definitions
     */
    Map generateDocumentation(List<Map<String, Object>> categoryList)
    {
        verifyNCubeSetup()
        if (!ncubeCategories)
        {
            throw new IllegalStateException("Categories ncube not setup in app: ${appId}.")
        }
        List ruleGroups = []
        for (Map categories : categoryList)
        {
            ruleGroups.addAll(getRuleGroupsFromMap(categories))
        }
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

        List<String> groups = []
        List<Column> columns = ncubeRules.getAxis(AXIS_RULE_GROUP).columnsWithoutDefault
        for (Column column : columns)
        {
            groups.add((String) column.value)
        }

        Map<String, Set> categories = [:]
        if (ncubeCategories)
        {
            List<Column> categoryGroups = ncubeCategories.getAxis(AXIS_RULE_GROUP).columnsWithoutDefault
            List<Column> categoryColumns = ncubeCategories.getAxis(AXIS_CATEGORY).columnsWithoutDefault
            for (Column column : categoryColumns)
            {
                String categoryName = (String) column.value
                Set values = new LinkedHashSet<>()
                for (Column group : categoryGroups)
                {
                    String groupName = (String) group.value
                    Object value = ncubeCategories.getCellNoExecute([(AXIS_RULE_GROUP): groupName, (AXIS_CATEGORY): categoryName])
                    values.add(value)
                }
                categories[categoryName] = values
            }
        }

        Map info = [groups: groups, categories: categories]
        return info
    }

    private void generateObjectDocumentation(Map map, String ruleGroup, Class rule, String ncubeName)
    {
        String entityName = Splitter.on('.').split(ncubeName).last()
        List methods = []
        NCube rulesNCube = ncubeRuntime.getCube(appId, ncubeName)
        Axis ruleAxis = rulesNCube.getAxis('rule')
        List<Column> columns = ruleAxis.columns
        for (Column column : columns)
        {
            // TODO - enhance this part of the code if rule orchestration gets scoped (for example, by clientName)
            GroovyExpression expression = (GroovyExpression) rulesNCube.getCellNoExecute([rule: column.columnName])
            String cmd = expression.cmd
            String condition = ((GroovyExpression) column.value).cmd

            if (cmd.startsWith('input.rule.'))
            {
                Map methodInfo = generateMethodDocumentation(rule, cmd, condition)
                methods.add(methodInfo)
            }
            else if (cmd.contains("rule.${ruleGroup}."))
            {
                String ncubeNameNext = findStringAgainstPattern(PATTERN_NCUBE_NAME, cmd)
                generateObjectDocumentation(map, ruleGroup, rule, ncubeNameNext)
            }
            else
            {
                methods.add([value: cmd, condition: condition])
            }
        }
        if (methods)
        {
            map[entityName] = [rules: methods] as Map
        }
    }

    private Map generateMethodDocumentation(Class rule, String cmd, String condition)
    {
        String methodName = findStringAgainstPattern(PATTERN_METHOD_NAME, cmd)
        Map methodInfo = [methodName: methodName, condition: condition] as Map
        Method method = getMethod(rule, methodName)
        Documentation documentation = (Documentation) ReflectionUtils.getMethodAnnotation(method, Documentation)
        if (!documentation)
        {
            methodInfo['value'] = 'No documentation provided'
        }
        else
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
                        methodInfo[declaredName] = value
                        if ('ncubes' == declaredName)
                        {
                            addDefaultAppId(documentation, methodInfo)
                        }
                    }
                }
            }
        }
        return methodInfo
    }

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

    private static Method getMethod(Class clazz, String name)
    {
        Method[] methods = clazz.methods
        for (Method method : methods)
        {
            if (method.name == name)
            {
                return method
            }
        }
        throw new IllegalStateException("Method: ${name} does not exist on class: ${clazz.name}")
    }

    private List<String> getRuleGroupsFromClosure(Closure where)
    {
        Map options = [(NCube.MAP_REDUCE_COLUMNS_TO_RETURN): [] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): columnsToReturn]
        Map result = ncubeCategories.mapReduce(AXIS_CATEGORY, where, options)
        List<String> ruleGroups = new ArrayList<>(result.keySet())
        return ruleGroups
    }

    private List<String> getRuleGroupsFromMap(Map<String, Object> categories)
    {
        List ruleGroups = []
        List<Column> columns = ncubeCategories.getAxis(AXIS_RULE_GROUP).columnsWithoutDefault
        for (Column column : columns)
        {
            boolean matches = true
            String ruleGroup = column.value
            Map definedCategories = ncubeCategories.getMap([(AXIS_RULE_GROUP): ruleGroup, (AXIS_CATEGORY): [] as Set])
            for (String tag : categories.keySet())
            {
                Object value = categories[tag]
                if (value instanceof Collection)
                {
                    if (!value.contains(definedCategories[tag]))
                    {
                        matches = false
                        break
                    }
                }
                else if (definedCategories[tag] != value)
                {
                    matches = false
                    break
                }
            }
            if (matches)
            {
                ruleGroups.add(ruleGroup)
            }
        }
        return ruleGroups
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
        verifyRulesSetup()

        if (hasContent(categories))
        {
            ncubeCategories = ncubeRuntime.getCube(appId, categories)
            if (!ncubeCategories)
            {
                throw new IllegalStateException("RulesEngine: ${name} requires an NCube named ${categories} in appId: ${this.appId}.")
            }
            verifyCategoriesSetup()
            columnsToReturn = [(String) ncubeCategories.getAxis(AXIS_CATEGORY).columns.first().value] as Set
        }
        verificationComplete = true
    }

    private void verifyRulesSetup()
    {
        checkAxis(ncubeRules, AXIS_RULE_GROUP)
        checkAxis(ncubeRules, AXIS_ATTRIBUTE)
        checkColumns(ncubeRules, AXIS_ATTRIBUTE, [COL_CLASS, COL_NCUBE])

    }

    private void verifyCategoriesSetup()
    {
        if (ncubeCategories)
        {
            checkAxis(ncubeCategories, AXIS_RULE_GROUP)
            checkAxis(ncubeCategories, AXIS_CATEGORY)
        }
    }

    private void checkAxis(NCube ncube, String axisName)
    {
        Axis axis = ncube.getAxis(axisName)
        if (axis)
        {
            if (axis.type != DISCRETE)
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name}, Axis: ${axisName} must be type ${DISCRETE.name()}, but was ${axis.type}.")
            }
            if (axis.valueType != STRING && axis.valueType != CISTRING)
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name}, Axis: ${axisName} must be value type ${STRING.name()} or ${CISTRING.name()}, but was ${axis.valueType}.")
            }
        }
        else
        {
            throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name} must have Axis: ${axisName}.")
        }
    }

    private void checkColumns(NCube ncube, String axisName, List<String> columnNames)
    {
        Axis axis = ncube.getAxis(axisName)
        for (String columnName : columnNames)
        {
            if (!axis.findColumn(columnName))
            {
                throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name}, Axis: ${axis.name} must have Column: ${columnName}.")
            }
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
            throw new IllegalStateException("RulesEngine: ${name}, AppId: ${appId}, NCube: ${ncube.name} must have Axis: rule.")
        }

        verifiedOrchestrations[ncube.name] = true
    }
}
