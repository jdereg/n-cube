package com.cedarsoftware.ncube

import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import groovy.transform.CompileStatic

/**
 * This class contains information about the rule execution.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class RuleInfo extends CompactCILinkedMap<String, Object>
{
    // Convert Enums to String constants for performance gain
    public static final String RULES_EXECUTED = 'RULES_EXECUTED'
    public static final String RULE_STOP = 'RULE_STOP'
    public static final String SYSTEM_OUT = 'SYSTEM_OUT'
    public static final String SYSTEM_ERR = 'SYSTEM_ERR'
    public static final String ASSERTION_FAILURES = 'ASSERTION_FAILURES'
    public static final String LAST_EXECUTED_STATEMENT = 'LAST_EXECUTED_STATEMENT'
    public static final String AXIS_BINDINGS = 'AXIS_BINDINGS'
    public static final String INPUT_KEYS_USED = 'INPUT_KEYS_ACCESSED'
    public static final String UNBOUND_AXES_USED = 'UNBOUND_AXES_ACCESSED'

    RuleInfo()
    {
        // For speed, we are using the String (no function call - this code gets executed frequently)
        // Key = RuleMetaKeys.RULES_EXECUTED.name()
        put(RULES_EXECUTED, [])
    }

    /**
     * @return long indicating the number of conditions that fired (and therefore steps that executed).
     */
    long getNumberOfRulesExecuted()
    {
        return axisBindings.size()
    }

    /**
     * Set the indicator that a ruleStop was thrown
     */
    protected void ruleStopThrown()
    {
        put(RULE_STOP, Boolean.TRUE)
    }

    /**
     * @return true if a RuleStop was thrown during rule execution
     */
    boolean wasRuleStopThrown()
    {
        return containsKey(RULE_STOP) && Boolean.TRUE == get(RULE_STOP)
    }

    /**
     * @return String output of all println calls that occurred from the start of the initial getCell() call
     * until it returned.
     */
    String getSystemOut()
    {
        if (containsKey(SYSTEM_OUT))
        {
            return (String) get(SYSTEM_OUT)
        }
        return ''
    }

    void setSystemOut(String out)
    {
        put(SYSTEM_OUT, out)
    }

    /**
     * @return String output of system.err that occurred from the start of the initial getCell() call
     * until it returned.  Kept separately per thread.
     */
    String getSystemErr()
    {
        if (containsKey(SYSTEM_ERR))
        {
            return (String) get(SYSTEM_ERR)
        }
        return ''
    }

    void setSystemErr(String err)
    {
        put(SYSTEM_ERR, err)
    }

    /**
     * @return Set of String assert failures that occurred when runTest was called.
     */
    Set<String> getAssertionFailures()
    {
        if (containsKey(ASSERTION_FAILURES))
        {
            return (Set<String>) get(ASSERTION_FAILURES)
        }
        Set<String> failures = new CaseInsensitiveSet<>()
        put(ASSERTION_FAILURES, failures)
        return failures

    }

    void setAssertionFailures(Set<String> failures)
    {
        put(ASSERTION_FAILURES, failures)
    }

    /**
     * @return Object value of last executed statement value.
     */
    Object getLastExecutedStatementValue()
    {
        if (containsKey(LAST_EXECUTED_STATEMENT))
        {
            return get(LAST_EXECUTED_STATEMENT)
        }
        return null
    }

    protected void setLastExecutedStatement(Object value)
    {
        put(LAST_EXECUTED_STATEMENT, value)
    }

    /**
     * @return List of Binding instances which describe each Binding per getCell() call.  A binding
     * holds the axisNames and values that were supplied.
     */
    List<Binding> getAxisBindings()
    {
        if (containsKey(AXIS_BINDINGS))
        {
            return (List<Binding>)get(AXIS_BINDINGS)
        }
        List<Binding> bindings = []
        put(AXIS_BINDINGS, bindings)
        return bindings
    }

    /**
     * @return Set of input keys that were used (.get() or .containsKey() accessed) from the input Map.
     */
    Set getInputKeysUsed()
    {
        Set keysUsed = (Set)get(INPUT_KEYS_USED)
        if (keysUsed == null)
        {
            keysUsed = new CaseInsensitiveSet()
            put(INPUT_KEYS_USED, keysUsed)
        }
        return keysUsed
    }

    protected void addInputKeysUsed(Collection keys)
    {
        inputKeysUsed.addAll(keys)
    }

    /**
     * @return List of map entries where the key is a cube name and the value is
     * another map entry. The second map entry contains the name of an
     * unbound axis and the value that could not be bound.
     */
    List<MapEntry> getUnboundAxesList()
    {
        List<MapEntry> unBoundAxesList = get(UNBOUND_AXES_USED) as List<MapEntry>
        if (unBoundAxesList == null)
        {
            unBoundAxesList = []
            put(UNBOUND_AXES_USED, unBoundAxesList)
        }
        return unBoundAxesList
    }

    /**
     * @return Map of cube names containing a map keyed by axis name.
     * Each axis name map contains a list of column values that could
     * not be bound.
     */
    Map<String, Map<String, Set<Object>>> getUnboundAxesMap()
    {
        Map<String, Map<String, Set<Object>>> unBoundAxesMap = new CompactCILinkedMap<>()
        unboundAxesList.each { MapEntry entry ->
            String cubeName = entry.key
            MapEntry axisBinding = entry.value as MapEntry
            String axisName = axisBinding.key
            
            Map<String, Set<Object>> axisMap = unBoundAxesMap.get(cubeName)
            if (axisMap == null)
            {
                axisMap = new CompactCILinkedMap<>()
                unBoundAxesMap.put(cubeName, axisMap)
            }

            Set<Object> values = axisMap.get(axisName)
            if (values == null)
            {
                values = new LinkedHashSet<>()
                axisMap.put(axisName, values)
            }
            values << axisBinding.value
        }
        return unBoundAxesMap
    }

    protected void addUnboundAxis(String cubeName, String axisName, Object value)
    {
        unboundAxesList.add(new MapEntry(cubeName, new MapEntry(axisName, value)))
    }
}
