package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.*
import com.cedarsoftware.util.CaseInsensitiveMap
import groovy.transform.CompileStatic
import org.junit.Test

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime

@CompileStatic
class RpmVisualizerTest extends VisualizerBaseTest implements RpmVisualizerConstants, VisualizerTestConstants
{
    static final String DETAILS_LABEL_UTILIZED_SCOPE_WITH_TRAITS = 'Utilized scope with traits'
    static final String DETAILS_LABEL_UTILIZED_SCOPE_WITHOUT_TRAITS = 'Utilized scope with no traits'
    static final String DETAILS_LABEL_FIELDS = 'Fields</b>'
    static final String DETAILS_LABEL_FIELDS_AND_TRAITS = 'Fields and traits'
    static final String DETAILS_LABEL_CLASS_TRAITS = 'Class traits'
    static final String VALID_VALUES_FOR_FIELD_SENTENCE_CASE = 'Valid values for field '
    static final String VALID_VALUES_FOR_FIELD_LOWER_CASE = 'valid values for field '

    static final Map DEFAULT_SCOPE = [_effectiveVersion: TEST_APP_VERSION,
                                      policyControlDate: DEFAULT_SCOPE_DATE,
                                      quoteDate        : DEFAULT_SCOPE_DATE] as CaseInsensitiveMap

    protected static List<NCube> libraryCubes = []

    @Test
    void testLoadGraph_checkVisInfo()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION, coverage: 'FCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        //Check visInfo
        assert 5 == visInfo.nodes.size()
        assert 4 == visInfo.edges.size()
        assert 4l == visInfo.maxLevel
        assert 4l == visInfo.edgeIdCounter
        assert 5l == visInfo.nodeIdCounter
        assert 4l == visInfo.defaultLevel
        assert 1l == visInfo.selectedNodeId
        assert '_ENUM' == visInfo.groupSuffix
        assert 'class' == visInfo.nodeLabel
        assert 'traits' == visInfo.cellValuesLabel

        Map allGroups =  [PRODUCT: 'Product', FORM: 'Form', RISK: 'Risk', COVERAGE: 'Coverage', CONTAINER: 'Container', DEDUCTIBLE: 'Deductible', LIMIT: 'Limit', RATE: 'Rate', RATEFACTOR: 'Rate Factor', PREMIUM: 'Premium', PARTY: 'Party', PLACE: 'Place', ROLE: 'Role', ROLEPLAYER: 'Role Player', UNSPECIFIED: 'Unspecified']
        assert allGroups == visInfo.allGroups
        assert allGroups.keySet() == visInfo.allGroupsKeys
        assert ['COVERAGE', 'RISK'] as Set == visInfo.availableGroupsAllLevels

        //Spot check typesToAddMap
        assert ['Coverage', 'Deductible', 'Limit', 'Premium', 'Rate', 'Ratefactor', 'Role'] == visInfo.typesToAddMap['Coverage']

        //Spot check the network overrides
        assert (visInfo.networkOverridesBasic.groups as Map).keySet().containsAll(allGroups.keySet())
        assert true == ((visInfo.networkOverridesFull.nodes as Map).shadow as Map).enabled
    }

    @Test
    void testLoadGraph_canLoadTargetAsRpmClass()
    {
        Map utilizedScope = new CaseInsensitiveMap()
        Map availableScope = getAvailableScope([coverage: 'CCCoverage'])

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)
        Map node = nodes.values().find { Map node1 -> "${UNABLE_TO_LOAD}Location".toString() == node1.label }
        node = loadNodeDetails(node)
        checkNode('Location', 'Risk', UNABLE_TO_LOAD, 'Coverage points directly to Risk via field Location, but there is no risk named Location on Risk.', true)
        availableScope.Risk = 'Location'
        assert availableScope == node.availableScope
        assert utilizedScope == node.scope
    }

    @Test
    void testLoadGraph_checkNodeAndEdge_nonEPM()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map availableScope = new CaseInsensitiveMap(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.partyrole.LossPrevention'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)

        //Top level source node
        checkNode('partyrole.LossPrevention', 'partyrole.LossPrevention')
        assert null == node.fromFieldName
        assert 'UNSPECIFIED' == node.group
        assert '1' == node.level
        assert null == node.sourceCubeName
        assert null == node.sourceDescription
        assert null == node.typesToAdd
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert (node.details as String).contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>roleRefCode</li><li>Parties</li></ul></pre>")

        //Edge from top level node to enum
        Map edge = edges.values().find { Map edge -> 'partyrole.LossPrevention' == edge.fromName && 'partyrole.BasePartyRole.Parties' == edge.toName}
        assert 'Parties' == edge.fromFieldName
        assert '2' == edge.level
        assert 'Parties' == edge.label
        assert "Field Parties cardinality ${V_MIN_CARDINALITY}:${V_MAX_CARDINALITY}".toString() == edge.title

        //Enum node under top level node
        String nodeTitle = "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Parties on partyrole.LossPrevention"
        node = nodes.values().find {Map node1 ->  nodeTitle == node1.title}
        node = loadNodeDetails(node)
        checkEnumNode(nodeTitle, '', false)
        assert 'Parties' == node.fromFieldName
        assert 'PARTY_ENUM' == node.group
        assert '2' == node.level
        assert 'rpm.class.partyrole.LossPrevention' == node.sourceCubeName
        assert 'LossPrevention' == node.sourceDescription
        assert null == node.typesToAdd
        assert utilizedScope == node.scope
        availableScope.sourceFieldName = 'Parties'
        assert availableScope == node.availableScope
        assert (node.details as String).contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>party.MoreNamedInsured</li><li>party.ProfitCenter</li></ul></pre>")

        //Edge from enum to target node
        edge = edges.values().find { Map edge1 -> 'partyrole.BasePartyRole.Parties' == edge1.fromName && 'party.ProfitCenter' == edge1.toName}
        assert 'party.ProfitCenter' == edge.fromFieldName
        assert '3' == edge.level
        assert !edge.label
        assert 'Valid value party.ProfitCenter cardinality 0:1' == edge.title

        //Target node under enum
        String nodeName = 'party.ProfitCenter'
        node = nodes.values().find { Map node1 -> nodeName == node1.label }
        node = loadNodeDetails(node)
        checkNode(nodeName, nodeName)
        assert nodeName == node.fromFieldName
        assert 'PARTY' == node.group
        assert '3' == node.level
        assert 'rpm.enum.partyrole.BasePartyRole.Parties' == node.sourceCubeName
        assert 'partyrole.BasePartyRole.Parties' == node.sourceDescription
        assert  [] == node.typesToAdd
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert (node.details as String).contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>roleRefCode</li><li>fullName</li><li>fein</li></ul></pre>")
    }

    @Test
    void testLoadGraph_checkStructure()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'FCoverage',
                             risk             : 'WProductOps'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        assert nodes.size() == 5
        assert edges.size() == 4

        assert nodes.values().find { Map node -> 'FCoverage' == node.label}
        assert nodes.values().find { Map node -> 'ICoverage' == node.label}
        assert nodes.values().find { Map node -> 'CCCoverage' == node.label}
        assert nodes.values().find { Map node -> "${UNABLE_TO_LOAD}Location".toString() == node.label}
        assert nodes.values().find { Map node -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on FCoverage".toString() == node.title}

        assert edges.values().find { Map edge -> 'FCoverage' == edge.fromName && 'Coverage.Coverages' == edge.toName}
        assert edges.values().find { Map edge -> 'Coverage.Coverages' == edge.fromName && 'ICoverage' == edge.toName}
        assert edges.values().find { Map edge -> 'Coverage.Coverages' == edge.fromName && 'CCCoverage' == edge.toName}
        assert edges.values().find { Map edge -> 'CCCoverage' == edge.fromName && 'Location' == edge.toName}
    }

    @Test
    void testLoadGraph_checkStructure_nonEPM()
    {
        Map availableScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap

        //Load graph
        String startCubeName = 'rpm.class.partyrole.LossPrevention'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        assert nodes.size() == 4
        assert edges.size() == 3

        assert nodes.values().find { Map node ->'rpm.class.partyrole.LossPrevention' == node.cubeName}
        assert nodes.values().find { Map node ->'rpm.class.party.MoreNamedInsured' == node.cubeName}
        assert nodes.values().find { Map node ->'rpm.class.party.ProfitCenter' == node.cubeName}
        assert nodes.values().find { Map node ->'rpm.enum.partyrole.BasePartyRole.Parties' == node.cubeName}

        assert edges.values().find { Map edge ->'partyrole.BasePartyRole.Parties' == edge.fromName && 'party.ProfitCenter' == edge.toName}
        assert edges.values().find { Map edge ->'partyrole.BasePartyRole.Parties' == edge.fromName && 'party.MoreNamedInsured' == edge.toName}
        assert edges.values().find { Map edge ->'partyrole.LossPrevention' == edge.fromName && 'partyrole.BasePartyRole.Parties' == edge.toName}
    }

    @Test
    void testLoadGraph_checkNodeAndEdge()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'FCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        Map enumScope = new CaseInsensitiveMap(utilizedScope)
        enumScope.sourceFieldName = 'Coverages'

        Map availableEnumScope = new CaseInsensitiveMap(availableScope)
        availableEnumScope.sourceFieldName = 'Coverages'

        Map ccCoverageScope = new CaseInsensitiveMap(utilizedScope)
        ccCoverageScope.coverage = 'CCCoverage'

        Map availableCCCoverageScope = new CaseInsensitiveMap(availableScope)
        availableCCCoverageScope.putAll(ccCoverageScope)
        availableCCCoverageScope.sourceCoverage = 'FCoverage'
        availableCCCoverageScope.sourceFieldName = 'Coverages'

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)

        //Top level source node
        checkNode('FCoverage', 'Coverage')
        assert 'rpm.class.Coverage' == node.cubeName
        assert null == node.fromFieldName
        assert 'COVERAGE' == node.group
        assert '1' == node.level
        assert null == node.sourceCubeName
        assert null == node.sourceDescription
        assert ['Coverage', 'Deductible', 'Limit', 'Premium', 'Rate', 'Ratefactor', 'Role'] == node.typesToAdd
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert (node.details as String).contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>Coverages</li><li>Exposure</li><li>StatCode</li></ul></pre>")

        //Edge from top level node to enum
        Map edge = edges.values().find { Map edge -> 'FCoverage' == edge.fromName && 'Coverage.Coverages' == edge.toName}
        assert 'Coverages' == edge.fromFieldName
        assert '2' == edge.level
        assert 'Coverages' == edge.label
        assert "Field Coverages cardinality ${V_MIN_CARDINALITY}:${V_MAX_CARDINALITY}".toString() == edge.title

        //Enum node under top level node
        String nodeTitle = "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on FCoverage"
        node = nodes.values().find {Map node1 ->  nodeTitle == node1.title}
        node = loadNodeDetails(node)
        checkEnumNode(nodeTitle, '', false)
        assert 'rpm.enum.Coverage.Coverages' == node.cubeName
        assert 'Coverages' == node.fromFieldName
        assert 'COVERAGE_ENUM' == node.group
        assert '2' == node.level
        assert 'rpm.class.Coverage' == node.sourceCubeName
        assert 'FCoverage' == node.sourceDescription
        assert enumScope == node.scope
        assert availableEnumScope == node.availableScope
        assert null == node.typesToAdd
        assert (node.details as String).contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>CCCoverage</li><li>ICoverage</li></ul></pre>")

        //Edge from enum to target node
        edge = edges.values().find { Map edge1 -> 'Coverage.Coverages' == edge1.fromName && 'CCCoverage' == edge1.toName}
        assert 'CCCoverage' == edge.fromFieldName
        assert '3' == edge.level
        assert !edge.label
        assert "Valid value CCCoverage cardinality ${V_MIN_CARDINALITY}:${V_MAX_CARDINALITY}".toString() == edge.title

        //Target node of top level node
        String nodeName = 'CCCoverage'
        node = nodes.values().find { Map node1 -> nodeName == node1.label }
        node = loadNodeDetails(node)
        checkNode(nodeName, 'Coverage')
        assert 'rpm.class.Coverage' == node.cubeName
        assert nodeName == node.fromFieldName
        assert 'COVERAGE' == node.group
        assert '3' == node.level
        assert 'rpm.enum.Coverage.Coverages' == node.sourceCubeName
        assert 'field Coverages on FCoverage' == node.sourceDescription
        assert ['Coverage', 'Deductible', 'Limit', 'Premium', 'Rate', 'Ratefactor', 'Role'] == node.typesToAdd
        assert ccCoverageScope == node.scope
        assert availableCCCoverageScope == node.availableScope
        assert (node.details as String).contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>Exposure</li><li>Location</li><li>StatCode</li><li>field1</li><li>field2</li><li>field3</li><li>field4</li></ul></pre>")
    }

    @Test
    void testGetCellValues_classNode_show()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'CCCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        checkNode('CCCoverage', 'Coverage')
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user clicks Show Traits for the node. Optional scope prompts display.
        node.showCellValues = true
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('CCCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        String nodeDetails = node.details as String
        assert nodeDetails.contains("Exposure</b></li><pre><ul><li>r:declared: true</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("Location</b></li><pre><ul><li>r:declared: true</li><li>r:exists: true</li><li>r:rpmType: Risk</li><li>v:max: 1</li><li>v:min: 0</li></ul></pre><li><b>")
        assert nodeDetails.contains("StatCode</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: None</li><li>r:exists: true</li><li>r:extends: DataElementInventory[StatCode]</li><li>r:rpmType: string</li></ul></pre>")
        assert nodeDetails.contains("field1</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: DEI default for field1</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("field2</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: DEI default for field2</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("field3</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: DEI default for field3</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("field4</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: DEI default for field4</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre></ul></pre>")
        assert nodeDetails.contains("${DETAILS_LABEL_CLASS_TRAITS}</b><pre><ul><li>r:exists: true</li><li>r:name: CCCoverage</li><li>r:scopedName: CCCoverage</li></ul></pre><br><b>")
    }

    @Test
    void testGetCellValues_classNode_show_unboundAxes_changeToNonDefault()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'CCCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        checkNode('CCCoverage', 'Coverage')

        //Simulate that the user clicks Show Traits for the node
        node.showCellValues = true
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('CCCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        checkScopePromptTitle(node, 'businessDivisionCode', false, '  - rpm.scope.class.Coverage.traits.StatCode\n  - rpm.scope.class.Coverage.traits.field1And2\n  - rpm.scope.class.Coverage.traits.field4')
        checkScopePromptDropdown(node, 'businessDivisionCode', DEFAULT, ['AAADIV', 'BBBDIV', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE, true)
        checkScopePromptTitle(node, 'program', false, '  - rpm.scope.class.Coverage.traits.field1And2\n  - rpm.scope.class.Coverage.traits.field4')
        checkScopePromptDropdown(node, 'program', DEFAULT, ['program1', 'program2', 'program3', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE, true)
        checkScopePromptTitle(node, 'type', false, '  - rpm.scope.class.Coverage.traits.field1And2\n  - rpm.scope.class.Coverage.traits.field3CovC\n  - rpm.scope.class.Coverage.traits.field4')
        checkScopePromptDropdown(node, 'type', DEFAULT, ['type1', 'type2', 'type3', 'typeA', 'typeB', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user picks businessDivisionCode = AAADIV
        utilizedScope.businessDivisionCode = 'AAADIV'
        availableScope.businessDivisionCode = 'AAADIV'
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node.showingHidingCellValues = false
        node = loadScopeChange(node)
        checkNode('CCCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        checkScopePromptTitle(node, 'businessDivisionCode', false, '  - rpm.scope.class.Coverage.traits.StatCode\n  - rpm.scope.class.Coverage.traits.field1And2\n  - rpm.scope.class.Coverage.traits.field4')
        checkScopePromptDropdown(node, 'businessDivisionCode', 'AAADIV', ['AAADIV', 'BBBDIV', DEFAULT], [], '', true)
        checkScopePromptTitle(node, 'program', false, '  - rpm.scope.class.Coverage.traits.field1And2\n  - rpm.scope.class.Coverage.traits.field4')
        checkScopePromptDropdown(node, 'program', DEFAULT, ['program1', 'program2', 'program3', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE, true)
        checkScopePromptTitle(node, 'type', false, '  - rpm.scope.class.Coverage.traits.field1And2\n  - rpm.scope.class.Coverage.traits.field3CovC\n  - rpm.scope.class.Coverage.traits.field4')
        checkScopePromptDropdown(node, 'type', DEFAULT, ['type1', 'type2', 'type3', 'typeA', 'typeB', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        String nodeDetails = node.details as String
        assert nodeDetails.contains("Exposure</b></li><pre><ul><li>r:declared: true</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("Location</b></li><pre><ul><li>r:declared: true</li><li>r:exists: true</li><li>r:rpmType: Risk</li><li>v:max: 1</li><li>v:min: 0</li></ul></pre><li><b>")
        assert nodeDetails.contains("StatCode</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: 1133</li><li>r:exists: true</li><li>r:extends: DataElementInventory[StatCode]</li><li>r:rpmType: string</li></ul></pre>")
        assert nodeDetails.contains("field1</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: 1133</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("field2</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: 1133</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("field3</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: DEI default for field3</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre><li><b>")
        assert nodeDetails.contains("field4</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: 1133</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre></ul></pre>")
        assert nodeDetails.contains("${DETAILS_LABEL_CLASS_TRAITS}</b><pre><ul><li>r:exists: true</li><li>r:name: CCCoverage</li><li>r:scopedName: CCCoverage</li></ul></pre><br><b>")
    }

    @Test
    void testGetCellValues_classNode_show_URLs()
    {
        String httpsURL = 'https://mail.google.com'
        String fileURL = 'file:///C:/Users/bheekin/Desktop/honey%20badger%20thumbs%20up.jpg'
        String httpURL = 'http://www.google.com'
        String fileLink = """<a href="#" onclick='window.open("${fileURL}");return false;'>${fileURL}</a>"""
        String httpsLink = """<a href="#" onclick='window.open("${httpsURL}");return false;'>${httpsURL}</a>"""
        String httpLink = """<a href="#" onclick='window.open("${httpURL}");return false;'>${httpURL}</a>"""

        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'AdmCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        checkNode('AdmCoverage', 'Coverage')
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user clicks Show Traits for the node.
        //An optional scope prompt for business division code shows.
        node.showCellValues = true
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('AdmCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user picks businessDivisionCode = AAADIV
        utilizedScope.businessDivisionCode = 'AAADIV'
        availableScope.businessDivisionCode = 'AAADIV'
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node.showingHidingCellValues = false
        node = loadScopeChange(node)
        checkNode('AdmCoverage', 'Coverage', '', '', false, true)
        String nodeDetails = node.details as String
        checkScopePromptTitle(node, 'businessDivisionCode', false, 'rpm.scope.class.Coverage.traits.StatCode')
        checkScopePromptDropdown(node, 'businessDivisionCode', 'AAADIV', ['AAADIV', 'BBBDIV', DEFAULT], [], '', true)
        assert nodeDetails.contains("Exposure</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: ${fileLink}</li><li>r:exists: true</li><li>r:extends: DataElementInventory</li><li>r:rpmType: string</li></ul></pre>")
        assert nodeDetails.contains("Location</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: ${httpLink}</li><li>r:exists: true</li><li>r:rpmType: Risk</li><li>v:max: 1</li><li>v:min: 0</li></ul></pre><li><b>")
        assert nodeDetails.contains("StatCode</b></li><pre><ul><li>r:declared: true</li><li>r:defaultValue: ${httpsLink}</li><li>r:exists: true</li><li>r:extends: DataElementInventory[StatCode]</li><li>r:rpmType: string</li></ul></pre></ul></pre>")
        assert nodeDetails.contains("${DETAILS_LABEL_CLASS_TRAITS}</b><pre><ul><li>r:exists: true</li><li>r:name: AdmCoverage</li><li>r:scopedName: AdmCoverage</li></ul></pre><br><b>")
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
    }

    @Test
    void testGetCellValues_enumNode_show()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'FCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)
        String enumNodeTitle = "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on FCoverage"
        Map enumNode = nodes.values().find {Map node1 ->  enumNodeTitle == node1.title}
        enumNode = loadNodeDetails(enumNode)
        checkEnumNode(enumNodeTitle)

        //Simulate that the user clicks Show Traits for the node
        enumNode.showCellValues = true
        enumNode.showingHidingCellValues = true
        enumNode = loadNodeDetails(enumNode)
        checkEnumNode(enumNodeTitle, '', false, true)
        utilizedScope.sourceFieldName = 'Coverages'
        assert utilizedScope == enumNode.scope
        availableScope.sourceFieldName = 'Coverages'
        assert availableScope == enumNode.availableScope
        String nodeDetails = enumNode.details as String
        assert nodeDetails.contains("${DETAILS_LABEL_FIELDS_AND_TRAITS}</b><pre><ul><li><b>CCCoverage</b></li><pre><ul><li>r:declared: true</li><li>r:exists: true</li><li>r:name: CCCoverage</li><li>v:max: 999999</li><li>v:min: 0</li></ul></pre><li><b>ICoverage</b></li><pre><ul><li>r:declared: true</li><li>r:exists: true</li><li>r:name: ICoverage</li><li>v:max: 1</li><li>v:min: 0</li></ul>")
        assert nodeDetails.contains("${DETAILS_LABEL_CLASS_TRAITS}</b><pre><ul><li>r:exists: true</li></ul></pre><br><b>")
    }

    @Test
    void testGetCellValues_classNode_hide()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'TCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        checkNode('TCoverage', 'Coverage')
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user clicks Show Traits for the node.
        //Required node scope prompt shows for points. No traits show yet.
        node.showCellValues = true
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('TCoverage', 'Coverage', '', ADDITIONAL_SCOPE_REQUIRED, true, true)
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', '', ['A', 'B', 'C'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user picks points = A in the node scope prompt.
        //Required node scope prompt shows for points. Traits now show
        utilizedScope.points = 'A'
        availableScope.points = 'A'
        node.showCellValues = true
        node.showingHidingCellValues = false
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node = loadScopeChange(node)
        checkNode('TCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', 'A', ['A', 'B', 'C'], [DEFAULT], '', true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user clicks Hide Traits for the node.
        //Required node scope prompt shows for points. No traits show.
        node.showCellValues = false
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('TCoverage', 'Coverage')
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', 'A', ['A', 'B', 'C'], [DEFAULT], '', true)
        utilizedScope.remove('points')
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
      }

    @Test
    void testLoadGraph_cubeNotFound()
    {
        NCube cube = ncubeRuntime.getCube(appId, 'rpm.enum.partyrole.BasePartyRole.Parties')
        try
        {
            //Change enum to have reference to non-existing cube
            cube.addColumn((AXIS_NAME), 'party.NoCubeExists')
            cube.setCell(true,[name:'party.NoCubeExists', trait: R_EXISTS])

            Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
            Map availableScope = getAvailableScope(utilizedScope)

            //Load graph
            String startCubeName = 'rpm.class.partyrole.LossPrevention'
            Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
            loadGraph(options, true)

            assert 1 == messages.size()
            assert 'No cube exists with name of rpm.class.party.NoCubeExists. Cube not included in the visualization.' == messages.first()
        }
        finally
        {
            //Reset cube
            cube.deleteColumn((AXIS_NAME), 'party.NoCubeExists')
            assert !cube.findColumn(AXIS_NAME, 'party.NoCubeExists')
        }
    }

    @Test
    void testLoadGraph_effectiveVersionApplied_beforeFieldAddAndObsolete()
    {
        Map utilizedScope = [_effectiveVersion: '1.0.0',
                             product          : 'WProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        String nodeDetails = node.details as String
        assert nodeDetails.contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>CurrentCommission</li><li>CurrentExposure</li><li>Risks</li><li>fieldObsolete101</li></ul></pre>")
    }

    @Test
    void testLoadGraph_effectiveVersionApplied_beforeFieldAddAfterFieldObsolete()
    {
        Map utilizedScope = [_effectiveVersion: '1.0.1',
                             product          : 'WProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        String nodeDetails = node.details as String
        assert nodeDetails.contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>CurrentCommission</li><li>CurrentExposure</li><li>Risks</li></ul></pre>")
    }

    @Test
    void testLoadGraph_effectiveVersionApplied_afterFieldAddAndObsolete()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          : 'WProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        String nodeDetails = node.details as String
        assert nodeDetails.contains("${DETAILS_LABEL_FIELDS}<pre><ul><li>CurrentCommission</li><li>CurrentExposure</li><li>Risks</li><li>fieldAdded102</li></ul></pre>")
    }

    @Test
    void testLoadGraph_validRpmClass()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        checkValidRpmClass( startCubeName)
    }

    @Test
    void testLoadGraph_validRpmClass_notStartWithRpmClass()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.klutz.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options, true)

        assert 1 == messages.size()
        String message = messages.first()
        assert "Starting cube for visualization must begin with 'rpm.class', ${startCubeName} does not.".toString() == message
    }

    @Test
    void testLoadGraph_validRpmClass_startCubeNotFound()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'ValidRpmClass'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options, true)

        assert 1 == messages.size()
        String message = messages.first()
        assert "No cube exists with name of ${startCubeName} for application id ${appId.toString()}".toString() == message
    }

    @Test
    void testLoadGraph_validRpmClass_noTraitAxis()
    {
        String startCubeName = 'rpm.class.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        NCube cube = ncubeRuntime.getCube(appId, startCubeName)
        cube.deleteAxis(AXIS_TRAIT)

        Map scope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map options = [startCubeName: startCubeName, scope: scope]

        loadGraph(options, true)
        assert 1 == messages.size()
        String message = messages.first()
        assert "Cube ${startCubeName} is not a valid rpm class since it does not have both a field axis and a traits axis.".toString() == message
    }

    @Test
    void testLoadGraph_validRpmClass_noFieldAxis()
    {
        String startCubeName = 'rpm.class.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        NCube cube = ncubeRuntime.getCube(appId, startCubeName)
        cube.deleteAxis(AXIS_FIELD)

        Map scope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map options = [startCubeName: startCubeName, scope: scope]

        loadGraph(options, true)
        assert 1 == messages.size()
        String message = messages.first()
        assert "Cube ${startCubeName} is not a valid rpm class since it does not have both a field axis and a traits axis.".toString() == message
    }

    @Test
    void testLoadGraph_validRpmClass_noCLASSTRAITSField()
    {
        String startCubeName = 'rpm.class.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        NCube cube = ncubeRuntime.getCube(appId, startCubeName)
        cube.getAxis(AXIS_FIELD).columns.remove(CLASS_TRAITS)

        Map scope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map options = [startCubeName: startCubeName, scope: scope]

        loadGraph(options)
        checkValidRpmClass(startCubeName)
    }

    @Test
    void testLoadGraph_validRpmClass_noRExistsTrait()
    {
        String startCubeName = 'rpm.class.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        NCube cube = ncubeRuntime.getCube(appId, startCubeName)
        cube.getAxis(AXIS_TRAIT).columns.remove(R_EXISTS)

        Map scope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map options = [startCubeName: startCubeName, scope: scope]

        loadGraph(options)
        checkValidRpmClass(startCubeName)
    }

    @Test
    void testLoadGraph_validRpmClass_noRRpmTypeTrait()
    {
        String startCubeName = 'rpm.class.ValidRpmClass'
        createNCubeWithValidRpmClass(startCubeName)
        NCube cube = ncubeRuntime.getCube(appId, startCubeName)
        cube.getAxis(AXIS_TRAIT).columns.remove(R_RPM_TYPE)

        Map scope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map options = [startCubeName: startCubeName, scope: scope]

        loadGraph(options)
        checkValidRpmClass( startCubeName)
    }

    @Test
    void testLoadGraph_invokedWithParentVisualizerInfoClass()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          : 'WProduct',
                             coverage         : 'FCoverage',
                             risk             : 'WProductOps'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)


        VisualizerInfo notRpmVisInfo = new VisualizerInfo()
        notRpmVisInfo.groupSuffix = 'shouldGetReset'

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, visInfo: notRpmVisInfo, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)

        assert 'RpmVisualizerInfo' == visInfo.class.simpleName
        assert '_ENUM' ==  visInfo.groupSuffix
        assert 'COVERAGE' == node.group
    }

    @Test
    void testLoadGraph_exceptionInMinimumTrait()
    {
        NCube cube = ncubeRuntime.getCube(appId, 'rpm.scope.class.Coverage.traits')
        Map coordinate = [(AXIS_FIELD): 'Exposure', (AXIS_TRAIT): R_EXISTS, coverage: 'FCoverage'] as Map

        try
        {
            //Change r:exists trait for FCoverage to throw an exception
            String expression = 'int a = 5; int b = 0; return a / b'
            cube.setCell(new GroovyExpression(expression, null, false), coordinate)

            Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                                 product          : 'WProduct'] as CaseInsensitiveMap
            Map availableScope = getAvailableScope(utilizedScope)

            //Load graph
            String startCubeName = 'rpm.class.Product'
            Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
            loadGraph(options)

            Map node = nodes.values().find { Map node1 -> "${UNABLE_TO_LOAD}FCoverage".toString() == node1.label }
            node = loadNodeDetails(node)
            checkNode('FCoverage', 'Coverage', UNABLE_TO_LOAD, 'Unable to load the class due to an exception.', true)
            String nodeDetails = node.details as String
            assert nodeDetails.contains(DETAILS_LABEL_MESSAGE)
            assert nodeDetails.contains(DETAILS_LABEL_ROOT_CAUSE)
            assert nodeDetails.contains('java.lang.ArithmeticException: Division by zero')
            assert nodeDetails.contains(DETAILS_LABEL_STACK_TRACE)
        }
        finally
        {
            //Reset cube
            cube.setCell(new GroovyExpression('true', null, false), coordinate)
        }
    }

    @Test
    void testLoadGraph_scopePrompt_nodeScope_afterTopNodeScopeChange()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          : 'AProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        Calendar cal = Calendar.getInstance()
        cal.add(Calendar.DATE, 1)
        String tomorrow = RpmVisualizerRelInfo.DATE_TIME_FORMAT.format(cal.getTime())

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert 8 == nodes.size()
        assert 7 == edges.size()
        assert 8 == nodes.values().findAll { Map node1 -> DEFAULT_SCOPE_DATE == (node1.availableScope as Map).policyControlDate }.size()

        //Simulate that the user changes policyControlDate on AProduct. The policy control date
        //should have changed on product and on all its target nodes.
        availableScope.policyControlDate = tomorrow
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node = loadScopeChange(node)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert 8 == nodes.size()
        assert 7 == edges.size()

        //Changed date for all
        assert 8 == nodes.values().findAll { Map node1 -> tomorrow == (node1.availableScope as Map).policyControlDate }.size()
    }

    @Test
    void testLoadGraph_scopePrompt_nodeScope_afterNonTopNodeScopeChange()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          : 'AProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        Calendar cal = Calendar.getInstance()
        cal.add(Calendar.DATE, 1)
        String tomorrow = RpmVisualizerRelInfo.DATE_TIME_FORMAT.format(cal.getTime())

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert 8 == nodes.size()
        assert 7 == edges.size()
        assert 8 == nodes.values().findAll { Map node1 -> DEFAULT_SCOPE_DATE == (node1.availableScope as Map).policyControlDate }.size()

        //Simulate that the user changes policyControlDate on for ARisk. The policy control date
        //should have changed on it and on all its target nodes, but no other classes.
        node = nodes.values().find { Map node1 -> 'ARisk' == node1.label }
        (node.availableScope as Map).policyControlDate = tomorrow
        loadScopeChange(node)
        assert 8 == nodes.size()
        assert 7 == edges.size()

        //Unchanged date
        node = nodes.values().find { Map node1 -> 'AProduct' == node1.label }
        assert DEFAULT_SCOPE_DATE == (node.availableScope as Map).policyControlDate
        node = nodes.values().find { Map node1 -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Risks on AProduct".toString() == node1.title }
        assert DEFAULT_SCOPE_DATE == (node.availableScope as Map).policyControlDate
        node = nodes.values().find { Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}BRisk".toString() == node1.label }
        assert DEFAULT_SCOPE_DATE == (node.availableScope as Map).policyControlDate

        //Changed date
        node = nodes.values().find { Map node1 -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on ARisk".toString() == node1.title }
        assert tomorrow == (node.availableScope as Map).policyControlDate
        node = nodes.values().find { Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}ACoverage".toString() == node1.label }
        assert tomorrow == (node.availableScope as Map).policyControlDate
        node = nodes.values().find { Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}BCoverage".toString() == node1.label }
        assert tomorrow == (node.availableScope as Map).policyControlDate
        node = nodes.values().find { Map node1 -> 'CCoverage' == node1.label }
        assert tomorrow == (node.availableScope as Map).policyControlDate
    }

    @Test
    void testLoadGraph_scopePrompt_noInitialScope()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
        Map availableScope = getAvailableScope()

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap()]
        Map node = loadGraph(options)
        checkNode('Product', 'Product', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true, false)
        checkScopePromptTitle(node, 'product', true, 'rpm.scope.class.Product.traits')
        checkScopePromptDropdown(node, 'product', '', ['AProduct', 'BProduct', 'GProduct', 'UProduct', 'WProduct'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert 1 == nodes.size()
        assert 0 == edges.size()

        //Simulate that the user picks product = AProduct in the node scope prompt.
        utilizedScope.product = 'AProduct'
        availableScope.product = 'AProduct'
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node = loadScopeChange(node)
        checkNode('AProduct', 'Product')
        checkScopePromptTitle(node, 'product', true, 'rpm.scope.class.Product.traits')
        checkScopePromptDropdown(node, 'product', 'AProduct', ['AProduct', 'BProduct', 'GProduct', 'UProduct', 'WProduct'], [DEFAULT], '',  true)
        checkDefaultScopePrompts(node)

        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert 8 == nodes.size()
        assert 7 == edges.size()
    }

    @Test
    void testLoadGraph_scopePrompt_nodes_afterScopeChange()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          : 'AProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
        assert 8 == nodes.size()
        assert 7 == edges.size()
        List<Map> preScopeChangeNodes = []
        nodes.values().each{
            preScopeChangeNodes << new LinkedHashMap(it)
        }

        //Risk.Coverages for ARisk
        node =  nodes.values().find {Map node1 -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on ARisk".toString() == node1.title}
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkNoScopePrompt(node, 'pgm')
        checkNoScopePrompt(node, 'state')
        checkNoScopePrompt(node, 'div')

        //ACoverage
        node =  nodes.values().find {Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}ACoverage".toString() == node1.label}
        node = loadNodeDetails(node)
        checkNode('ACoverage', 'Coverage', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true)
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'pgm', true, 'rpm.scope.class.Coverage.traits.fieldACoverage')
        checkScopePromptDropdown(node, 'pgm', '', ['pgm1', 'pgm2', 'pgm3'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        checkScopePromptTitle(node, 'div', true, 'rpm.scope.class.Coverage.traits.fieldACoverage')
        checkScopePromptDropdown(node, 'div', '', ['div1', 'div2'], ['div3', DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkNoScopePrompt(node, 'state')

        //BCoverage
        node = nodes.values().find { Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}${'BCoverage'}".toString() == node1.label }
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkNode('BCoverage', 'Coverage', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true)
        checkScopePromptTitle(node, 'div', true, 'rpm.scope.class.Coverage.traits.fieldBCoverage')
        checkScopePromptDropdown(node, 'div', '', ['div3'], ['div1', 'div2', DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkNoScopePrompt(node, 'pgm')
        checkNoScopePrompt(node, 'state')

        //CCoverage
        node = nodes.values().find { Map node1 -> 'CCoverage' == node1.label }
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkNode('CCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false)
        checkScopePromptTitle(node, 'state', false, 'rpm.scope.class.Coverage.traits.fieldCCoverage')
        checkScopePromptDropdown(node, 'state', DEFAULT, ['GA', 'IN', 'NY', DEFAULT], ['KY', 'OH'], DETAILS_CLASS_DEFAULT_VALUE )
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkNoScopePrompt(node, 'div')
        checkNoScopePrompt(node, 'pgm')

        //Simulate that the user picks state = OH on ARisk.
        node = nodes.values().find { Map node1 -> 'ARisk' == node1.label }
        (node.availableScope as Map).state = 'OH'
        loadScopeChange(node)

        //Simulate that the user picks div = div1 on ARisk
        node = nodes.values().find { Map node1 -> 'ARisk' == node1.label }
        (node.availableScope as Map).div = 'div1'
        loadScopeChange(node)
        assert 8 == nodes.size()
        assert 7 == edges.size()

        //Unchanged nodes
        assert nodes.values().find {Map node1 -> 'AProduct' == node1.label} ==
                preScopeChangeNodes.find {Map node1 -> 'AProduct' == node1.label}
        assert nodes.values().find {Map node1 -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Risks on AProduct".toString() == node1.title} ==
                preScopeChangeNodes.find {Map node1 -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Risks on AProduct".toString() == node1.title}
        assert nodes.values().find {Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}BRisk".toString() == node1.label} ==
                preScopeChangeNodes.find {Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}BRisk".toString() == node1.label}

        //Changed nodes

        //Risk.Coverages for ARisk
        node =  nodes.values().find {Map node1 -> "${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on ARisk".toString() == node1.title}
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkNoScopePrompt(node, 'pgm')
        checkScopePromptTitle(node, 'state', false, null, false, true)
        checkScopePromptDropdown(node, 'state', 'OH', null, null, '', false, false)
        checkScopePromptTitle(node, 'div', false, null, false, true)
        checkScopePromptDropdown(node, 'div', 'div1', null, null, '', false, false)

        //ACoverage
        node =  nodes.values().find {Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}ACoverage".toString() == node1.label}
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkNode('ACoverage', 'Coverage', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true)
        checkScopePromptTitle(node, 'pgm', true, 'rpm.scope.class.Coverage.traits.fieldACoverage')
        checkScopePromptDropdown(node, 'pgm', '', ['pgm1', 'pgm2', 'pgm3'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkScopePromptTitle(node, 'state', false, null, false, true)
        checkScopePromptDropdown(node, 'state', 'OH', null, null, '', false, false)
        checkScopePromptDropdown(node, 'div', 'div1', ['div1', 'div2'], ['div3', DEFAULT])

        //BCoverage
        node = nodes.values().find { Map node1 -> "${REQUIRED_SCOPE_VALUE_NOT_FOUND_FOR}${'BCoverage'}".toString() == node1.label }
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkNode('BCoverage', 'Coverage', REQUIRED_SCOPE_VALUE_NOT_FOUND_FOR, IS_NOT_VALID_FOR, true)
        checkScopePromptTitle(node, 'div', true, 'rpm.scope.class.Coverage.traits.fieldBCoverage')
        checkScopePromptDropdown(node, 'div', 'div1', ['div3'], ['div1', 'div2', DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkNoScopePrompt(node, 'pgm')
        checkScopePromptTitle(node, 'state', false, null, false, true)
        checkScopePromptDropdown(node, 'state', 'OH', null, null, '', false, false)

        //CCoverage
        node = nodes.values().find { Map node1 -> 'CCoverage' == node1.label }
        node = loadNodeDetails(node)
        checkDefaultScopePrompts(node)
        checkNode('CCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false)
        checkScopePromptTitle(node, 'state', false, 'rpm.scope.class.Coverage.traits.fieldCCoverage')
        checkScopePromptDropdown(node, 'state', 'OH', ['GA', 'IN', 'NY', DEFAULT], ['KY', 'OH'], DETAILS_CLASS_DEFAULT_VALUE)
        checkScopePromptDropdown(node, 'product', 'AProduct', null, null, '', false, true)
        checkScopePromptTitle(node, 'div', false, null, false, true)
        checkScopePromptDropdown(node, 'div', 'div1', null, null, '', false, false)
        checkNoScopePrompt(node, 'pgm')
     }

    @Test
    void testLoadGraph_scopePrompt_missingRequiredScope_nonEPM()
    {
        NCube cube = ncubeRuntime.getCube(appId, 'rpm.class.party.ProfitCenter')
        try
        {
            //Change cube to have declared required scope
            cube.setMetaProperty(REQUIRED_SCOPE, ['dummyRequiredScopeKey'])

            Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION] as CaseInsensitiveMap
            Map availableScope = new CaseInsensitiveMap(utilizedScope)

            //Load graph
            String startCubeName = 'rpm.class.partyrole.LossPrevention'
            Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
            loadGraph(options)
            Map node = nodes.values().find { Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}${'party.ProfitCenter'}".toString() == node1.label }
            node = loadNodeDetails(node)
            checkDefaultScopePrompts(node, false, false)
            checkNode('party.ProfitCenter', 'party.ProfitCenter', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true, false)
            checkScopePromptTitle(node, 'dummyRequiredScopeKey', true, 'rpm.class.party.ProfitCenter')
            checkScopePromptDropdown(node, 'dummyRequiredScopeKey', '', [], [], DETAILS_CLASS_MISSING_VALUE)
        }
        finally
        {
            //Reset cube
            cube.removeMetaProperty('requiredScopeKeys')
        }
    }

    @Test
    void testLoadGraph_scopePrompt_missingDeclaredRequiredScope()
    {
        NCube cube = ncubeRuntime.getCube(appId, 'rpm.class.Coverage')
        try
        {
            //Change cube to have declared required scope
            cube.setMetaProperty(REQUIRED_SCOPE, ['dummyRequiredScopeKey'])

            Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                                 risk             : 'WProductOps'] as CaseInsensitiveMap
            Map availableScope = getAvailableScope(utilizedScope)

            //Load graph
            String startCubeName = 'rpm.class.Risk'
            Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
            loadGraph(options)
            Map node = nodes.values().find { Map node1 -> "${ADDITIONAL_SCOPE_REQUIRED_FOR}${'CCCoverage'}".toString() == node1.label }
            node = loadNodeDetails(node)
            checkNode('CCCoverage', 'Coverage', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true, false)
            checkScopePromptTitle(node, 'dummyRequiredScopeKey', true, 'rpm.class.Coverage')
            checkScopePromptDropdown(node, 'dummyRequiredScopeKey', '', [], [], DETAILS_CLASS_MISSING_VALUE)
        }
        finally
        {
            //Reset cube
            cube.removeMetaProperty(REQUIRED_SCOPE)
        }
    }

    @Test
    void testGetCellValues_classNode_show_missingRequiredScope()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'TCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        checkNode('TCoverage', 'Coverage')
      
        //Simulate that the user clicks Show Traits for the node.
        //Required node scope prompt now shows for points, but not yet one for businessDivision code
        node.showCellValues = true
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('TCoverage', 'Coverage', '', ADDITIONAL_SCOPE_REQUIRED, true, true)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', '', ['A', 'B', 'C'], [DEFAULT],DETAILS_CLASS_MISSING_VALUE, true)
        checkNoScopePrompt(node, 'businessDivisionCode')
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user picks points = A in the node scope prompt.
        //Both required scope points and unbound node scope prompt businessDivisionCode are now showing.
        utilizedScope.points = 'A'
        availableScope.points = 'A'
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node.showingHidingCellValues = false
        node = loadScopeChange(node)
        checkNode('TCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', 'A', ['A', 'B', 'C'], [DEFAULT], '', true)
        checkScopePromptTitle(node, 'businessDivisionCode', false, 'rpm.scope.class.Coverage.traits.StatCode')
        checkScopePromptDropdown(node, 'businessDivisionCode', DEFAULT, ['AAADIV', 'BBBDIV', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE,  true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user picks businessDivisionCode = AAADIV in the node scope prompt.
        //Both scope prompts are still showing.
        utilizedScope.businessDivisionCode = 'AAADIV'
        availableScope.businessDivisionCode = 'AAADIV'
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node = loadScopeChange(node)
        checkNode('TCoverage', 'Coverage', '', DEFAULTS_WERE_USED, false, true)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', 'A', ['A', 'B', 'C'], [DEFAULT], '', true)
        checkScopePromptTitle(node, 'businessDivisionCode', false, 'rpm.scope.class.Coverage.traits.StatCode')
        checkScopePromptDropdown(node, 'businessDivisionCode', 'AAADIV', ['AAADIV', 'BBBDIV', DEFAULT], [], '', true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
    }

    @Test
    void testGetCellValues_classNode_show_invalidRequiredScope()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             coverage         : 'TCoverage'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)
        
        //Load graph
        String startCubeName = 'rpm.class.Coverage'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)
        checkNode('TCoverage', 'Coverage')
        checkNoScopePrompt(node, 'points')

        //Simulate that the user clicks Show Traits for the node.
        //Required node scope prompt now shows for points.
        node.showCellValues = true
        node.showingHidingCellValues = true
        node = loadNodeDetails(node)
        checkNode('TCoverage', 'Coverage', '', ADDITIONAL_SCOPE_REQUIRED, true, true)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', '', ['A', 'B', 'C'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE, true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope

        //Simulate that the user enters points = bogus in the node scope prompt.
        //Invalid node scope prompt should now show for points.
        availableScope.points = 'bogus'
        node.availableScope = new CaseInsensitiveMap(availableScope)
        node = loadScopeChange(node)
        checkNode('TCoverage', 'Coverage', REQUIRED_SCOPE_VALUE_NOT_FOUND_FOR, IS_NOT_VALID_FOR, true, true)
        checkScopePromptTitle(node, 'points', true, 'rpm.scope.class.Coverage.traits.fieldTCoverage')
        checkScopePromptDropdown(node, 'points', 'bogus', ['A', 'B', 'C'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE,  true)
        assert utilizedScope == node.scope
        assert availableScope == node.availableScope
    }


    @Test
    void testLoadGraph_scopePrompt_nodeWithSingleDefaultValue()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          :'BProduct'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Product'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        Map node = nodes.values().find { Map node1 -> 'DRisk' == node1.label }
        node = loadNodeDetails(node)
        checkNode('DRisk', 'Risk', '', DEFAULTS_WERE_USED, false)
        checkScopePromptTitle(node, 'state', false, 'rpm.scope.class.Risk.traits.fieldDRisk')
        checkScopePromptDropdown(node, 'state', DEFAULT, [DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE)
    }

    @Test
    void testLoadGraph_scopePrompt_enumWithMissingThenInvalidRequiredScope()
    {
        Map availableScope = getAvailableScope([risk: 'DRisk'])

        Map enumUtilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                                 risk             : 'DRisk',
                                 sourceFieldName  : 'Coverages'] as CaseInsensitiveMap
        Map enumAvailableScope = getAvailableScope(enumUtilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Risk'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        //The edge for field Coverages from DRisk to enum Risk.Coverages has missing scope label.
        Map edge = edges.values().find { Map edge -> 'DRisk' == edge.fromName && 'Risk.Coverages' == edge.toName}
        assert "${ADDITIONAL_SCOPE_REQUIRED_FOR}Coverages".toString() == edge.label
        assert "Field Coverages cardinality ${V_MIN_CARDINALITY}:${V_MAX_CARDINALITY}".toString() == edge.title

        //Risk.Coverages enum has one required prompt
        String enumNodeTitle = "${ADDITIONAL_SCOPE_REQUIRED_FOR}${VALID_VALUES_FOR_FIELD_LOWER_CASE}Coverages on DRisk"
        Map node = nodes.values().find {Map node1 ->  enumNodeTitle == node1.title}
        node = loadNodeDetails(node)
        checkEnumNode(enumNodeTitle, ADDITIONAL_SCOPE_REQUIRED, true)
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'pgm', true, 'rpm.scope.enum.Risk.Coverages.traits.exists')
        checkScopePromptDropdown(node, 'pgm', '', ['pgm1', 'pgm2', 'pgm3'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        assert enumUtilizedScope == node.scope
        assert enumAvailableScope == node.availableScope

        //Simulate that the user enters invalid pgm = pgm4 in the node scope prompt.
        enumAvailableScope.pgm = 'pgm4'
        node.availableScope = new CaseInsensitiveMap(enumAvailableScope)
        node = loadScopeChange(node)

        //The edge for field Coverages from DRisk to enum Risk.Coverages has invalid scope label.
        edge = edges.values().find { Map edge1 -> 'DRisk' == edge1.fromName && 'Risk.Coverages' == edge1.toName}
        assert "${REQUIRED_SCOPE_VALUE_NOT_FOUND_FOR}Coverages".toString() == edge.label
        assert "Field Coverages cardinality ${V_MIN_CARDINALITY}:${V_MAX_CARDINALITY}".toString() == edge.title

        //Risk.Coverages enum has one required prompt
        checkEnumNode("${REQUIRED_SCOPE_VALUE_NOT_FOUND_FOR}${VALID_VALUES_FOR_FIELD_LOWER_CASE}Coverages on DRisk", IS_NOT_VALID_FOR, true)
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'pgm', true, 'rpm.scope.enum.Risk.Coverages.traits.exists')
        checkScopePromptDropdown(node, 'pgm', 'pgm4', ['pgm1', 'pgm2', 'pgm3'], [DEFAULT], DETAILS_CLASS_MISSING_VALUE)
        enumUtilizedScope.pgm = 'pgm4'
        assert enumUtilizedScope == node.scope
        assert enumAvailableScope == node.availableScope

        //Simulate that the user enters valid pgm = pgm1 in the node scope prompt.
        enumAvailableScope.pgm = 'pgm1'
        node.availableScope = new CaseInsensitiveMap(enumAvailableScope)
        node = loadScopeChange(node)

        //The edge for field Coverages from DRisk to enum Risk.Coverages has no label.
        edge = edges.values().find { Map edge1 -> 'DRisk' == edge1.fromName && 'Risk.Coverages' == edge1.toName}
        assert 'Coverages' == edge.label
        assert "Field Coverages cardinality ${V_MIN_CARDINALITY}:${V_MAX_CARDINALITY}".toString() == edge.title

        //Risk.Coverages enum has one required prompt
        checkEnumNode("${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Coverages on DRisk")
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'pgm', true, 'rpm.scope.enum.Risk.Coverages.traits.exists')
        checkScopePromptDropdown(node, 'pgm', 'pgm1', ['pgm1', 'pgm2', 'pgm3'], [DEFAULT], '')
        enumUtilizedScope.pgm = 'pgm1'
        assert enumUtilizedScope == node.scope
        assert enumAvailableScope == node.availableScope
    }

    @Test
    void testLoadGraph_scopePrompt_derivedScopeKey_topNode()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             risk             : 'StateOps'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Risk'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        Map node = loadGraph(options)

        //Check that sourceRisk is part of node scope
        List<String> risks = ['ARisk', 'BRisk', 'DRisk', 'GProductOps', 'GStateOps', 'ProductLocation', 'StateOps', 'WProductOps']
        checkNode('StateOps', 'Risk', ADDITIONAL_SCOPE_REQUIRED_FOR, ADDITIONAL_SCOPE_REQUIRED, true)
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'sourceRisk', true, 'rpm.scope.class.Risk.traits.Risks')
        checkScopePromptDropdown(node, 'sourceRisk', '', risks, [DEFAULT], DETAILS_CLASS_MISSING_VALUE)
    }

    @Test
    void testLoadGraph_scopePrompt_derivedScopeKey_notTopNode()
    {
        Map utilizedScope = [_effectiveVersion: TEST_APP_VERSION,
                             product          : 'WProduct',
                             risk             : 'WProductOps'] as CaseInsensitiveMap
        Map availableScope = getAvailableScope(utilizedScope)

        //Load graph
        String startCubeName = 'rpm.class.Risk'
        Map options = [startCubeName: startCubeName, scope: new CaseInsensitiveMap(availableScope)]
        loadGraph(options)

        //Check sourceRisk cope prompt
        Map node = nodes.values().find { Map node1 -> 'StateOps' == node1.label }
        node = loadNodeDetails(node)
        checkNode('StateOps', 'Risk')
        checkDefaultScopePrompts(node)
        checkScopePromptTitle(node, 'sourceRisk', false, null, true)
        checkScopePromptDropdown(node, 'sourceRisk', 'WProductOps', null, null, '', false, true)
    }

    /* TODO: Will revisit providing "in scope" available scope values for r:exists at a later time.
   @Test
   void testLoadGraph_inScopeScopeValues_unboundAxis()
   {
       //Load graph with no scope
       String startCubeName = 'rpm.class.Product'
       topNodeScope = new CaseInsensitiveMap()
       Map options = [startCubeName: startCubeName, scope: topNodeScope]
       Map node = loadGraph(options)
       assert 1 == nodes.size()

       //User picks GProduct. Reload. This will result in unboundAxis on div.
       topNodeScope.product = 'GProduct'
       options = [startCubeName: startCubeName, scope: topNodeScope, visInfo: visInfo]
       Map node = loadGraph(options)
       assert 2 == nodes.size()

       //Check node scope prompt
       Map node = checkEnumNodeBasics("${VALID_VALUES_FOR_FIELD_SENTENCE_CASE}Risks on GProduct")
       checkScopePromptDropdown(node, 'div', DEFAULT, ['div1', 'div2', DEFAULT], [], DETAILS_CLASS_DEFAULT_VALUE, )
   }

   @Test
   void testLoadGraph_inScopeScopeValues_invalidCoordinate()
   {
       //Load graph with no scope
       String startCubeName = 'rpm.class.Product'
       topNodeScope = new CaseInsensitiveMap()
       Map options = [startCubeName: startCubeName, scope: topNodeScope]
       Map node = loadGraph(options)
       assert 1 == nodes.size()

       //User picks GProduct. Reload.
       topNodeScope.product = 'GProduct'
       options = [startCubeName: startCubeName, scope: topNodeScope, visInfo: visInfo]
       Map node = loadGraph(options)
       assert 2 == nodes.size()

       //User picks div = div1. Reload. This will result in InvalidCoordinateException due to missing category scope.
       topNodeScope.div = 'div1'
       options = [startCubeName: startCubeName, scope: topNodeScope, visInfo: visInfo]
       Map node = loadGraph(options)
       assert 2 == nodes.size()

       //Check graph scope prompt
       assert 5 == scopeInfo.optionalGraphScopeAvailableValues.category.size()
       assert ['cat1', 'cat2', 'cat3', 'cat4', 'cat5'] as Set == scopeInfo.optionalGraphScopeAvailableValues.category as Set

       //Check node scope prompt
       Map node = checkEnumNodeBasics("${ADDITIONAL_SCOPE_REQUIRED_FOR}${VALID_VALUES_FOR_FIELD_LOWER_CASE}Risks on GProduct", ADDITIONAL_SCOPE_REQUIRED, true)
       checkScopePromptDropdown(node, 'category', '', ['cat1', 'cat2', 'cat3', 'cat4', 'cat5'], [DEFAULT], SELECT_OR_ENTER_VALUE)
   }

   @Test
   void testLoadGraph_inScopeScopeValues_coordinateNotFound()
   {
       //Load graph with no scope
       String startCubeName = 'rpm.class.Product'
       topNodeScope = new CaseInsensitiveMap()
       Map options = [startCubeName: startCubeName, scope: topNodeScope]
       Map node = loadGraph(options)
       assert 1 == nodes.size()

       //User picks GProduct. Reload.
       topNodeScope.product = 'GProduct'
       options = [startCubeName: startCubeName, scope: topNodeScope, visInfo: visInfo]
       Map node = loadGraph(options)
       assert 2 == nodes.size()

       //User picks div = div1. Reload. This will result in InvalidCoordinateException since category is required scope.
       topNodeScope.div = 'div1'
       options = [startCubeName: startCubeName, scope: topNodeScope, visInfo: visInfo]
       Map node = loadGraph(options)
       assert 2 == nodes.size()

       //User picks category = catBogus. Reload. This will result in CoordinateNotFoundException since catBogus doesn't exist.
       topNodeScope.category = 'catBogus'
       options = [startCubeName: startCubeName, scope: topNodeScope, visInfo: visInfo]
       Map node = loadGraph(options)
       assert 2 == nodes.size()

       //Check graph scope prompt for category
       assert 5 == scopeInfo.optionalGraphScopeAvailableValues.category.size()
       assert ['cat1', 'cat2', 'cat3', 'cat4', 'cat5'] as Set == scopeInfo.optionalGraphScopeAvailableValues.category as Set

       //Check node scope prompt
       Map node = checkEnumNodeBasics("${REQUIRED_SCOPE_VALUE_NOT_FOUND_FOR}${VALID_VALUES_FOR_FIELD_LOWER_CASE}Risks on GProduct", IS_NOT_VALID_FOR, true)
       checkScopePromptDropdown(node, 'category', '', ['cat1', 'cat2', 'cat3', 'cat4',  'cat5'], [DEFAULT,], SELECT_OR_ENTER_VALUE, '')
   }
   */


    //*************************************************************************************

    @Override
    protected Visualizer getVisualizer()
    {
        return new RpmVisualizer(ncubeRuntime)
    }

    @Override
    protected VisualizerRelInfo getVisualizerRelInfo()
    {
        return new RpmVisualizerRelInfo(ncubeRuntime, appId)
    }

    private void checkNode(String nodeName, String nodeType, String nodeNamePrefix = '', String nodeDetailsMessage = '', boolean unableToLoad = false, boolean showCellValues = false)
    {
        Map node = nodes.values().find { Map node1 -> "${nodeNamePrefix}${nodeName}".toString() == node1.label }
        checkNodeAndEnumNodeBasics(node, unableToLoad, showCellValues)
        assert nodeType == node.title
        assert nodeType == node.detailsTitle1
        assert (node.details as String).contains(nodeDetailsMessage)
        assert "${nodeNamePrefix}${nodeName}".toString() == node.label
        if (showCellValues)
        {
            assert nodeName == node.detailsTitle2
        }
        else if (nodeName == nodeType)  //No detailsTitle2 for non-EPM classes (i.e. nodeName equals nodeType)
        {
            assert null == node.detailsTitle2
        }
        else
        {
            assert nodeName == node.detailsTitle2
        }
    }

    private void checkEnumNode(String nodeTitle, String nodeDetailsMessage = '', boolean unableToLoad = false, boolean showCellValues = false)
    {
        Map node = nodes.values().find {Map node1 ->  nodeTitle == node1.title}
        checkNodeAndEnumNodeBasics(node, unableToLoad, showCellValues)
        assert null == node.label
        assert nodeTitle == node.detailsTitle1
        assert null == node.detailsTitle2
        assert (node.details as String).contains(nodeDetailsMessage)
    }

    private static void checkNodeAndEnumNodeBasics(Map node, boolean unableToLoad = false, boolean showCellValues = false)
    {
        String nodeDetails = node.details as String
        if (showCellValues && unableToLoad)
        {
            assert nodeDetails.contains("${UNABLE_TO_LOAD}traits")
            assert false == node.showCellValuesLink
            assert false == node.cubeLoaded
            assert true == node.showCellValues
        }
        else if (unableToLoad)
        {
            assert nodeDetails.contains("${UNABLE_TO_LOAD}")
            assert false == node.showCellValuesLink
            assert false == node.cubeLoaded
            assert false == node.showCellValues
        }
        else if (showCellValues)
        {
            assert !nodeDetails.contains("${UNABLE_TO_LOAD}")
            assert true == node.showCellValuesLink
            assert true == node.cubeLoaded
            assert true == node.showCellValues
        }
        else
        {
            assert !nodeDetails.contains("${UNABLE_TO_LOAD}")
            assert true == node.showCellValuesLink
            assert true == node.cubeLoaded
            assert false == node.showCellValues
        }

        if (unableToLoad)
        {
            assert nodeDetails.contains(DETAILS_LABEL_AVAILABLE_SCOPE)
            assert !nodeDetails.contains(DETAILS_LABEL_UTILIZED_SCOPE_WITHOUT_TRAITS)
            assert !nodeDetails.contains(DETAILS_LABEL_UTILIZED_SCOPE_WITH_TRAITS)
            assert !nodeDetails.contains(DETAILS_LABEL_FIELDS)
            assert !nodeDetails.contains(DETAILS_LABEL_FIELDS_AND_TRAITS)
            assert !nodeDetails.contains(DETAILS_LABEL_CLASS_TRAITS)
        }
        else if (showCellValues)
        {
            assert nodeDetails.contains(DETAILS_LABEL_AVAILABLE_SCOPE)
            assert !nodeDetails.contains(UNABLE_TO_LOAD)
            assert !nodeDetails.contains(DETAILS_LABEL_UTILIZED_SCOPE_WITHOUT_TRAITS)
            assert nodeDetails.contains(DETAILS_LABEL_UTILIZED_SCOPE_WITH_TRAITS)
            assert nodeDetails.contains(DETAILS_LABEL_FIELDS_AND_TRAITS)
            assert nodeDetails.contains(DETAILS_LABEL_CLASS_TRAITS)
        }
        else
        {
            assert nodeDetails.contains(DETAILS_LABEL_AVAILABLE_SCOPE)
            assert !nodeDetails.contains(UNABLE_TO_LOAD)
            assert nodeDetails.contains(DETAILS_LABEL_UTILIZED_SCOPE_WITHOUT_TRAITS)
            assert nodeDetails.contains(DETAILS_LABEL_FIELDS)
            assert !nodeDetails.contains(DETAILS_LABEL_FIELDS_AND_TRAITS)
            assert !nodeDetails.contains(DETAILS_LABEL_CLASS_TRAITS)
        }
    }

    private static void checkDefaultScopePrompts(Map node, boolean isTopNode = false, boolean epmClass = true)
    {
        checkScopePromptTitle(node, '_effectiveVersion', true)
        checkScopePromptDropdown(node, '_effectiveVersion', TEST_APP_VERSION, [], [], '', isTopNode)
        if (epmClass)
        {
            checkScopePromptTitle(node, 'policyControlDate', true)
            checkScopePromptDropdown(node, 'policyControlDate', DEFAULT_SCOPE_DATE, [], [], '', isTopNode)
            checkScopePromptTitle(node, 'quoteDate', true)
            checkScopePromptDropdown(node, 'quoteDate', DEFAULT_SCOPE_DATE, [], [], '', isTopNode)
        }
        else
        {
            checkNoScopePrompt(node, 'policyControlDate')
            checkNoScopePrompt(node, 'quoteDate')
        }
    }

    private checkValidRpmClass( String startCubeName)
    {
        assert nodes.size() == 1
        assert edges.size() == 0
        Map node = nodes.values().find { startCubeName == (it as Map).cubeName}
        assert 'ValidRpmClass' == node.title
        assert 'ValidRpmClass' == node.detailsTitle1
        assert null == node.detailsTitle2
        assert 'ValidRpmClass' == node.label
        assert  null == node.typesToAdd
        assert UNSPECIFIED == node.group
        assert null == node.fromFieldName
        assert '1' ==  node.level
        String nodeDetails = node.details as String
        assert nodeDetails.contains(DETAILS_LABEL_UTILIZED_SCOPE_WITHOUT_TRAITS)
        assert nodeDetails.contains("${DETAILS_LABEL_FIELDS}<pre><ul></ul></pre>")
        assert !nodeDetails.contains(DETAILS_LABEL_FIELDS_AND_TRAITS)
        assert !nodeDetails.contains(DETAILS_LABEL_CLASS_TRAITS)
    }

    private static NCube createNCubeWithValidRpmClass(String cubeName)
    {
        NCube cube = new NCube(cubeName)
        cube.applicationID = appId
        String axisName = AXIS_FIELD
        cube.addAxis(new Axis(axisName, AxisType.DISCRETE, AxisValueType.STRING, false, Axis.SORTED, 1))
        cube.addColumn(axisName, CLASS_TRAITS)
        axisName = AXIS_TRAIT
        cube.addAxis(new Axis(axisName, AxisType.DISCRETE, AxisValueType.STRING, false, Axis.SORTED, 2))
        cube.addColumn(axisName, R_EXISTS)
        cube.addColumn(axisName, R_RPM_TYPE)
        ncubeRuntime.addCube(cube)
        return cube
    }

    private static Map getAvailableScope(Map utilizedScope = [:])
    {
        Map availableScope = new CaseInsensitiveMap(DEFAULT_SCOPE)
        availableScope.putAll(utilizedScope)
        return availableScope
    }

    List<String> getLibraryCubeNames()
    {
        return ['rpmvisualizer/rpm.json',
                'rpmvisualizer/Product.json',
                'rpmvisualizer/Risk.json',
                'rpmvisualizer/Coverage.json',
                'rpmvisualizer/ent.manual.BusinessDivision.json',
                'rpmvisualizer/ent.manual.State.json']
    }

    List<String> getTestCubesNames()
    {
        return ['rpmvisualizer/rpm.class.Coverage.json',
                'rpmvisualizer/rpm.class.Product.json',
                'rpmvisualizer/rpm.class.Risk.json',
                'rpmvisualizer/rpm.enum.Coverage.Coverages.json',
                'rpmvisualizer/rpm.enum.Product.Risks.json',
                'rpmvisualizer/rpm.enum.Risk.Coverages.json',
                'rpmvisualizer/rpm.enum.Risk.Risks.json',
                'rpmvisualizer/rpm.scope.class.Coverage.classTraits.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.field1And2.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.field3CovC.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.field3CovI.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.fieldACoverage.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.fieldBCoverage.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.fieldCCoverage.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.fieldTCoverage.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.field4.json',
                'rpmvisualizer/rpm.scope.class.Coverage.traits.StatCode.json',
                'rpmvisualizer/rpm.scope.class.Product.classTraits.json',
                'rpmvisualizer/rpm.scope.class.Product.traits.json',
                'rpmvisualizer/rpm.scope.class.Risk.classTraits.json',
                'rpmvisualizer/rpm.scope.class.Risk.traits.json',
                'rpmvisualizer/rpm.scope.class.Risk.traits.Coverages.json',
                'rpmvisualizer/rpm.scope.class.Risk.traits.Risks.json',
                'rpmvisualizer/rpm.scope.class.Risk.traits.fieldARisk.json',
                'rpmvisualizer/rpm.scope.class.Risk.traits.fieldBRisk.json',
                'rpmvisualizer/rpm.scope.class.Risk.traits.fieldDRisk.json',
                'rpmvisualizer/rpm.scope.enum.Coverage.Coverages.traits.json',
                'rpmvisualizer/rpm.scope.enum.Product.Risks.traits.json',
                'rpmvisualizer/rpm.scope.enum.Product.Risks.traits.exists.json',
                'rpmvisualizer/rpm.scope.enum.Product.Risks.traits.exists.category.json',
                'rpmvisualizer/rpm.scope.enum.Risk.Coverages.traits.json',
                'rpmvisualizer/rpm.scope.enum.Risk.Coverages.traits.exists.json',
                'rpmvisualizer/rpm.scope.enum.Risk.Risks.traits.json',
                'rpmvisualizer/rpm.class.DataElementInventory.json',
                'rpmvisualizer/rpm.enum.dataelement.PrintWaiverIndicator.json',
                'rpmvisualizer/rpm.class.party.BaseParty.json',
                'rpmvisualizer/rpm.class.party.MoreNamedInsured.json',
                'rpmvisualizer/rpm.class.party.ProfitCenter.json',
                'rpmvisualizer/rpm.class.partyrole.BasePartyRole.json',
                'rpmvisualizer/rpm.class.partyrole.LossPrevention.json',
                'rpmvisualizer/rpm.enum.partyrole.BasePartyRole.Parties.json']
    }

    @Override
    protected void addTestCubesToCache()
    {
        if (!libraryCubes)
        {
            libraryCubes = getCubesFromResource(libraryCubeNames)
        }
        addCubes(libraryCubes)

        super.addTestCubesToCache()
    }
}
