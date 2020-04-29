package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.ncube.RuleInfo
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.exception.InvalidCoordinateException
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.SafeSimpleDateFormat
import com.google.common.base.Splitter
import groovy.transform.CompileStatic

import static com.cedarsoftware.util.ExceptionUtilities.getDeepestException

/**
 * Provides information to visualize a source rpm class, a target rpm class
 * and their relationship.
 */

@CompileStatic
class RpmVisualizerRelInfo extends VisualizerRelInfo implements RpmVisualizerConstants
{
	protected RpmVisualizerHelper helper = new RpmVisualizerHelper(runtimeClient, appId)
	protected String sourceFieldRpmType
	protected Map<String, Map<String, Object>> sourceTraits
	protected Map<String, Map<String, Object>> targetTraits

	static final SafeSimpleDateFormat DATE_TIME_FORMAT = new SafeSimpleDateFormat('yyyy-MM-dd')

	protected RpmVisualizerRelInfo(){}

	protected RpmVisualizerRelInfo(NCubeRuntimeClient runtimeClient, ApplicationID appId)
	{
		super(runtimeClient, appId)
	}

	@Override
	protected Set<String> getRequiredScope()
	{
		Set<String> requiredScope = super.requiredScope
		requiredScope.remove(AXIS_FIELD)
		requiredScope.remove(AXIS_NAME)
		requiredScope.remove(AXIS_TRAIT)
		return requiredScope
	}

	@Override
	protected initSelectedNode(VisualizerInfo visInfo, Map selectedNode)
	{
		super.initSelectedNode(visInfo, selectedNode)
		targetScope = new CompactCILinkedMap()
		sourceTraits = selectedNode.sourceTraits as Map
		sourceFieldRpmType = selectedNode.sourceFieldRpmType as String
	}

	@Override
	protected String getDetails(VisualizerInfo visInfo)
	{
		StringBuilder sb = new StringBuilder()

		//Scope messages
		sb.append(createNodeDetailsScopeMessage(visInfo))

		//Scope
		if (cubeLoaded)
		{
			String heading
			String title
			if (showCellValues)
			{
				heading = 'Utilized scope with traits'
				title = 'The scope keys used to load the class with all available traits. A sub-set of available scope.'
			}
			else
			{
				heading = 'Utilized scope with no traits'
				title = 'The scope keys used to load the class with the minimal set of traits needed for visualization. A sub-set of available scope.'
			}
			getDetailsMap(sb, heading, targetScope.sort(), title)
		}

		//Fields
		if (cubeLoaded)
		{
			if (showCellValues)
			{
				addClassTraits(sb)
			}
			addFieldDetails(sb)
		}
		return sb.toString()
	}

	private void addFieldDetails(StringBuilder sb)
	{
		if (showCellValues)
		{
			sb.append("<b>Fields and traits</b>")
		}
		else
		{
			sb.append("<b>Fields</b>")
		}
		sb.append("<pre><ul>")
		targetTraits.each { String fieldName, v ->
			if (CLASS_TRAITS != fieldName)
			{
				if (showCellValues)
				{
					sb.append("<li><b>${fieldName}</b></li>")
					addTraits(sb, fieldName)
				}
				else
				{
					sb.append("<li>${fieldName}</li>")
				}
			}
		}
		sb.append("</ul></pre>")
	}

	private void addTraits(StringBuilder sb, String fieldName)
	{
		Map<String, Object> traits = (Map<String, Object>) targetTraits[fieldName].sort()
		sb.append("<pre><ul>")
		traits.each { String traitName, Object traitValue ->
			if (traitValue != null)
			{
				String traitString = traitValue.toString()
				if (traitString.startsWith(HTTP) || traitString.startsWith(HTTPS) || traitString.startsWith(FILE))
				{
					sb.append("""<li>${traitName}: <a href="#" onclick='window.open("${traitString}");return false;'>${traitString}</a></li>""")
				}
				else
				{
					sb.append("<li>${traitName}: ${traitValue}</li>")
				}
			}
		}
		sb.append("</ul></pre>")
	}

	private void addClassTraits(StringBuilder sb)
	{
		sb.append("<b>Class traits</b>")
		addTraits(sb, CLASS_TRAITS)
		sb.append("${BREAK}")
	}

	@Override
	protected String getGroupName(VisualizerInfo visInfo, String cubeName = targetCube.name)
	{
		Iterable<String> splits = Splitter.on('.').split(cubeName)
		String group = splits[2].toUpperCase()
		return visInfo.allGroupsKeys.contains(group) ? group : UNSPECIFIED
	}

	private String getTargetScopedName()
	{
		Map<String, Object> classTraitsTraitMap = targetTraits ? targetTraits[CLASS_TRAITS] as Map : null
		return classTraitsTraitMap ? classTraitsTraitMap[R_SCOPED_NAME] : null
	}

	protected String getNextTargetCubeName(String targetFieldName)
	{
		if (hasScopedNameTrait(sourceCube))
		{
			String scopedName = sourceTraits[CLASS_TRAITS][R_SCOPED_NAME]
			return !scopedName ?: RPM_CLASS_DOT + sourceFieldRpmType
		}
		return RPM_CLASS_DOT + targetFieldName
	}

	private void setTargetScope(VisualizerInfo visInfo, Map output)
	{
		Set<String> scopeCollector = new CaseInsensitiveSet<>()
		addRequiredScopeKeys(visInfo)
		scopeCollector.addAll(visInfo.requiredScopeKeysByCube[targetCube.name])
		scopeCollector << EFFECTIVE_VERSION

		Set keysUsed = NCube.getRuleInfo(output).getInputKeysUsed()
		scopeCollector.addAll(keysUsed)

		Map<String, Object> scope = new CompactCILinkedMap(availableTargetScope)
		cullScope(scope.keySet(), scopeCollector)
		targetScope.putAll(scope)
	}

	private void addBackToAvailableTargetScope(VisualizerInfo visInfo)
	{
		visInfo.inputScope.each{String scopeKey, Object ScopeValue ->
			availableTargetScope.putIfAbsent(scopeKey, ScopeValue)
		}
	}

	private void removeNotExistsFields()
	{
		targetTraits.keySet().removeAll { String fieldName -> !targetTraits[fieldName][R_EXISTS] }
	}

	private static void cullScope(Set<String> scopeKeys, Set scopeCollector)
	{
		scopeKeys.removeAll { String scopeKey -> !(scopeCollector.contains(scopeKey) || scopeKey.startsWith(SYSTEM_SCOPE_KEY_PREFIX)) }
	}

	@Override
	protected Map<String, Object> createEdge(Long edgeId)
	{
		Map<String, Object> edge = super.createEdge(edgeId)
		Map<String, Map<String, Object>> sourceTraits = sourceTraits

		Map<String, Map<String, Object>> sourceFieldTraitMap = sourceTraits[sourceFieldName] as Map
		String vMin = sourceFieldTraitMap[V_MIN] as String ?: V_MIN_CARDINALITY
		String vMax = sourceFieldTraitMap[V_MAX] as String ?: V_MAX_CARDINALITY

		if (targetCube.name.startsWith(RPM_ENUM_DOT))
		{
			edge.label = nodeLabelPrefix + sourceFieldName
			edge.title = "Field ${sourceFieldName} cardinality ${vMin}:${vMax}".toString()
		}
		else
		{
			edge.title = "Valid value ${sourceFieldName} cardinality ${vMin}:${vMax}".toString()
		}

		return edge
	}

	@Override
	protected Map<String, Object> createNode(VisualizerInfo visInfo, String group = null)
	{
		Map<String, Object> node = super.createNode(visInfo, group)
		node.sourceTraits = sourceTraits
		node.sourceFieldRpmType = sourceFieldRpmType
		if (targetCube.name.startsWith(RPM_ENUM_DOT))
		{
			node.label = null
			node.detailsTitle2 = null
			node.title = node.detailsTitle1
			node.typesToAdd = null
			node.newVisualFromHereMessage = 'Functionality to start a new visual from a valid values (rpm.enum) class is not yet implemented'
		}
		return node
	}

	@Override
	protected boolean isDerivedScopeKey(VisualizerInfo visInfo, String scopeKey)
	{
		//For the starting cube of the graph (top node) no scope keys are derived by
		//the visualizer. For all other classes (which all have a sourceCube), the
		//visualizer adds to the scope as it processes through the graph and that scope is
		//considered derived (e.g. scope for product, risk, coverage, sourceProduct,
		//sourceRisk, sourceCoverage, etc.).
		if (SOURCE_FIELD_NAME == scopeKey)
		{
			return true
		}
		else if (!sourceCube)
		{
			return false
		}

		String strippedKey = scopeKey.replaceFirst('source', '')
		if (visInfo.allGroupsKeys.contains(strippedKey))
		{
			return true
		}
		return false
	}

	@Override
	protected String getLabel(String cubeName)
	{
		String scopeKey = getDotSuffix(cubeName)
		return availableTargetScope[scopeKey] ?: getCubeDisplayName(cubeName)
	}

	@Override
	protected String getCubeDisplayName(String cubeName)
	{
		if (cubeName.startsWith(RPM_CLASS_DOT))
		{
			return cubeName - RPM_CLASS_DOT
		}
		else if (cubeName.startsWith(RPM_ENUM_DOT))
		{
			return cubeName - RPM_ENUM_DOT
		}
		else{
			return cubeName
		}
	}

	@Override
	protected String getSourceDescription()
	{
		String sourceCubeName = sourceCube.name
		if (sourceCubeName.startsWith(RPM_CLASS_DOT))
		{
			return getDotSuffix(getLabel(sourceCubeName))
		}
		else if (sourceCubeName.startsWith(RPM_ENUM_DOT))
		{
			if (targetScopedName)
			{
				String sourceDisplayName = getCubeDisplayName(sourceCubeName)
				String scopeKeyForSourceOfSource = getDotPrefix(sourceDisplayName)
				String nameOfSourceOfSource = sourceScope[scopeKeyForSourceOfSource]
				String fieldNameSourceOfSource = sourceScope[SOURCE_FIELD_NAME]
				return "field ${fieldNameSourceOfSource} on ${nameOfSourceOfSource}".toString()
			}
			else{
				return getCubeDisplayName(sourceCubeName)
			}
		}
		return null
	}

	@Override
	protected String getCubeDetailsTitle1()
	{
		String targetCubeName = targetCube.name
		if (targetCubeName.startsWith(RPM_CLASS_DOT))
		{
			return getCubeDisplayName(targetCubeName)
		}
		else if (targetCubeName.startsWith(RPM_ENUM_DOT))
		{
			if (sourceFieldName && sourceCube)
			{
				String prefix = nodeLabelPrefix ? "${nodeLabelPrefix}valid" : 'Valid'
				return "${prefix} values for field ${sourceFieldName} on ${getLabel(sourceCube.name)}".toString()
			}
			else{
				return getCubeDisplayName(targetCubeName)
			}
		}
		return null
	}

	@Override
	protected String getCubeDetailsTitle2(String label)
	{
		return targetCube.name.startsWith(RPM_CLASS_DOT) ? label : null
	}

	/**
	 * Loads fields and traits on the class into the targetTraits map.
	 * If the current node is the selected node and an invalid, missing or unbound scope key is encountered,
	 * checks if the inputScope contains the key. If yes, loads again using the key provided in inputScope.
	 *
	 * @return boolean cubeLoaded
	 */

	protected boolean loadCube(VisualizerInfo visInfo, Map output = new CompactCILinkedMap())
	{
		if (!canLoadTarget())
		{
			return false
		}
		
		loadAgain = false
		try
		{
			targetTraits = new CompactCILinkedMap()
			if (targetCube.name.startsWith(RPM_ENUM))
			{
				helper.loadRpmClassFields(appId, RPM_ENUM, targetCube.name - RPM_ENUM_DOT, availableTargetScope, targetTraits, showCellValues, output)
			}
			else
			{
				helper.loadRpmClassFields(appId, RPM_CLASS, targetCube.name - RPM_CLASS_DOT, availableTargetScope, targetTraits, showCellValues, output)
			}
			handleUnboundScope(visInfo, targetCube.getRuleInfo(output))
			removeNotExistsFields()
			cubeLoaded = true
			showCellValuesLink = true
		}
		catch (Exception e)
		{
			cubeLoaded = false
			showCellValuesLink = false
			Throwable t = getDeepestException(e)
			if (t instanceof InvalidCoordinateException)
			{
				handleInvalidCoordinateException(t as InvalidCoordinateException, visInfo)
			}
			else if (t instanceof CoordinateNotFoundException)
			{
				handleCoordinateNotFoundException(t as CoordinateNotFoundException, visInfo)
			}
			else
			{
				handleException(t, visInfo)
			}
		}

		if (loadAgain)
		{
			return loadCube(visInfo, output)
		}

		setTargetScope(visInfo, output)
		addBackToAvailableTargetScope(visInfo)
		return true
	}

	private boolean canLoadTarget()
	{
		//When the source cube points directly to the target cube (source cube and target cube are both rpm.class),
		//check if the source field name matches up with the scoped name of the target. If not, traits cannot
		//be loaded for the target in the visualization.
		if (sourceCube && sourceCube.name.startsWith(RPM_CLASS_DOT) && targetCube.name.startsWith(RPM_CLASS_DOT) &&
				hasScopedNameTrait(targetCube))
		{
			NCube classTraitsCube = runtimeClient.getCube(appId, RPM_SCOPE_CLASS_DOT + sourceFieldRpmType + DOT_CLASS_TRAITS)
			if (!classTraitsCube.getAxis(sourceFieldRpmType).findColumn(sourceFieldName))
			{
				nodeLabelPrefix = 'Unable to load '
				targetTraits = new CompactCILinkedMap()
				nodeDetailsMessages << getCannotLoadTargetMessage(sourceFieldRpmType)
				cubeLoaded = false
				showCellValuesLink = false
				return false
			}
		}
		return true
	}

	private void handleUnboundScope(VisualizerInfo visInfo, RuleInfo ruleInfo)
	{
		List<MapEntry> unboundAxesList = ruleInfo.getUnboundAxesList()
		if (unboundAxesList && helper.handleUnboundScope(visInfo, this, unboundAxesList))
		{
			if (loadAgain)
			{
				return
			}
			nodeDetailsMessages << """<span class="${DETAILS_CLASS_DEFAULT_VALUE}">Defaults were used for some scope keys.</span>${DOUBLE_BREAK}""".toString()
		}
	}

	private void handleCoordinateNotFoundException(CoordinateNotFoundException e, VisualizerInfo visInfo)
	{
		StringBuilder sb = helper.handleCoordinateNotFoundException(e, visInfo, this)
		if (loadAgain)
		{
			return
		}
		StringBuilder message = new StringBuilder("""<span class="${DETAILS_CLASS_MISSING_VALUE}">Unable to load ${visInfo.getLoadTarget(this.showingHidingCellValues)}. The value ${e.value} is not valid for ${e.axisName}.</span>${DOUBLE_BREAK}""")
		message.append(sb)
		nodeDetailsMessages << message.toString()
		nodeLabelPrefix = 'Required scope value not found for '
		targetTraits = new CompactCILinkedMap()
	}

	private void handleInvalidCoordinateException(InvalidCoordinateException e, VisualizerInfo visInfo)
	{
		StringBuilder sb = helper.handleInvalidCoordinateException(e, visInfo, this, MANDATORY_SCOPE_KEYS)
		if (loadAgain)
		{
			return
		}

		StringBuilder message = new StringBuilder("""<span class="${DETAILS_CLASS_MISSING_VALUE}">Unable to load ${visInfo.getLoadTarget(this.showingHidingCellValues)}. Additional scope is required.</span>${DOUBLE_BREAK}""")
		message.append(sb)
		nodeDetailsMessages << message.toString()
		nodeLabelPrefix = 'Additional scope required for '
		targetTraits = new CompactCILinkedMap()
	}

	private void handleException(Throwable e, VisualizerInfo visInfo)
	{
		StringBuilder sb = new StringBuilder("<b>Unable to load ${visInfo.getLoadTarget(this.showingHidingCellValues)} due to an exception.</b>${DOUBLE_BREAK}")
		sb.append(helper.handleException(e))
		nodeDetailsMessages << sb.toString()
		nodeLabelPrefix = "Unable to load "
		targetTraits = new CompactCILinkedMap()
	}

	/**
	 * Sets the basic nodeScope required to load a target class based on scoped source class,
	 * source field name, target class name, and current nodeScope.
	 * Retains all other nodeScope.
	 *
	 * @param targetCube String target cube
	 * @param sourceFieldRpmType String source field type
	 * @param sourceFieldName String source field name
	 * @param scope Map<String, Object> nodeScope
	 *
	 * @return Map new nodeScope
	 *
	 */
	protected void populateScopeRelativeToSource(String sourceFieldRpmType, String targetFieldName, Map scope)
	{
		availableTargetScope = new CompactCILinkedMap(scope)

		if (targetCube.name.startsWith(RPM_ENUM))
		{
			availableTargetScope[SOURCE_FIELD_NAME] = targetFieldName
		}
		else if (hasScopedNameTrait(targetCube))
		{
			String newScopeKey = sourceFieldRpmType
			String oldValue = scope[newScopeKey]
			if (oldValue)
			{
				availableTargetScope[SOURCE_SCOPE_KEY_PREFIX + sourceFieldRpmType] = oldValue
			}
			availableTargetScope[newScopeKey] = targetFieldName
		}
	}

	@Override
	protected void populateScopeDefaults(VisualizerInfo visInfo)
	{
		Map<String, Object> scopeDefaults = new CompactCILinkedMap()
		String scopeValue = visInfo.inputScope[EFFECTIVE_VERSION] ?: appId.version
		addScopeDefault(scopeDefaults, EFFECTIVE_VERSION, scopeValue)

		if (hasScopedNameTrait(targetCube) || targetCube.name.startsWith(RPM_ENUM_DOT) && hasScopedNameTrait(sourceCube))
		{
			String date = DATE_TIME_FORMAT.format(new Date())
			scopeValue = visInfo.inputScope[POLICY_CONTROL_DATE] ?: date
			addScopeDefault(scopeDefaults, POLICY_CONTROL_DATE, scopeValue)
			scopeValue = visInfo.inputScope[QUOTE_DATE] ?: date
			addScopeDefault(scopeDefaults, QUOTE_DATE, scopeValue)
		}
		availableTargetScope.putAll(scopeDefaults)
	}

	private void addScopeDefault(Map<String, Object> scopeDefaults, String scopeKey, Object value)
	{
		addNodeScope(null, scopeKey, false, true)
		scopeDefaults[scopeKey] = value
	}

	@Override
	protected void setLoadAgain(VisualizerInfo visInfo, String scopeKey)
	{
		Object scopeValue = visInfo.inputScope[scopeKey]
		if (availableTargetScope[scopeKey] != scopeValue)
		{
			availableTargetScope[scopeKey] = scopeValue
			targetScope[scopeKey] = scopeValue
			loadAgain = true
		}
	}

	private static boolean hasScopedNameTrait(NCube cube)
	{
		return cube.getAxis(AXIS_TRAIT).findColumn(R_SCOPED_NAME)
	}

	/* TODO: Will revisit providing "in scope" available scope values for r:exists at a later time.
    @Override
    protected Set<Object> getColumnValues(String cubeName, String axisName, Map coordinate)
    {
        NCube cube = runtimeClient.getCube(appId, cubeName)
        if (coordinate && R_EXISTS == coordinate[AXIS_TRAIT])
        {
            try
            {
                return getInScopeColumnValues(cube, axisName, coordinate)
            }
            catch (CoordinateNotFoundException|InvalidCoordinateException e)
            {
                //There is more than one missing or invalid scope key so cannot determine "in scope" column values.
                //Get all column values instead.
                int debug = 0
            }
        }
        return getAllColumnValues(cube, axisName)
    }

    private static Set<Object> getInScopeColumnValues(NCube cube, String axisName, Map coordinate)
    {
        coordinate[axisName] = new LinkedHashSet()
        Map map = cube.getMap(coordinate)
        Map inScopeMapEntries = map.findAll{Object columnName, Object columnValue ->
            true == columnValue
        }
        return inScopeMapEntries.keySet()
    }*/

	private String getCannotLoadTargetMessage(String type) {

		String sourceCubeDisplayName = getCubeDisplayName(sourceCube.name)
		String targetCubeDisplayName = getCubeDisplayName(targetCube.name)

		"""\
<b>Unable to load the class. ${sourceCubeDisplayName} points directly to ${targetCubeDisplayName} via field ${sourceFieldName}, but \
there is no ${type.toLowerCase()} named ${sourceFieldName} on ${type}.</b> ${DOUBLE_BREAK}"""
	}
}