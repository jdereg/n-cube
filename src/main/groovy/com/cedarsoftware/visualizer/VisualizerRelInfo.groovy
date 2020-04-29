package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import groovy.transform.CompileStatic

import static com.cedarsoftware.visualizer.VisualizerConstants.*

/**
 * Provides information to visualize a source cube, a target cube
 * and their relationship.
 */

@CompileStatic
class VisualizerRelInfo
{
	protected static NCubeRuntimeClient runtimeClient
	protected ApplicationID appId
	protected List<String> nodeDetailsMessages = []
	protected Map<String, Object> availableTargetScope = new CompactCILinkedMap()
	protected Map<String, Set<Object>> availableScopeValues = new CompactCILinkedMap()
	protected Map<String, Set<String>> scopeCubeNames = new CompactCILinkedMap()

	protected boolean showingHidingCellValues

	protected long targetId
	protected NCube targetCube
	protected Map<String, Object> targetScope = new CompactCILinkedMap()
	protected long targetLevel
	protected String nodeLabelPrefix = ''

	protected long sourceId
	protected NCube sourceCube
	protected Map<String, Object> sourceScope
	protected String sourceFieldName
	protected List<Long> sourceTrail = []

	protected boolean cubeLoaded
	protected boolean showCellValuesLink
	protected boolean showCellValues
	protected boolean loadAgain

	protected List<VisualizerCellInfo> cellInfo

	protected List<String> typesToAdd

	VisualizerRelInfo() {}

	VisualizerRelInfo(NCubeRuntimeClient runtimeClient, ApplicationID applicationId)
	{
		this.runtimeClient = runtimeClient
		this.appId = applicationId
	}

	protected init(Map options, VisualizerInfo visInfo)
	{
		targetId = 1
		targetLevel = 1
		targetCube = runtimeClient.getCube(appId, options.startCubeName as String)
		addRequiredScopeKeys(visInfo)
		showCellValuesLink = true
		populateScopeDefaults(visInfo)
	}

	protected initSelectedNode(VisualizerInfo visInfo, Map selectedNode)
	{
		targetCube = runtimeClient.getCube(appId, selectedNode.cubeName as String)
		String sourceCubeName = selectedNode.sourceCubeName as String
		sourceCube = sourceCubeName ? runtimeClient.getCube(appId, sourceCubeName) : null
		sourceFieldName = selectedNode.fromFieldName as String
		sourceScope = selectedNode.sourceScope as CompactCILinkedMap
		sourceTrail = selectedNode.sourceTrail as List
		sourceId = selectedNode.sourceId as Long
		targetId = Long.valueOf(selectedNode.id as String)
		targetLevel = Long.valueOf(selectedNode.level as String)
		showCellValues = selectedNode.showCellValues as boolean
		showCellValuesLink = selectedNode.showCellValuesLink as boolean
		cubeLoaded = selectedNode.cubeLoaded as boolean
		typesToAdd = selectedNode.typesToAdd as List
		visInfo.inputScope = new CompactCILinkedMap(selectedNode.availableScope as Map)
		targetScope = selectedNode.scope as CompactCILinkedMap ?:  new CompactCILinkedMap()
		availableTargetScope = selectedNode.availableScope as CompactCILinkedMap ?:  new CompactCILinkedMap()
		availableScopeValues = selectedNode.availableScopeValues as CompactCILinkedMap ?:  new CompactCILinkedMap()
		showingHidingCellValues = selectedNode.showingHidingCellValues as boolean

		//If in the process of showing/hiding cell values, then the supplied scope, available scope values and scope
		//cube names are used to load cell values.
		if (showingHidingCellValues)
		{
			scopeCubeNames = selectedNode.scopeCubeNames as CompactCILinkedMap ?:  new CompactCILinkedMap()
		}
		//Else, node details are being loaded for the node or a scope change is being applied to the node. In this case,
		//clear out any available target scope that is derived scope. This will result in the node being
		//reloaded with updated available scope values for the removed scope.
		else
		{
			Set<String> scopeKeys = (Set<String>)new CaseInsensitiveSet(availableTargetScope.keySet())
			scopeKeys.each {String scopeKey ->
				if (!isDerivedScopeKey(visInfo, scopeKey))
				{
					availableTargetScope.remove(scopeKey)
				}
			}
			availableScopeValues = new CompactCILinkedMap()
			scopeCubeNames = new CompactCILinkedMap()
		}
		populateScopeDefaults(visInfo)
	}

	protected boolean loadCube(VisualizerInfo visInfo)
	{
		cellInfo = []
		cubeLoaded = true
		if (showCellValues)
		{
			Map<Set<Long>, Object> cellMap = (Map<Set<Long>, Object>) targetCube.cellMap
			cellMap.each { Set<Long> ids, Object noExecuteCell ->
				Map<String, Object> coordinate = availableTargetScope as CompactCILinkedMap ?: new CompactCILinkedMap()
				coordinate.putAll(targetCube.getCoordinateFromIds(ids))
				VisualizerCellInfo visCellInfo = new VisualizerCellInfo(runtimeClient, appId, String.valueOf(targetId), coordinate)
				try
				{
					visCellInfo.cell = targetCube.getCell(coordinate)
				}
				catch (Exception e)
				{
					visCellInfo.exception = e
				}
				visCellInfo.noExecuteCell = noExecuteCell
				cellInfo << visCellInfo
			}
			cellInfo.sort { VisualizerCellInfo cellInfo ->
				cellInfo.coordinate.toString()
			}
		}
		return true
	}

	protected Set<String> getRequiredScope()
	{
		return targetCube.getRequiredScope(availableTargetScope, new CompactCILinkedMap())
	}

	protected String getDetails(VisualizerInfo visInfo)
	{
		StringBuilder sb = new StringBuilder()

		getDetailsMap(sb, 'Scope', targetScope.sort(), 'The scope keys used to load the cube. A sub-set of available scope.')
		getDetailsMap(sb, 'Available scope', availableTargetScope.sort(), 'The scope keys available as the cube was loaded in the visualization.')
		getDetailsSet(sb, 'Axes', targetCube.axisNames)

		//Cell values
		if (cubeLoaded && showCellValues)
		{
			addCellValueSection(visInfo, sb)
		}
		return sb.toString()
	}

	private void addCellValueSection(VisualizerInfo visInfo, StringBuilder sb)
	{
		StringBuilder cellValuesBuilder = new StringBuilder()
		StringBuilder linkBuilder = new StringBuilder()
		sb.append("<b>Cell values</b>")
		getCellValues(visInfo, cellValuesBuilder, linkBuilder )
		sb.append(linkBuilder.toString())
		sb.append("""<pre><ul class="${DETAILS_CLASS_CELL_VALUES}">""")
		sb.append(cellValuesBuilder.toString())
		sb.append("</ul></pre>")
	}

	private void getCellValues(VisualizerInfo visInfo, StringBuilder cellValuesBuilder, StringBuilder linkBuilder)
	{
		Long id = 0l

		if (cellInfo)
		{
			cellInfo.each { VisualizerCellInfo visCellInfo ->
				visCellInfo.getCellValue(visInfo, this, id++, cellValuesBuilder)
			}

			linkBuilder.append(DOUBLE_BREAK)
			linkBuilder.append("""<a href="#" title="Expand all cell details" class="${DETAILS_CLASS_EXPAND_ALL}"">Expand all</a>""")
			linkBuilder.append("${SPACE}${SPACE}")
			linkBuilder.append("""<a href="#" title="Collapse all cell details" class="${DETAILS_CLASS_COLLAPSE_ALL}"">Collapse all</a>""")
			linkBuilder.append(BREAK)
		}
		else
		{
			cellValuesBuilder.append('none')
		}
	}

	protected static void getDetailsMap(StringBuilder sb, String heading, Map<String, Object> map, String title = '')
	{
		sb.append("<b>${heading}</b>")
		sb.append("""<pre title="${title}"><ul>""")
		if (map)
		{
			map.each { String key, Object value ->
				sb.append("<li>${key}: ${value}</li>")
			}
		}
		else
		{
			sb.append("<li>none</li>")
		}
		sb.append("</ul></pre>${BREAK}")
	}

	protected static void getDetailsSet(StringBuilder sb, String heading, Collection<String> set, String title = '')
	{
		sb.append("<b>${heading}</b>")
		sb.append("""<pre title="$title"><ul>""")
		if (set)
		{
			set.each { String value ->
				sb.append("<li>${value}</li>")
			}
		}
		else
		{
			sb.append("<li>none</li>")
		}
		sb.append("</ul></pre>${BREAK}")
	}

	protected String getGroupName(VisualizerInfo visInfo = null, String cubeName = targetCube.name)
	{
		return targetCube.hasRuleAxis() ? RULE_NCUBE : NCUBE
	}

	protected static String getDotSuffix(String value)
	{
		int lastIndexOfDot = value.lastIndexOf('.')
		return lastIndexOfDot == -1 ? value : value.substring(lastIndexOfDot + 1)
	}

	protected static String getDotPrefix(String value) {
		int indexOfDot = value.indexOf('.')
		return indexOfDot == -1 ? value : value.substring(0, value.indexOf('.'))
	}

	/**
	 *  If the required scope keys have not already been loaded for this cube,
	 *  load them.
	 */
	protected void addRequiredScopeKeys(VisualizerInfo visInfo)
	{
		String cubeName = targetCube.name
		if (!visInfo.requiredScopeKeysByCube.containsKey(cubeName))
		{
			visInfo.requiredScopeKeysByCube[cubeName] = requiredScope
		}
	}

	protected Map<String, Object> createEdge(Long edgeId)
	{
		String sourceFieldName = sourceFieldName
		Map<String, Object> edge = [:]
		edge.id = edgeId
		edge.from = String.valueOf(sourceId)
		edge.to = String.valueOf(targetId)
		edge.fromName = getLabel(sourceCube.name)
		edge.toName = getLabel(targetCube.name)
		edge.fromFieldName = sourceFieldName
		edge.sourceTrail = sourceTrail
		edge.level = String.valueOf(targetLevel)
		edge.title = sourceFieldName
		return edge
	}

	protected Map<String, Object> createNode(VisualizerInfo visInfo, String group = null)
	{
		NCube targetCube = targetCube
		String targetCubeName = targetCube.name
		String sourceCubeName = sourceCube ? sourceCube.name : null
		String sourceFieldName = sourceFieldName

		Map<String, Object> node = [:]
		node.id = String.valueOf(targetId)
		node.level = String.valueOf(targetLevel)
		node.cubeName = targetCubeName
		node.sourceCubeName = sourceCubeName
		node.sourceTrail = sourceTrail
		node.sourceId = sourceId
		node.sourceScope = sourceScope
		node.scope = targetScope
		node.availableScope = availableTargetScope
		node.availableScopeValues = availableScopeValues
		node.scopeCubeNames = scopeCubeNames
		node.fromFieldName = sourceFieldName
		node.sourceDescription = sourceCubeName ? sourceDescription : null
		node.title = getCubeDisplayName(targetCubeName)
		group = group ?: getGroupName(visInfo)
		node.group = group
		node.showCellValuesLink = showCellValuesLink
		node.showCellValues = showCellValues
		node.cubeLoaded = cubeLoaded
		String label = getLabel(targetCubeName)
		node.label = nodeLabelPrefix + label
		node.detailsTitle1 = cubeDetailsTitle1
		node.detailsTitle2 = getCubeDetailsTitle2(label)
		if (node.detailsTitle1 == node.detailsTitle2){
			node.detailsTitle2 = null
		}
		node.typesToAdd = visInfo.getTypesToAdd(group)

		if (targetId == visInfo.selectedNodeId)
		{
			node.details = getDetails(visInfo)
		}
		return node
	}

	protected void populateScopeDefaults(VisualizerInfo visInfo) {}

	protected void addNodeScope(String cubeName, String scopeKey, boolean optional = false, boolean skipAvailableScopeValues = false, Map coordinate = null)
	{
		availableScopeValues = availableScopeValues ?: new CompactCILinkedMap<String, Set<Object>>()
		scopeCubeNames = scopeCubeNames ?: new CompactCILinkedMap<String, Set<String>>()
		addAvailableValues(cubeName, scopeKey, optional, skipAvailableScopeValues, coordinate)
		addCubeNames(scopeKey, cubeName)
	}

	private void addAvailableValues(String cubeName, String scopeKey, boolean optional, boolean skipAvailableScopeValues, Map coordinate)
	{
		Set<Object> scopeValues = availableScopeValues[scopeKey] as Set ?: new LinkedHashSet()
		if (skipAvailableScopeValues)
		{
			availableScopeValues[scopeKey] = scopeValues
		}
		else
		{
			Set scopeValuesThisCube = getColumnValues(cubeName, scopeKey, coordinate)
			if (availableScopeValues.containsKey(scopeKey))
			{
				if (optional)
				{
					availableScopeValues[scopeKey].addAll(scopeValuesThisCube)
				}
				else
				{
					availableScopeValues[scopeKey] = scopeValues.intersect(scopeValuesThisCube) as Set
				}
			}
			else
			{
				availableScopeValues[scopeKey] = scopeValuesThisCube
			}
		}
	}

	private void addCubeNames(String scopeKey, String valueToAdd)
	{
		Set<String> values = scopeCubeNames[scopeKey] as Set ?: new LinkedHashSet()
		values << valueToAdd
		scopeCubeNames[scopeKey] = values
	}

	protected Set<Object> getColumnValues(String cubeName, String axisName, Map coordinate)
	{
		NCube cube = runtimeClient.getCube(appId, cubeName)
		return getAllColumnValues(cube, axisName)
	}

	protected static Set<Object> getAllColumnValues(NCube cube, String axisName)
	{
		Set values = new LinkedHashSet()
		Axis axis = cube.getAxis(axisName)
		if (axis)
		{
			for (Column column : axis.columns)
			{
				values.add(column.value)
			}
		}
		return values
	}

	protected String createNodeDetailsScopeMessage(VisualizerInfo visInfo)
	{
		availableScopeValues =  availableScopeValues ?: new CompactCILinkedMap<String, Set<Object>> ()
		scopeCubeNames = scopeCubeNames ?: new CompactCILinkedMap<String, Set<String>> ()

		StringBuilder sb = new StringBuilder()
		sb.append(nodeDetailsMessageSet)
		sb.append(getNodeScopeMessage(visInfo))
		return sb.toString()
	}

	private StringBuilder getNodeDetailsMessageSet()
	{
		StringBuilder sb = new StringBuilder()
		nodeDetailsMessages.each {String message ->
			sb.append("${message}")
		}
		return sb
	}

	private StringBuilder getNodeScopeMessage(VisualizerInfo visInfo)
	{
		String nodeName = getLabel()
		StringBuilder sb = new StringBuilder()
		sb.append("<b>Available scope</b>")
		sb.append(BREAK)

		Set<String> scopeKeys = new CaseInsensitiveSet(availableTargetScope.keySet())
		scopeKeys.addAll(availableScopeValues.keySet())
		if (scopeKeys)
		{
			List<String> sortedScopeKeys = scopeKeys as List
			Collections.sort(sortedScopeKeys, String.CASE_INSENSITIVE_ORDER)
			sortedScopeKeys.each { String scopeKey ->
				Set<String> cubeNames = scopeCubeNames[scopeKey]
				Set<Object> availableValues = availableScopeValues[scopeKey]
				StringBuilder title = new StringBuilder()
				String nodeLabel = visInfo.nodeLabel
				boolean disabled = false
				if (availableValues == null)
				{
					if (isDerivedScopeKey(visInfo, scopeKey))
					{
						title.append("Scope key ${scopeKey} was added by the visualizer and may not be changed for this ${nodeLabel} in this visualization.")
						title.append("\nStart a new visual from here to access all scope keys for the ${nodeLabel}.")
						disabled = true
					}
					else
					{
						title.append("Scope key ${scopeKey} was added for a source ${nodeLabel} of this ${nodeLabel}, but is not used by this ${nodeLabel}.")
						title.append("\nAccess a source ${nodeLabel} that uses the scope key in order to see a list of available scope values.")
					}
				}
				else
				{
					cubeNames.remove(null)
					String cubeNamesTitle = getCubeNamesTitle(availableValues.contains(null))
					String requiredOrOptional = availableValues.contains(null) ? 'optional' : 'required'
					title.append("Scope key ${scopeKey} is ${requiredOrOptional} to load ${nodeName}. ")
					title.append(addCubeNamesList(cubeNamesTitle, cubeNames))
				}
				sb.append(getScopeMessage(scopeKey, availableValues, title, availableTargetScope[scopeKey], disabled))
			}
		}
		else
		{
			sb.append(BREAK)
			sb.append("<b>No scope</b>")
			sb.append(BREAK)
		}
		sb.append(BREAK)
		return sb
	}

	private static getCubeNamesTitle(boolean optional)
	{
		return optional ? 'The scope key was encountered on the following cubes:' : 'The scope key was first encountered on the following cubes, but may also be required on others:'
	}

	protected StringBuilder getScopeMessage(String scopeKey, Set<Object> availableScopeValues, StringBuilder title, Object providedScopeValue, boolean disabled)
	{
		StringBuilder sb = new StringBuilder()
		String caret = availableScopeValues ? """<span class="caret"></span>""" : ''
		String placeHolder = availableScopeValues ? 'Select or enter value...' : 'Enter value...'
		String topNodeClass = targetId == 1l ? DETAILS_CLASS_TOP_NODE : ''
		Object value = getValue(availableScopeValues, providedScopeValue)
		String valueClass = getClassForValue(availableScopeValues, value, providedScopeValue)
		String disabledAttribute = disabled ? 'disabled="disabled"' : ''

		sb.append("""<div class="input-group input-group-sm input-scope-group" title="${title}">""")
		sb.append("""<div class="input-group-btn">""")
		sb.append("""<button type="button" class="btn btn-default dropdown-toggle"  data-toggle="dropdown">${scopeKey} ${caret}</button>""")
		if (availableScopeValues)
		{
			sb.append("""<ul class="dropdown-menu">""")
			availableScopeValues.each {Object scopeValue ->
				if (scopeValue)
				{
					sb.append("""<li id="${scopeKey}: ${scopeValue}" class="${DETAILS_CLASS_SCOPE_CLICK} ${topNodeClass}" style="color: black;">${scopeValue}</li>""")
				}
				else
				{
					sb.append("""<li id="${scopeKey}: Default" class="${DETAILS_CLASS_SCOPE_CLICK} ${topNodeClass}" style="color: black;">Default</li>""")
				}
			}
			sb.append("""</ul>""")
		}
		sb.append("""</div>""")
		sb.append("""<input id="${scopeKey}" value="${value}" ${disabledAttribute} placeholder="${placeHolder}" class="${DETAILS_CLASS_SCOPE_INPUT} ${DETAILS_CLASS_FORM_CONTROL} ${valueClass} ${topNodeClass}" style="color: black;" type="text">""")
		sb.append("""</div>""")
		return sb
	}

	private static Object getValue(Set<Object> availableScopeValues, Object providedScopeValue)
	{
		if (availableScopeValues && availableScopeValues.contains(null))
		{
			return providedScopeValue ?: DEFAULT
		}
		else
		{
			return providedScopeValue ?: ''
		}
	}

	private static String getClassForValue(Set<Object> availableScopeValues, Object value, Object providedScopeValue)
	{
		if (null == availableScopeValues)
		{
			return ''
		}
		else if (availableScopeValues.contains(null))
		{
			if (DEFAULT == value || !availableScopeValues.contains(providedScopeValue))
			{
				return DETAILS_CLASS_DEFAULT_VALUE
			}
		}
		else if (!providedScopeValue)
		{
			return DETAILS_CLASS_MISSING_VALUE
		}
		else if (providedScopeValue && availableScopeValues && !availableScopeValues.contains(providedScopeValue))
		{
			return DETAILS_CLASS_MISSING_VALUE
		}
		return ''
	}

	protected void setLoadAgain(VisualizerInfo visInfo, String scopeKey)
	{
		loadAgain = false
	}

	private static StringBuilder addCubeNamesList(String prefix, Set<String> cubeNames)
	{
		StringBuilder sb = new StringBuilder()
		if (cubeNames)
		{
			sb.append("${prefix}\n")
			cubeNames.each { String cubeName ->
				sb.append("  - ${cubeName}\n")
			}
		}
		return sb
	}

	protected boolean isDerivedScopeKey(VisualizerInfo visInfo, String scopeKey)
	{
		return true
	}

	protected String getLabel(String cubeName = targetCube.name)
	{
		cubeName
	}

	protected String getCubeDisplayName(String cubeName)
	{
		return cubeName
	}

	protected String getSourceDescription()
	{
		return sourceCube.name
	}

	protected String getCubeDetailsTitle1()
	{
		return targetCube.name
	}

	protected String getCubeDetailsTitle2(String label)
	{
		return null
	}
}