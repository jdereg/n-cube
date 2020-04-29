package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.util.CompactCILinkedMap
import com.google.common.base.Joiner
import groovy.transform.CompileStatic

/**
 * Provides information to visualize n-cubes.
 */

@CompileStatic
class Visualizer
{
	protected ApplicationID appId
	protected Set<String> visited = []
	protected Deque<VisualizerRelInfo> stack = new ArrayDeque<>()
	protected Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(": ")
	protected VisualizerHelper helper
	protected static NCubeRuntimeClient runtimeClient

	Visualizer(NCubeRuntimeClient runtimeClient)
	{
		this.runtimeClient = runtimeClient
	}

	/**
	 * Builds the graph for an n-cube and all its referenced n-cubes.
	 *
	 * @param applicationID
	 * @param options - a map containing:
	 *           VisualizerInfo visInfo, information about the visualization
	 *           String startCubeName, name of the starting cube
	 *           Map scope, the scope used in the visualization
	 * @return   VisualizerInfo visInfo, information about the visualization
	 */
	Map<String, Object> loadGraph(ApplicationID applicationID, Map options)
	{
		appId = applicationID
		String startCubeName = options.startCubeName as String
		VisualizerInfo visInfo = getVisualizerInfo(options)
		visInfo.init(options)

		if (!isValidStartCube(visInfo, startCubeName))
		{
			return [visInfo: visInfo] as Map
		}

		VisualizerRelInfo relInfo = visualizerRelInfo
		relInfo.init(options, visInfo)

		getVisualization(visInfo, relInfo)
		return [visInfo: visInfo] as Map
	}

	/**
	 * Re-builds the section of the graph affected by a scope change to an n-cube.
	 *
	 * @param applicationID
	 * @param options - a map containing:
	 *           VisualizerInfo visInfo, information about the visualization
	 *           String startCubeName, name of the starting cube
	 * @return   VisualizerInfo visInfo, information about the visualization
	 */
	Map<String, Object> loadScopeChange(ApplicationID applicationID, Map options)
	{
		appId = applicationID
		VisualizerInfo visInfo = getVisualizerInfo(options)
		Map selectedNode = new LinkedHashMap(visInfo.nodes[visInfo.selectedNodeId])
		visInfo.initScopeChange()
		VisualizerRelInfo relInfo = visualizerRelInfo
		relInfo.initSelectedNode(visInfo, selectedNode)
		getVisualization(visInfo, relInfo)
		return [visInfo: visInfo] as Map
	}

	/**
	 * Loads graph details for an n-cube given the scope provided.
	 *
	 * @param  applicationID
	 * @param  options - a map containing:
	 *            VisualizerInfo visInfo, information about the visualization
	 *            String startCubeName, name of the starting cube
	 * @return VisualizerInfo visInfo, information about the visualization
	 */
	Map<String, Object> loadNodeDetails(ApplicationID applicationID, Map options)
	{
		appId = applicationID
		VisualizerInfo visInfo = options.visInfo as VisualizerInfo
		Map selectedNode = visInfo.nodes[visInfo.selectedNodeId]
		visInfo.inputScope = selectedNode.availableScope as CompactCILinkedMap
		VisualizerRelInfo relInfo = visualizerRelInfo
		relInfo.initSelectedNode(visInfo, selectedNode)
		return loadNodeDetails(visInfo, relInfo, selectedNode)
	}

	protected static Map<String, Object> loadNodeDetails(VisualizerInfo visInfo, VisualizerRelInfo relInfo, Map node)
	{
		visInfo.messages = new LinkedHashSet()
		relInfo.loadCube(visInfo)
		node.details = relInfo.getDetails(visInfo)
		node.showCellValuesLink = relInfo.showCellValuesLink
		node.cubeLoaded = relInfo.cubeLoaded
		node.showCellValues = relInfo.showCellValues
		node.scope = relInfo.targetScope
		node.availableScope = relInfo.availableTargetScope
		node.availableScopeValues = relInfo.availableScopeValues
		node.scopeCubeNames = relInfo.scopeCubeNames
		return [visInfo: visInfo] as Map
	}

	protected VisualizerInfo getVisualizerInfo(Map options)
	{
		VisualizerInfo visInfo = options.visInfo as VisualizerInfo
		if (!visInfo || visInfo.class.name != this.class.name)
		{
			visInfo = new VisualizerInfo(runtimeClient, appId)
		}
		else
		{
			visInfo.appId = appId
		}
		return visInfo
	}

	protected VisualizerRelInfo getVisualizerRelInfo()
	{
		return new VisualizerRelInfo(runtimeClient, appId)
	}

	private void getVisualization(VisualizerInfo visInfo, VisualizerRelInfo relInfo)
	{
		stack.push(relInfo)

		while (!stack.empty)
		{
			processCube(visInfo, stack.pop())
		}

		visInfo.calculateAggregateInfo()

	}

	protected void processCube(VisualizerInfo visInfo, VisualizerRelInfo relInfo)
	{
		NCube targetCube = relInfo.targetCube
		String targetCubeName = targetCube.name

		if (relInfo.sourceCube)
		{
			Long edgeId = visInfo.edgeIdCounter += 1
			visInfo.edges[edgeId] = relInfo.createEdge(edgeId)
		}

		if (!visited.add(targetCubeName + relInfo.availableTargetScope.toString()))
		{
			return
		}

		visInfo.nodes[relInfo.targetId] = relInfo.createNode(visInfo)

		Map<Map, Set<String>> refs = targetCube.referencedCubeNames

		refs.each {Map coordinates, Set<String> cubeNames ->
			cubeNames.each { String cubeName ->
				addToStack(visInfo, relInfo, new VisualizerRelInfo(), cubeName, coordinates)
			}
		}
	}

	protected void addToStack(VisualizerInfo visInfo, VisualizerRelInfo relInfo, VisualizerRelInfo nextRelInfo, String nextTargetCubeName, Map coordinates = [:])
	{
		NCube nextTargetCube = runtimeClient.getCube(appId, nextTargetCubeName)
		if (nextTargetCube)
		{
				nextRelInfo.appId = appId
				long nextTargetLevel = relInfo.targetLevel + 1
				nextRelInfo.targetLevel = nextTargetLevel
				visInfo.nodeIdCounter += 1
				nextRelInfo.targetId = visInfo.nodeIdCounter
				nextRelInfo.targetCube = nextTargetCube
				nextRelInfo.sourceCube = relInfo.targetCube
				nextRelInfo.sourceScope = new CompactCILinkedMap(relInfo.targetScope)
				nextRelInfo.sourceId = relInfo.targetId
				nextRelInfo.sourceTrail = new ArrayList(relInfo.sourceTrail)
		 		nextRelInfo.sourceTrail << relInfo.targetId
				nextRelInfo.sourceFieldName = mapJoiner.join(coordinates)
				nextRelInfo.targetScope = new CompactCILinkedMap(coordinates)
				nextRelInfo.availableTargetScope = new CompactCILinkedMap(relInfo.availableTargetScope)
				nextRelInfo.availableTargetScope.putAll(coordinates)
				nextRelInfo.addRequiredScopeKeys(visInfo)
				nextRelInfo.showCellValuesLink = true
				stack.push(nextRelInfo)
		}
		else
		{
			visInfo.messages << "No cube exists with name of ${nextTargetCubeName}. Cube not included in the visualization.".toString()
		}
	}

	protected VisualizerHelper getVisualizerHelper()
	{
		helper =  new VisualizerHelper(runtimeClient, appId)
	}

	protected NCube isValidStartCube(VisualizerInfo visInfo, String cubeName)
	{
		NCube cube = runtimeClient.getCube(appId, cubeName)
		if (!cube)
		{
			visInfo.messages << "No cube exists with name of ${cubeName} for application id ${appId.toString()}".toString()
		}
		return cube
	}
}