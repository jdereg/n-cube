package com.cedarsoftware.ncube

import com.google.common.base.Splitter
import groovy.transform.CompileStatic

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeClient
import static com.cedarsoftware.util.StringUtilities.isEmpty

/**
 * This class loads an reference axis.  This entails loading the cube containing
 * the referenced axis, grabbing the axis columns from this cube, passing the
 * columns to an [optional] transformer cube, and then ultimately building the
 * axis from these columns.
 *
 * The source axis type, valueType, and meta properties are maintained.  Any
 * meta-properties on the referencing axis, are added to the meta-properties
 * from the referenced axis.  If both have the same key, the referring axis
 * meta-properties take priority.
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
class ReferenceAxisLoader implements Axis.AxisRefProvider
{
    public static final String REF_TENANT = 'referenceTenant'
    public static final String REF_APP = 'referenceApp'
    public static final String REF_VERSION = 'referenceVersion'
    public static final String REF_STATUS = 'referenceStatus'
    public static final String REF_BRANCH = 'referenceBranch'
    public static final String REF_CUBE_NAME = 'referenceCubeName'
    public static final String REF_AXIS_NAME = 'referenceAxisName'

    public static final String TRANSFORM_APP = 'transformApp'
    public static final String TRANSFORM_VERSION = 'transformVersion'
    public static final String TRANSFORM_STATUS = 'transformStatus'
    public static final String TRANSFORM_BRANCH = 'transformBranch'
    public static final String TRANSFORM_CUBE_NAME = 'transformCubeName'

    private String cubeName
    private String axisName
    private Map<String, Object> args

    /**
     * @param containingCubeName String name of cube that holds the 'pointer' (referring) axis
     * @param sourceAxisName String name of the referring axis
     * @param args Map containing all the key-value pairs to describe the referenced n-cube + axis
     * and an optional reference to a transformation cube.<br>
     * required keys: sourceTenant, sourceApp, sourceVersion, sourceStatus, sourceBranch, referenceCubeName, referenceAxisName
     * optional keys (transformer): transformApp, transformVersion, transformStatus, transformBranch, transformCubeName
     */
    ReferenceAxisLoader(String containingCubeName, String sourceAxisName, Map<String , Object> args)
    {
        cubeName = containingCubeName
        axisName = sourceAxisName
        this.args = args
    }

    /**
     * Axis calls this method to load itself from this AxisRefProvider.
     * @param axis Axis to 'fill-up' from the reference and 'transform' from the optional transformer.
     */
    void load(Axis axis)
    {
        for (Map.Entry<String, Object> entry : args.entrySet())
        {
            axis.setMetaProperty(entry.key, entry.value)
        }

        NCube refCube = getReferencedCube(axis, args[REF_CUBE_NAME] as String, args[REF_AXIS_NAME] as String)
        NCube transformCube = null

        if (axis.referenceTransformed)
        {
            transformCube = getTransformCube(axis, args[TRANSFORM_CUBE_NAME] as String)
        }

        Axis refAxis = getReferencedAxis(refCube, args[REF_AXIS_NAME] as String, axis)

        // Allow the transformer access to the referenced n-cube (from which they can get ApplicationID and another reference axis for combining)
        axis.name = axisName
        axis.type = refAxis.type
        axis.valueType = refAxis.valueType
        axis.fireAll = refAxis.fireAll
        List<Column> columns

        if (transformCube != null)
        {   // Allow this cube to manipulate the passed in Axis.
            ensureTransformCubeIsCorrect(transformCube, axis)
            columns = transform(transformCube, refAxis.columnsWithoutDefault, axis)
        }
        else
        {
            columns = refAxis.columnsWithoutDefault
        }

        // Bring over referenced axis meta properties
        for (Map.Entry<String, Object> entry : refAxis.metaProperties.entrySet())
        {
            if (axis.getMetaProperty(entry.key) == null)
            {   // only override properties not already set.
                axis.setMetaProperty(entry.key, entry.value)
            }
        }

        // Bring over columns
        for (Column column : columns)
        {
            axis.addColumn(column)
        }
    }

    private List<Column> transform(NCube transformCube, List<Column> columns, Axis axis)
    {
        transformCube.getAxis('transform').columnsWithoutDefault.each { Column column ->
            def typeCell = transformCube.getCellNoExecute([transform: column.value, property: 'type'])
            if (!(typeCell instanceof String) || isEmpty(typeCell as String))
            {
                throw new IllegalArgumentException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Please enter a String for type in transform id: ${column.value}.""")
            }

            String type = (typeCell as String).toLowerCase()
            def valueCell = transformCube.getCellNoExecute([transform: column.value, property: 'value'])
            
            if (!(valueCell instanceof String) || isEmpty(valueCell as String))
            {
                throw new IllegalArgumentException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Please enter a String for value in transform id: ${column.value}.""")
            }
            
            String value = valueCell as String
            List<String> values = Splitter.on(',').trimResults().splitToList(value)
            
            switch (type)
            {
                case 'add':
                    if (values.size() > 1)
                    {
                        throw new IllegalArgumentException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Transform type 'add' only supports adding one column at a time in transform id: ${column.value}.""")
                    }
                    Comparable newValue = Axis.promoteValue(axis.valueType, values[0])
                    columns.add(new Column(newValue, Integer.MAX_VALUE - (column.value as Long)))
                    break

                case 'remove':
                    values.each { String val ->
                        Comparable newValue = Axis.promoteValue(axis.valueType, val)
                        Column columnToRemove = columns.find { it.value == newValue }
                        columns.remove(columnToRemove)
                    }
                    break

                case 'subset':
                    Set<Column> columnsToKeep = new LinkedHashSet<>()
                    columns.each { Column col ->
                        for (String val : values)
                        {
                            Comparable newValue = Axis.promoteValue(axis.valueType, val)
                            if (col.value == newValue)
                            {
                                columnsToKeep.add(col)
                                break
                            }
                        }
                    }
                    columns = columnsToKeep as List
                    break

                case 'addaxis':
                    if (values.size() != 4)
                    {
                        throw new IllegalArgumentException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Transform type addAxis must have a value with format 'app name, version, cube name, axis name' in transform id: ${column.value} found: ${value}.""")
                    }
                    ApplicationID appId
                    try
                    {
                        appId = new ApplicationID(transformCube.applicationID.tenant, values[0], values[1], ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)
                    }
                    catch (Exception e)
                    {
                        throw new IllegalStateException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Transform type addAxis must use a valid version and app name. Invalid version: ${values[1]} or app name: ${values[0]} found.""", e)
                    }
                    NCube ncube = ncubeClient.getCube(appId, values[2])
                    if (ncube == null)
                    {
                        throw new IllegalStateException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Transform type addAxis must reference an existing ncube.  Could not find cube: ${values[2]}, appId: {$appId}""")
                    }
                    Axis cubeAxis = ncube.getAxis(values[3])
                    if (cubeAxis == null)
                    {
                        throw new IllegalStateException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Transform type addAxis could not find axis: ${values[3]}""")
                    }
                    columns.addAll(cubeAxis.columnsWithoutDefault)
                    break
                
                default:
                    throw new IllegalArgumentException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)}. Transform type must be one of [add, remove, subset, addAxis] in transform id: ${column.value} found: ${type}.""")
            }
        }
        return columns
    }

    private boolean ensureTransformCubeIsCorrect(NCube transformCube, Axis axis)
    {
        Axis property = transformCube.getAxis('property')
        Axis transform = transformCube.getAxis('transform')
        boolean incorrectShape = (transformCube.numDimensions != 2 ||
                property == null ||
                transform == null ||
                property.type != AxisType.DISCRETE ||
                transform.type != AxisType.DISCRETE ||
                property.valueType != AxisValueType.STRING ||
                transform.valueType != AxisValueType.LONG)
        if (incorrectShape) {
            throw new IllegalArgumentException("""${getFailMessage(axis.name)} \
It referenced axis: ${getReferencedAxisInfo(axis)} using transform n-cube: \
${getTransformInfo(axis)} which must have two DISCRETE axes: transform (LONG) and property (STRING)""")
        }
        return true
    }

    private Axis getReferencedAxis(NCube refCube, String refAxisName, Axis axis)
    {
        Axis refAxis = refCube.getAxis(refAxisName)
        if (refAxis == null)
        {
            throw new IllegalStateException("""\
Unable to load n-cube: ${cubeName}, which has a reference axis: ${axis.name}, \
but referenced axis: ${refAxisName} was not found on the referenced \
n-cube: ${refCube.name}, referenced app: ${axis.referencedApp}""")
        }
        return refAxis
    }

    private NCube getTransformCube(Axis axis, String transformCubeName)
    {
        NCube transformCube = ncubeClient.getCube(axis.transformApp, axis.getMetaProperty(TRANSFORM_CUBE_NAME) as String)
        if (transformCube == null)
        {
            throw new IllegalStateException("""\
Unable to load n-cube: ${cubeName}, which has a reference axis: ${axis.name}. \
Failed to load transform n-cube: ${transformCubeName}, transform app: ${axis.transformApp}""")
        }
        return transformCube
    }

    private NCube getReferencedCube(Axis axis, String refCubeName, String refAxisName)
    {
        NCube refCube = ncubeClient.getCube(axis.referencedApp, axis.getMetaProperty(REF_CUBE_NAME) as String)
        if (refCube == null)
        {
            throw new IllegalStateException("""\
Unable to load n-cube: ${cubeName}, which has a reference axis: ${axis.name}. \
Failed to load referenced n-cube: ${refCubeName}, axis: ${refAxisName}, referenced app: ${axis.referencedApp}""")
        }
        return refCube
    }

    private String getFailMessage(String axisName)
    {
        return "Unable to load n-cube: ${cubeName}, which has a reference axis: ${axisName}."
    }

    private static String getReferencedAxisInfo(Axis axis)
    {
        return "${axis.referencedApp}${axis.referenceCubeName}/${axis.referenceAxisName}"
    }

    private static String getTransformInfo(Axis axis)
    {
        return "${axis.transformApp}${axis.transformCubeName}"
    }
}