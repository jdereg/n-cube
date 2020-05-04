package com.cedarsoftware.ncube

import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import groovy.transform.CompileStatic
import it.unimi.dsi.fastutil.longs.Long2LongLinkedOpenHashMap

import static com.cedarsoftware.util.DeepEquals.deepEquals
import static com.cedarsoftware.util.StringUtilities.hasContent

/**
 * This class is used for comparing n-cubes, generating delta objects that
 * describe the difference.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class DeltaProcessor
{
    public static final String DELTA_NCUBE = 'delta-ncube'
    public static final String DELTA_CELLS = 'delta-cel'
    public static final String DELTA_AXES_COLUMNS = 'delta-col'
    public static final String DELTA_AXES = 'delta-axis'
    public static final String DELTA_TESTS = 'delta-test'

    public static final String DELTA_NCUBE_META_PUT = 'ncube-meta-put'
    public static final String DELTA_NCUBE_META_REMOVE = 'ncube-meta-del'

    public static final String DELTA_COLUMN_ADD = 'col-add'
    public static final String DELTA_COLUMN_REMOVE = 'col-del'
    public static final String DELTA_COLUMN_CHANGE = 'col-upd'
    public static final String DELTA_CELL_REMOVE = 'cell-del'

    public static final String DELTA_COLUMN_META_PUT = 'col-meta-put'
    public static final String DELTA_COLUMN_META_REMOVE = 'col-meta-del'

    public static final String DELTA_AXIS_REF_CHANGE = 'axis-ref-changed'
    public static final String DELTA_AXIS_SORT_CHANGED = 'axis-sort-changed'
    public static final String DELTA_AXIS_COLUMNS = 'axis-col-delta'

    public static final String DELTA_AXIS_META_PUT = 'axis-meta-put'
    public static final String DELTA_AXIS_META_REMOVE = 'axis-meta-del'

    /**
     * Fetch the difference between this cube and the passed in cube.  The two cubes must have the same number of axes
     * with the same names.  If those conditions are met, then this method will return a Map with keys for each delta
     * type.
     *
     * The key DELTA_AXES, contains the non-column differences between the axes.  First, it contains a Map of axis
     * name (case-insensitively) to a Map that records the differences.  This map may contain the key
     * DELTA_AXIS_SORT_CHANGED, and if so, the associated value is the new int sort order for the axis.  This map may
     * also contain the key DELTA_AXIS_REF_CHANGE.  If that key is present, then there is a reference axis difference,
     * and all of the keys on the associated Map are the reference axis settings (and transform settings).
     *
     * The key DELTA_AXES_COLUMNS, contains the column differences.  The value associated to this key is a Map, that
     * maps axis name (case-insensitively) to a Map where the key is a column and the associated value is
     * either the 'true' (new) or false (if it should be removed).
     *
     * The key DELTA_CELLS, will have an associated value that is a Map<Set<Long>, T> which are the cell contents
     * that are different.  These cell differences when applied to 'this' will result in this cube's cells matching
     * the passed in 'other'. If the value is NCUBE.REMOVE_CELL, then that indicates a cell that needs to be removed.
     * All other cell values are actual cell value changes.
     *
     * @param baseCube NCube considered the original
     * @param changeCube NCube proposing new changes
     * @return Map containing the proposed differences.  If different number of axes, or different axis names,
     * then null is returned.  There are three (3) keys currently in this map:
     * DeltaProcessor.DELTA_AXES (Map of axis name to List<AxisDelta>)
     * DeltaProcessor.DELTA_AXES_COLUMNS (Map of axis name to Map<Column value, ColumnDelta>)
     * DeltaProcessor.DELTA_CELLS (Map of coordinates to Delta description at coordinate).
     */
    static <T> Map<String, Object> getDelta(NCube<T> baseCube, NCube<T> changeCube)
    {
        Map<String, Object> delta = [:]

        if (!baseCube.isComparableCube(changeCube))
        {
            return null
        }

        Map<String, Map<String, Object>> metaDeltaMap = new CompactCILinkedMap<>()
        delta[DELTA_NCUBE] = metaDeltaMap
        List<Delta> metaChanges = compareMetaProperties(baseCube.metaProperties, changeCube.metaProperties, Delta.Location.NCUBE_META, "ncube: ${changeCube.name}", changeCube.name)
        for (Delta delta1 : metaChanges)
        {
            String propertyName
            String changeType
            if (Delta.Type.DELETE == delta1.type)
            {
                propertyName = (delta1.sourceVal as MapEntry).key as Comparable
                changeType = DELTA_NCUBE_META_REMOVE
            }
            else
            {
                propertyName = (delta1.destVal as MapEntry).key as Comparable
                changeType = DELTA_NCUBE_META_PUT
            }
            metaDeltaMap[propertyName] = [destVal: delta1.destVal, changeType: changeType] as Map
        }

        // Build axis differences
        Map<String, Map<String, Object>> axisDeltaMap = new CompactCILinkedMap<>()
        delta[DELTA_AXES] = axisDeltaMap

        // Build column differences per axis
        Map<String, Map<Comparable, ColumnDelta>> colDeltaMap = new CompactCILinkedMap<>()
        delta[DELTA_AXES_COLUMNS] = colDeltaMap

        for (Axis baseAxis : baseCube.axes)
        {
            Axis changeAxis = changeCube.getAxis(baseAxis.name)
            axisDeltaMap[baseAxis.name] = getAxisDelta(baseAxis, changeAxis)
            colDeltaMap[baseAxis.name] = getColumnDelta(baseAxis, changeAxis)
        }

        // Store updates-to-be-made so that if cell equality tests pass, these can be 'played' at the end to
        // transactionally apply the merge.  We do not want a partial merge.
        delta[DELTA_CELLS] = getCellDelta(baseCube, changeCube)
        delta[DELTA_TESTS] = getTestDeltas(baseCube, changeCube)
        return delta
    }

    /**
     * Merge the passed in cell change-set into this n-cube.  This will apply all of the cell changes
     * in the passed in change-set to the cells of this n-cube, including adds and removes.
     * @param mergeTarget NCube that has a change-set being merged into it.
     * @param deltaSet Map containing cell change-set.  The cell change-set contains cell coordinates
     * mapped to the associated value to set (or remove) for the given coordinate.
     */
    static <T> void mergeDeltaSet(NCube<T> mergeTarget, Map<String, Object> deltaSet)
    {
        // Step 0: Merge ncube-level changes
        Map<String, Map<String, Object>> ncubeDeltas = deltaSet[DELTA_NCUBE] as Map
        ncubeDeltas.each { name, delta ->
            String metaPropName = name as String
            Map<String, Object> ncubeDelta = delta as Map<String, Object>

            if (DELTA_NCUBE_META_PUT == ncubeDelta.changeType)
            {
                mergeTarget.setMetaProperty(metaPropName, (ncubeDelta.destVal as MapEntry).value)
            }
            else if (DELTA_AXIS_META_REMOVE == ncubeDelta.changeType)
            {
                mergeTarget.removeMetaProperty(metaPropName)
            }
        }

        // Step 1: Merge axis-level changes
        Map<String, Map<String, Object>> axisDeltas = deltaSet[DELTA_AXES] as Map
        axisDeltas.each { name, changes ->
            String axisName = name as String
            Map<String, Object> axisChanges = changes as Map<String, Object>

            if (axisChanges.size() > 0)
            {   // There exist changes on the Axis itself, not including possible column changes (sorted, reference, etc)
                Axis axis = mergeTarget.getAxis(axisName)
                if (axisChanges.containsKey(DELTA_AXIS_SORT_CHANGED))
                {
                    axis.columnOrder = axisChanges[DELTA_AXIS_SORT_CHANGED] as int
                }
                for (Object axisChange : axisChanges.values())
                {
                    if (axisChange instanceof AxisDelta)
                    {
                        AxisDelta axisDelta = axisChange as AxisDelta
                        String key = axisDelta.locatorKey
                        if (DELTA_AXIS_META_PUT == axisDelta.changeType)
                        {
                            axis.setMetaProperty(key, axisDelta.axis.getMetaProperty(key))
                        }
                        else if (DELTA_AXIS_META_REMOVE == axisDelta.changeType)
                        {
                            axis.removeMetaProperty(key)
                        }
                    }
                }
            }
        }

        // Step 2: Merge column-level changes
        Map<String, Map<Long, ColumnDelta>> deltaMap = deltaSet[DELTA_AXES_COLUMNS] as Map
        deltaMap.each { name, changes ->
            String axisName = name as String
            Map<String, ColumnDelta> colChanges = changes as Map<String, ColumnDelta>
            Axis axis = mergeTarget.getAxis(axisName)

            if (!axis.reference)
            {
                for (ColumnDelta colDelta : colChanges.values())
                {
                    Column column = colDelta.column
                    if (DELTA_COLUMN_ADD == colDelta.changeType)
                    {
                        Comparable value = axis.getValueToLocateColumn(column)
                        Column findCol = axis.findColumn(value)

                        /**
                         * findCol == null
                         *    1. you have a value not on the axis and there is no default
                         * findCol.default && value != null
                         *    1. default column is being added - skip because default already exists
                         *    2. value not found but landing on default
                         */
                        if (findCol == null || (findCol.default && value != null))
                        {
                            mergeTarget.addColumn(axisName, column.value, column.columnName, column.id)
                            mergeTarget.getAxis(axisName).getColumnById(column.id).addMetaProperties(column.metaProperties)
                        }
                    }
                    else if (DELTA_COLUMN_REMOVE == colDelta.changeType)
                    {
                        Comparable value = axis.getValueToLocateColumn(column)
                        mergeTarget.deleteColumn(axisName, value)
                    }
                    else if (DELTA_COLUMN_CHANGE == colDelta.changeType)
                    {
                        mergeTarget.updateColumn(column.id, column.value)
                    }
                    else if (DELTA_COLUMN_META_PUT == colDelta.changeType)
                    {
                        Comparable value = axis.getValueToLocateColumn(column)
                        Column findCol = axis.findColumn(value)
                        String key = colDelta.locatorKey as String
                        findCol.setMetaProperty(key, column.getMetaProperty(key))
                    }
                    else if (DELTA_COLUMN_META_REMOVE == colDelta.changeType)
                    {
                        Comparable value = axis.getValueToLocateColumn(column)
                        Column findCol = axis.findColumn(value)
                        findCol.removeMetaProperty(colDelta.locatorKey as String)
                    }
                }
            }
        }

        // Step 3: Merge cell-level changes
        Map<Map<String, Object>, T> cellDelta = (Map<Map<String, Object>, T>) deltaSet[DELTA_CELLS]
        // Passed all cell conflict tests, update 'this' cube with the new cells from the other cube (merge)
        cellDelta.each { k, v ->
            Set<Long> cols = deltaCoordToSetOfLong(mergeTarget, k)
            if (cols != null && cols.size() > 0)
            {
                T value = v
                if (DELTA_CELL_REMOVE == value)
                {   // Remove cell
                    mergeTarget.removeCellById(cols)
                }
                else
                {   // Add/Update cell
                    mergeTarget.setCellById(value, cols)
                }
            }
        }

        mergeTarget.clearSha1()
    }

    /**
     * Test the compatibility of two 'delta change-set' maps.  This method determines if these two
     * change sets intersect properly or intersect with conflicts.  Used internally when merging
     * two ncubes together in branch-merge operations.
     *
     * This code is looking at two change sets (A->Base, B->Base).  A is the delta set between the user's branch
     * n-cube and the n-cube the branch (HEAD(7)) was based on. B is the delta set between current HEAD(10) and Base
     * HEAD(7).  Example:
     * Delta set #1 = User's Branch -> HEAD (7)
     * Delta set #2 = Current HEAD (10) -> HEAD (7)
     * The 'headDelta' is the delta-between another person's branch and HEAD when merging between branches.
     * @param branchDelta Map of cell coordinates to values generated from comparing two cubes (A -> B)
     * @param headDelta Map of cell coordinates to values generated from comparing two cubes (A -> C)
     * @param direction = true (HEAD -> branch), false = (branch -> HEAD)
     * @return boolean true if the two cell change-sets are compatible, false otherwise.
     */
    static boolean areDeltaSetsCompatible(Map<String, Object> branchDelta, Map<String, Object> headDelta)
    {
        if (branchDelta == null || headDelta == null)
        {
            return false
        }

        return areNCubeDifferencesOK(branchDelta, headDelta) &&
                areAxisDifferencesOK(branchDelta, headDelta) &&
                areColumnDifferencesOK(branchDelta, headDelta) &&
                areCellDifferencesOK(branchDelta, headDelta) &&
                areTestDifferencesOK(branchDelta, headDelta)
    }

    /**
     * Verify that ncube-level changes are OK.
     * @return true if the ncube level changes between the two change sets are non-conflicting, false otherwise.
     */
    private static boolean areNCubeDifferencesOK(Map<String, Object> branchDelta, Map<String, Object> headDelta)
    {
        Map<String, Object> branchMetaDeltas = branchDelta[DELTA_NCUBE] as Map
        Map<String, Object> headMetaDeltas = headDelta[DELTA_NCUBE] as Map
        for (Map.Entry<String, Object> entry1 : branchMetaDeltas.entrySet())
        {
            String axisName = entry1.key
            Map<String, Object> branchChange = entry1.value as Map
            Map<String, Object> headChange = headMetaDeltas[axisName] as Map

            if (headChange == null)
            {
                continue   // no HEAD meta-property change, branchChange is OK
            }
            if (branchChange.changeType != headChange.changeType)
            {
                return false   // different change type (REMOVE vs ADD, CHANGE vs REMOVE, etc.)
            }
            if((branchChange.destVal as MapEntry).value != (headChange.destVal as MapEntry).value)
            {
                return false
            }
        }
        return true
    }

    /**
     * Verify that axis-level changes are OK.
     * @param reverse = true (HEAD -> branch), false = (branch -> HEAD)
     * @return true if the axis level changes between the two change sets are non-conflicting, false otherwise.
     */
    private static boolean areAxisDifferencesOK(Map<String, Object> branchDelta, Map<String, Object> headDelta)
    {
        Map<String, Object> branchAxisDelta = branchDelta[DELTA_AXES] as Map
        Map<String, Object> headAxisDelta = headDelta[DELTA_AXES] as Map

        if (!ensureAxisNamesAndCountSame(branchAxisDelta.keySet(), headAxisDelta.keySet()))
        {
            return false
        }

        // Column change maps must be compatible
        for (Map.Entry<String, Object> entry1 : branchAxisDelta.entrySet())
        {
            // Note: Not checking for possible (and noted) SORT difference, as that is always a compatible change.
            String axisName = entry1.key
            Map<Comparable, Object> branchChanges = entry1.value as Map
            Map<Comparable, Object> headChanges = headAxisDelta[axisName] as Map

            for (Map.Entry<Comparable, Object> axisEntry : branchChanges.entrySet())
            {
                String axisEntryKey = axisEntry.key
                if (axisEntryKey == DELTA_AXIS_REF_CHANGE)
                {
                    Map<String, Object> branchChange = branchChanges[axisEntryKey] as Map
                    Map<String, Object> headChange = headChanges[axisEntryKey] as Map
                    if (branchChange[DELTA_AXIS_COLUMNS] || headChange[DELTA_AXIS_COLUMNS])
                    {   // Delta marked it as conflicting
                        return false
                    }
                    branchChange.remove(DELTA_AXIS_COLUMNS)
                    headChange.remove(DELTA_AXIS_COLUMNS)
                }
                else if (branchChanges[axisEntryKey] instanceof AxisDelta)
                {
                    AxisDelta branchChange = branchChanges[axisEntryKey] as AxisDelta
                    AxisDelta headChange = headChanges[axisEntryKey] as AxisDelta

                    if (headChange == null)
                    {
                        continue   // no corresponding HEAD change, branchChange is OK
                    }
                    if (branchChange.changeType != headChange.changeType)
                    {
                        return false   // different change type (REMOVE vs ADD, CHANGE vs REMOVE, etc.)
                    }
                    if(branchChange.axis.getMetaProperty(axisEntryKey) != headChange.axis.getMetaProperty(axisEntryKey))
                    {
                        return false
                    }
                }
            }
        }
        return true
    }

    /**
     * Verify that axis-Column changes are OK.
     * @return true if the axis column changes between the two change sets are non-conflicting, false otherwise.
     */
    private static boolean areColumnDifferencesOK(Map<String, Object> branchDelta, Map<String, Object> headDelta)
    {
        Map<String, Map<Comparable, ColumnDelta>> deltaMap1 = branchDelta[DELTA_AXES_COLUMNS] as Map
        Map<String, Map<Comparable, ColumnDelta>> deltaMap2 = headDelta[DELTA_AXES_COLUMNS] as Map

        if (!ensureAxisNamesAndCountSame(deltaMap1.keySet(), deltaMap2.keySet()))
        {
            return false
        }

        // Column change maps must be compatible
        for (Map.Entry<String, Map<Comparable, ColumnDelta>> entry1 : deltaMap1.entrySet())
        {
            String axisName = entry1.key
            // Comparable key in Map below = locatorKey (rule name, rule ID, or valueThatMatches for other Axis Types)
            Map<Comparable, ColumnDelta> changes1 = entry1.value
            Map<Comparable, ColumnDelta> changes2 = deltaMap2[axisName]

            for (Map.Entry<Comparable, ColumnDelta> colEntry1 : changes1.entrySet())
            {
                ColumnDelta delta1 = colEntry1.value
                ColumnDelta delta2 = changes2[delta1.locatorKey]

                if (delta2 == null)
                {
                    continue   // no column changed with same ID, delta1 is OK
                }
                if (delta2.axisType != delta1.axisType)
                {
                    return false   // different axis types
                }
                if (delta1.column.value != delta2.column.value)
                {
                    return false   // value is different for column with same ID
                }
                if (delta1.changeType != delta2.changeType)
                {
                    return false   // different change type (REMOVE vs ADD, CHANGE vs REMOVE, etc.)
                }
                if (delta1.changeType.contains('meta'))
                {
                    String key = delta1.locatorKey as String
                    if (delta1.column.getMetaProperty(key) != delta2.column.getMetaProperty(key))
                    {
                        return false
                    }
                }
            }
        }
        return true
    }

    /**
     * Verify that cell changes are OK between the two change sets.
     * @return true if the cell changes between the two change sets are non-conflicting, false otherwise.
     */
    private static boolean areCellDifferencesOK(Map<String, Object> branchDelta, Map<String, Object> headDelta)
    {
        Map<Map<String, Object>, Object> delta1 = branchDelta[DELTA_CELLS] as Map
        Map<Map<String, Object>, Object> delta2 = headDelta[DELTA_CELLS] as Map
        Map<Map<String, Object>, Object> smallerChangeSet
        Map<Map<String, Object>, Object> biggerChangeSet

        // Performance optimization: determine which cell change set is smaller.
        if (delta1.size() < delta2.size())
        {
            smallerChangeSet = delta1
            biggerChangeSet = delta2
        }
        else
        {
            smallerChangeSet = delta2
            biggerChangeSet = delta1
        }

        for (Map.Entry<Map<String, Object>, Object> entry : smallerChangeSet.entrySet())
        {
            Map<String, Object> deltaCoord = entry.key

            if (biggerChangeSet.containsKey(deltaCoord))
            {
                CellInfo info1 = new CellInfo(entry.value)
                CellInfo info2 = new CellInfo(biggerChangeSet[deltaCoord])

                if (info1 != info2)
                {
                    return false
                }
            }
        }
        return true
    }

    private static boolean areTestDifferencesOK(Map<String, Object> branchDelta, Map<String, Object> headDelta)
    {
        Map<String, Map<String, Delta>> deltaMap1 = branchDelta[DELTA_TESTS] as Map
        Map<String, Map<String, Delta>> deltaMap2 = headDelta[DELTA_TESTS] as Map

        for (Map.Entry<String, Map<String, Delta>> entry1 : deltaMap1.entrySet())
        {
            String testName = entry1.key
            if (!deltaMap2.containsKey(testName))
            {
                continue // no test changed with same ID
            }

            Map<String, Delta> changes2 = deltaMap2[testName]
            for (Map.Entry<String, Delta> changes1 : entry1.value)
            {
                String objName = changes1.key
                if (!changes2.containsKey(objName)) {
                    continue // no coord or assertion changes with same ID
                }

                Delta delta1 = changes1.value
                Delta delta2 = changes2[objName]
                if (!deepEquals(delta1.destVal, delta2.destVal))
                {
                    return false
                }
            }
        }
        return true
    }

    /**
     * Gather difference between two axes as pertaining only to the Axis properties itself, not
     * the associated columns.
     */
    private static Map<String, Object> getAxisDelta(Axis baseAxis, Axis changeAxis)
    {
        Map<String, Object> axisDeltas = [:]

        if (baseAxis.columnOrder != changeAxis.columnOrder)
        {   // If column order changed, set the new column order
            axisDeltas[DELTA_AXIS_SORT_CHANGED] = changeAxis.columnOrder
        }

        Map ref = [:]
        axisDeltas[DELTA_AXIS_REF_CHANGE] = ref
        if (changeAxis.reference != baseAxis.reference)
        {
            ref[DELTA_AXIS_COLUMNS] = true
        }

        List<Delta> metaChanges = compareMetaProperties(baseAxis.metaProperties, changeAxis.metaProperties, Delta.Location.AXIS_META, "axis: ${changeAxis.name}", changeAxis.name)
        for (Delta delta : metaChanges)
        {
            String propertyName
            String changeType
            if (Delta.Type.DELETE == delta.type)
            {
                propertyName = (delta.sourceVal as MapEntry).key as Comparable
                changeType = DELTA_AXIS_META_REMOVE
            }
            else
            {
                propertyName = (delta.destVal as MapEntry).key as Comparable
                changeType = DELTA_AXIS_META_PUT
            }
            axisDeltas[propertyName] = new AxisDelta(changeAxis, propertyName, changeType)
        }

        return axisDeltas
    }

    /**
     * Ensure that the two passed in Maps have the same number of axes, and that the names are the same,
     * case-insensitive.
     * @return true if the key sets are compatible, false otherwise.
     */
    private static boolean ensureAxisNamesAndCountSame(Set<String> axisNames1, Set<String> axisNames2)
    {
        if (axisNames1.size() != axisNames2.size())
        {   // Must have same number of axis (axis name is the outer Map key).
            return false
        }

        Set<String> a1 = new CaseInsensitiveSet<>(axisNames1)
        Set<String> a2 = new CaseInsensitiveSet<>(axisNames2)
        a1.removeAll(a2)
        return a1.empty
    }

    /**
     * Gather the differences between the columns on the two passed in Axes.
     */
    private static Map<Comparable, ColumnDelta> getColumnDelta(Axis baseAxis, Axis changeAxis)
    {
        Map<Comparable, ColumnDelta> deltaColumns = new CompactCILinkedMap<>()
        Map<Comparable, Column> copyColumns = [:]

        for (Column baseColumn : baseAxis.columnsWithoutDefault)
        {
            Comparable locatorKey = baseAxis.getValueToLocateColumn(baseColumn)
            copyColumns[locatorKey] = baseColumn
        }

        for (Column changeColumn : changeAxis.columnsWithoutDefault)
        {
            Comparable locatorKey
            Column foundCol = baseAxis.getColumnById(changeColumn.id)
            if (foundCol == null)
            {
                locatorKey = changeAxis.getValueToLocateColumn(changeColumn)
                foundCol = baseAxis.findColumn(locatorKey)
            }
            else
            {
                locatorKey = baseAxis.getValueToLocateColumn(foundCol)
            }

            // add because you didn't find the column or you landed on the default
            if (foundCol == null || foundCol.default)
            {
                deltaColumns[locatorKey] = new ColumnDelta(baseAxis.type, changeColumn, locatorKey, DELTA_COLUMN_ADD)
            }
            else if (foundCol.value != changeColumn.value)
            {
                deltaColumns[locatorKey] = new ColumnDelta(baseAxis.type, changeColumn, locatorKey, DELTA_COLUMN_CHANGE)
                copyColumns.remove(locatorKey)
            }
            else
            {   // Matched - check for meta-property deltas
                List<Delta> metaChanges = compareMetaProperties(foundCol.metaProperties, changeColumn.metaProperties, Delta.Location.COLUMN_META, 'name', [axis: baseAxis.name, column: new Column(foundCol)])
                for (Delta delta : metaChanges)
                {
                    String propertyName
                    String changeType
                    if (Delta.Type.DELETE == delta.type)
                    {
                        propertyName = (delta.sourceVal as MapEntry).key as Comparable
                        changeType = DELTA_COLUMN_META_REMOVE
                    }
                    else
                    {
                        propertyName = (delta.destVal as MapEntry).key as Comparable
                        changeType = DELTA_COLUMN_META_PUT
                    }
                    deltaColumns[propertyName] = new ColumnDelta(baseAxis.type, changeColumn, propertyName, changeType)
                }
                copyColumns.remove(locatorKey)
            }
        }

        // Columns left over - these are columns 'this' axis has that the 'other' axis does not have.
        for (Column column : copyColumns.values())
        {   // If 'this' axis has columns 'other' axis does not, then mark these to be removed (like we do with cells).
            Comparable locatorKey = changeAxis.getValueToLocateColumn(column)
            deltaColumns[locatorKey] = new ColumnDelta(baseAxis.type, column, locatorKey, DELTA_COLUMN_REMOVE)
        }

        // handle add or remove default column
        if (baseAxis.hasDefaultColumn() && !changeAxis.hasDefaultColumn())
        {
            deltaColumns[null] = new ColumnDelta(baseAxis.type, baseAxis.defaultColumn, null, DELTA_COLUMN_REMOVE)
        }
        else if (!baseAxis.hasDefaultColumn() && changeAxis.hasDefaultColumn())
        {
            deltaColumns[null] = new ColumnDelta(changeAxis.type, changeAxis.defaultColumn, null, DELTA_COLUMN_ADD)
        }

        return deltaColumns
    }

    /**
     * Get all cellular differences between two n-cubes.
     * @param other NCube from which to generate the delta.
     * @return Map containing a Map of cell coordinates [key is Map<String, Object> and value (T)].
     */
    private static <T> Map<Map<String, Object>, T> getCellDelta(NCube<T> thisCube, NCube<T> other)
    {
        Map<Map<String, Object>, T> delta = new LinkedHashMap<>()
        Set<Map<String, Object>> copyCells = new LinkedHashSet<>()

        thisCube.cellMap.each { Set<Long> colIds, T value ->
            copyCells.add(thisCube.getCoordinateFromIds(colIds))
        }

        // At this point, the cubes have the same number of axes and same axis types.
        // Now, compute cell deltas.
        other.cellMap.each { Set<Long> colIds, T value ->
            Map<String, Object> deltaCoord = other.getCoordinateFromIds(colIds)
            Set<Long> idKey = deltaCoordToSetOfLong(other, deltaCoord)
            if (idKey != null)
            {   // Was able to bind deltaCoord between cubes
                T content = thisCube.getCellByIdNoExecute(idKey)
                T otherContent = value
                copyCells.remove(deltaCoord)

                CellInfo info = new CellInfo(content)
                CellInfo otherInfo = new CellInfo(otherContent)

                if (info != otherInfo)
                {
                    delta[deltaCoord] = otherContent
                }
            }
        }

        for (Map<String, Object> coord : copyCells)
        {
            delta[coord] = (T) DELTA_CELL_REMOVE
        }

        return delta
    }

    private static List<Delta> getTestDeltaList(NCube newCube, NCube oldCube)
    {
        List<Delta> deltas = []
        Map<String, Map<String, Delta>> deltaMap = getTestDeltas(newCube, oldCube)
        for (Map.Entry<String, Map<String, Delta>> testDeltaEntry : deltaMap)
        {
            Map<String, Delta> objMap = testDeltaEntry.value
            for (Map.Entry<String, Delta> objDeltaEntry : objMap)
            {
                Delta delta = objDeltaEntry.value
                deltas.add(delta)
            }
        }
        return deltas
    }

    private static Map<String, Map<String, Delta>> getTestDeltas(NCube newCube, NCube oldCube)
    {
        List<NCubeTest> newCubeTests = newCube.testData
        List<NCubeTest> oldCubeTests = oldCube.testData
        Map<String, Map<String, Delta>> deltaMap = new CompactCILinkedMap()
        if (!newCubeTests && !oldCubeTests)
        {
            return deltaMap
        }

        Object[] newCubeTestObj = newCubeTests.toArray()
        Object[] oldCubeTestObj = oldCubeTests.toArray()

        for (NCubeTest newCubeTest : newCubeTests)
        {
            String newCubeTestName = newCubeTest.name
            NCubeTest oldCubeTest = oldCubeTests.find { NCubeTest test -> test.name == newCubeTestName }

            if (oldCubeTest)
            {   // has the same test
                Map<String, Delta> testDeltaMap = new CompactCILinkedMap()

                // coords
                Map<String, CellInfo> oldTestCoords = oldCubeTest.coord
                for (Map.Entry<String, CellInfo> newTestCoordEntry : newCubeTest.coord)
                {
                    String key = newTestCoordEntry.key
                    CellInfo newTestCoord = newTestCoordEntry.value
                    MapEntry newEntry = new MapEntry(key, newTestCoord)
                    if (oldTestCoords.containsKey(key))
                    {   // has the same coord name
                        CellInfo oldTestCoord = oldTestCoords[key]
                        oldTestCoords.remove(key)
                        if (!deepEquals(newTestCoord, oldTestCoord))
                        {   // value changed
                            String s = "Change ${newCubeTestName} coord ${key}: ${oldTestCoord.value} ==> ${newTestCoord.value}"
                            MapEntry oldEntry = new MapEntry(key, oldTestCoord)
                            testDeltaMap[key] = new Delta(Delta.Location.TEST_COORD, Delta.Type.UPDATE, s, newCubeTestName, oldEntry, newEntry, oldCubeTestObj, newCubeTestObj)
                        }
                    }
                    else
                    {   // coord no longer present
                        String s = "Add ${newCubeTestName} coord {${key}: ${newTestCoord.value}}"
                        testDeltaMap[key] = new Delta(Delta.Location.TEST_COORD, Delta.Type.ADD, s, newCubeTestName, null, newEntry, oldCubeTestObj, newCubeTestObj)
                    }
                }
                for (Map.Entry<String, CellInfo> oldTestCoordEntry : oldTestCoords)
                {   // coord added
                    String key = oldTestCoordEntry.key
                    CellInfo value = oldTestCoordEntry.value
                    String s = "Remove ${newCubeTest.name} coord {${key}: ${value.value}}"
                    MapEntry oldEntry = new MapEntry(key, value)
                    testDeltaMap[key] = new Delta(Delta.Location.TEST_COORD, Delta.Type.DELETE, s, newCubeTestName, oldEntry, null, oldCubeTestObj, newCubeTestObj)
                }

                // assertions
                CellInfo[] oldCubeAsserts = oldCubeTest.assertions
                Set<String> checkedAsserts = []
                for (CellInfo newAssert : newCubeTest.assertions)
                {
                    String newVal = newAssert.value
                    CellInfo oldAssert = oldCubeAsserts.find { CellInfo oldAssert ->
                        oldAssert.value == newVal
                    }
                    if (oldAssert)
                    {
                        checkedAsserts.add(newVal)
                        if (!(oldAssert.isUrl == newAssert.isUrl))
                        {
                            String s = "Change assertion ${newVal} ${oldAssert.isUrl ? 'URL' : 'Value'} ==> ${newAssert.isUrl ? 'URL' : 'Value'}"
                            testDeltaMap[newVal] = new Delta(Delta.Location.TEST_ASSERT, Delta.Type.UPDATE, s, newCubeTestName, new CellInfo(oldAssert), new CellInfo(newAssert), oldCubeTestObj, newCubeTestObj)
                        }
                    }
                    else
                    {
                        String s = "Add assertion ${newVal}"
                        testDeltaMap[newVal] = new Delta(Delta.Location.TEST_ASSERT, Delta.Type.ADD, s, newCubeTestName, null, new CellInfo(newAssert), oldCubeTestObj, newCubeTestObj)
                    }
                }
                for (CellInfo oldAssert : oldCubeTest.assertions)
                {
                    String oldVal = oldAssert.value
                    if (!checkedAsserts.contains(oldVal))
                    {
                        String s = "Remove assertion ${oldVal}"
                        testDeltaMap[oldVal] = new Delta(Delta.Location.TEST_ASSERT, Delta.Type.DELETE, s, newCubeTestName, new CellInfo(oldAssert), null, oldCubeTestObj, newCubeTestObj)
                    }
                }

                oldCubeTests.remove(oldCubeTest) // remove to have shortened list
                if (!testDeltaMap.empty)
                {
                    deltaMap[newCubeTestName] = testDeltaMap
                }
            }
            else
            {   // added test
                String s = "Add test ${newCubeTest.name}"
                deltaMap[newCubeTestName] = [(Delta.Type.ADD.name()): new Delta(Delta.Location.TEST, Delta.Type.ADD, s, newCubeTestName, null, newCubeTest, oldCubeTestObj, newCubeTestObj)]
            }
        }

        for (NCubeTest oldCubeTest : oldCubeTests)
        {   // deleted test
            String oldCubeTestName = oldCubeTest.name
            String s = "Remove test ${oldCubeTestName}"
            deltaMap[oldCubeTestName] = [(Delta.Type.DELETE.name()): new Delta(Delta.Location.TEST, Delta.Type.DELETE, s, oldCubeTestName, oldCubeTest, null, oldCubeTestObj, newCubeTestObj)]
        }

        return deltaMap
    }

    /**
     * Return a list of Delta objects describing the differences between two n-cubes.
     * @param oldCube NCube to compare 'this' n-cube to
     * @return List<Delta> object.  The Delta class contains a Location (loc) which describes the
     * part of an n-cube that differs (ncube, axis, column, or cell) and the Type (type) of difference
     * (ADD, UPDATE, or DELETE).  Finally, it includes an English description of the difference.
     * NOTE: this will remove test data from the cube in order to not affect sha1 calculation.
     */
    static List<Delta> getDeltaDescription(NCube newCube, NCube oldCube)
    {
        List<Delta> changes = []

        changes.addAll(getTestDeltaList(newCube, oldCube))
        // remove test data to not affect the cubes
        newCube.removeMetaProperty(NCube.METAPROPERTY_TEST_DATA)
        oldCube.removeMetaProperty(NCube.METAPROPERTY_TEST_DATA)

        getNCubeChanges(newCube, oldCube, changes)

        List<Delta> metaChanges = compareMetaProperties(oldCube.metaProperties, newCube.metaProperties, Delta.Location.NCUBE_META, "n-cube '${newCube.name}'", null)
        changes.addAll(metaChanges)
        Object[] oldAxes = oldCube.axisNames as Object[]
        Object[] newAxes = newCube.axisNames as Object[]
        Set<String> newAxisNames = newCube.axisNames
        Set<String> oldAxisNames = oldCube.axisNames
        newAxisNames.removeAll(oldAxisNames)

        boolean axesChanged = false
        if (!newAxisNames.empty)
        {
            for (String axisName : newAxisNames)
            {
                String s = "Add axis: ${axisName}"
                changes.add(new Delta(Delta.Location.AXIS, Delta.Type.ADD, s, null, null, newCube.getAxis(axisName), oldAxes, newAxes))
            }
            axesChanged = true
        }

        newAxisNames = newCube.axisNames
        oldAxisNames.removeAll(newAxisNames)
        if (!oldAxisNames.empty)
        {
            for (String axisName : oldAxisNames)
            {
                String s = "Remove axis: ${axisName}"
                changes.add(new Delta(Delta.Location.AXIS, Delta.Type.DELETE, s, null, oldCube.getAxis(axisName), null, oldAxes, newAxes))
            }
            axesChanged = true
        }

        // Create Map that maps column IDs from one cube to another (needed when columns are matched by value)
        Map<Long, Long> idMap = new Long2LongLinkedOpenHashMap()

        for (Axis newAxis : newCube.axes)
        {
            Axis oldAxis = oldCube.getAxis(newAxis.name)
            if (oldAxis == null)
            {
                continue
            }
            if (!newAxis.areAxisPropsEqual(oldAxis))
            {
                String s = "Change axis '${oldAxis.name}' properties from ${oldAxis.axisPropString} to ${newAxis.axisPropString}"
                changes.add(new Delta(Delta.Location.AXIS, Delta.Type.UPDATE, s, null, oldAxis, newAxis, oldAxes, newAxes))
            }

            metaChanges = compareMetaProperties(oldAxis.metaProperties, newAxis.metaProperties, Delta.Location.AXIS_META, "axis: ${newAxis.name}", newAxis.name)
            changes.addAll(metaChanges)

            Set<String> oldColNames = new CaseInsensitiveSet<>()
            Set<String> newColNames = new CaseInsensitiveSet<>()
            oldAxis.columns.each { Column oldCol ->
                oldColNames.add(getDisplayColumnName(oldCol))
            }
            newAxis.columns.each { Column newCol ->
                newColNames.add(getDisplayColumnName(newCol))
            }
            Object[] oldCols = oldColNames as Object[]
            Object[] newCols = newColNames as Object[]
            boolean isRef = newAxis.reference
            boolean displayOrderMatters = !isRef && newAxis.columnOrder == Axis.DISPLAY

            List<Column> newColumns = getAllowedColumns(newAxis, isRef)
            boolean columnChanges = false
            boolean needsReorder = false

            for (Column newCol : newColumns)
            {
                Column oldCol = findColumn(newAxis, oldAxis, newCol)
                if (oldCol == null)
                {
                    String colName = newAxis.getDisplayColumnName(newCol)
                    String s = "Add column '${colName}' to axis: ${newAxis.name}"
                    changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.ADD, s, newAxis.name,
                            null, newCol, [] as Object[], [getDisplayColumnName(newCol)] as Object[]))
                    columnChanges = true

                    // If new Column has meta-properties, generate a Delta.COLUMN_META, ADD for each meta-property
                    addMetaPropertiesToColumn(newCol, changes, newAxis)
                }
                else
                {
                    if (newCol.id != oldCol.id && !oldCol.default)
                    {   // If a column has to be found by value, that means its ID changed.  Map the old ID to new ID.
                        // Later, when mapping cells, they will be checked against this Map if their ID is not found.
                        idMap[newCol.id] = oldCol.id
                    }

                    // Check Column meta properties
                    String colName = newAxis.getDisplayColumnName(newCol)
                    metaChanges = compareMetaProperties(oldCol.metaProperties, newCol.metaProperties, Delta.Location.COLUMN_META,
                            "column '${colName}'", [axis: newAxis.name, column: new Column(newCol)])
                    changes.addAll(metaChanges)

                    if (!deepEquals(oldCol.value, newCol.value))
                    {
                        String s = "Change column value from: ${oldCol.value} to: ${newCol.value}"
                        changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.UPDATE, s, newAxis.name,
                                oldCol, newCol, [getDisplayColumnName(oldCol)] as Object[], [getDisplayColumnName(newCol)] as Object[]))
                    }

                    // For non-reference axes, if they are manually ordered (DISPLAY) and the displayOrder field has changed...
                    if (displayOrderMatters && oldCol.displayOrder != newCol.displayOrder)
                    {   // ...create a COLUMN ORDER delta.
                        needsReorder = true
                    }
                }
            }

            if (isRef)
            {
                for (Column newCol : newAxis.columnsWithoutDefault)
                {
                    String colName = newAxis.getDisplayColumnName(newCol)
                    Column oldCol = findColumn(newAxis, oldAxis, newCol)
                    if (oldCol)
                    {
                        metaChanges = compareMetaProperties(oldCol.metaProperties, newCol.metaProperties, Delta.Location.COLUMN_META,
                                "column '${colName}'", [axis: newAxis.name, column: new Column(newCol)])
                        changes.addAll(metaChanges)
                    }
                }
            }

            List<Column> oldColumns = getAllowedColumns(oldAxis, isRef)
            for (Column oldCol : oldColumns)
            {
                Column newCol = findColumn(oldAxis, newAxis, oldCol)
                if (newCol == null)
                {
                    String colName = newAxis.getDisplayColumnName(oldCol)
                    String s = "Remove column '${colName}' from axis: ${oldAxis.name}"
                    changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.DELETE, s, newAxis.name,
                            oldCol, null, [getDisplayColumnName(oldCol)] as Object[], [] as Object[]))
                    columnChanges = true
                }
                else
                {
                    if (oldCol.id != newCol.id && !newCol.default)
                    {   // If a column has to be found by value, that means its ID changed.  Map the old ID to new ID.
                        // Later, when mapping cells, they will be checked against this Map if their ID is not found.
                        idMap[oldCol.id] = newCol.id
                    }
                }
            }

            // For non-reference axes, if they are manually ordered (DISPLAY) and the displayOrder field has changed...
            if (displayOrderMatters && !columnChanges && needsReorder)
            {   // ...create a REORDER columns delta.
                String s = "Column order changed on axis ${newAxis.name}"
                changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.ORDER, s, newAxis.name, null, newAxis.columnsWithoutDefault, oldCols, newCols))
            }
        }

        // Different dimensionality, don't compare cells
        if (axesChanged)
        {
            Collections.sort(changes)
            return changes
        }

        getCellChanges(newCube, oldCube, idMap, changes)
        Collections.sort(changes)
        return changes
    }

    private static String getDisplayColumnName(Column column)
    {
        String value = column.toString()
        return hasContent(column.columnName) ? "${column.columnName}:\n${value}" : value
    }

    private static void addMetaPropertiesToColumn(Column newCol, List<Delta> changes, Axis newAxis)
    {
        if (!newCol.metaProperties.isEmpty())
        {   // Add new column's meta-properties as Deltas
            List<String> newList = []
            newCol.metaProperties.each { String key, Object value ->
                newList.add("${key}: ${value?.toString()}".toString())
            }
            Object[] newMetaList = newList as Object[]
            String colName = newAxis.getDisplayColumnName(newCol)

            for (String key : newCol.metaProperties.keySet())
            {
                Object newVal = newCol.getMetaProperty(key)
                String s = "Add column '${colName}' meta-property {${key}: ${newVal}}"
                MapEntry pair = new MapEntry(key, newVal)
                changes.add(new Delta(Delta.Location.COLUMN_META, Delta.Type.ADD, s,
                        [axis: newAxis.name, column: new Column(newCol)],
                        null, pair, [] as Object[], newMetaList))
            }
        }
    }

    /**
     * Return all the Columns on the passed in Axis, unless the axis is a reference axis,
     * in which case either none are returned or the default column if it has one.
     */
    private static List<Column> getAllowedColumns(Axis axis, boolean isRef)
    {
        List<Column> columns = []
        if (isRef)
        {
            if (axis.hasDefaultColumn())
            {
                columns.add(axis.defaultColumn)
            }
        }
        else
        {
            columns.addAll(axis.columns)
        }
        return columns
    }

    private static void getCellChanges(NCube newCube, NCube oldCube, Map<Long, Long> idMap, List<Delta> changes)
    {
        Map<Set<Long>, Object> cellMap = newCube.cellMap
        cellMap.each { ids, value ->
            Set<Long> colIds = ids as Set<Long>
            Set<Long> coord = adjustCoord(colIds, oldCube.cellMap, idMap)
            if (oldCube.cellMap.containsKey(coord))
            {
                Object oldCellValue = oldCube.cellMap[coord]
                if (!deepEquals(value, oldCellValue))
                {
                    Map<String, Object> properCoord = newCube.getDisplayCoordinateFromIds(colIds)
                    String s = "Change cell at: ${properCoord} from: ${oldCellValue} to: ${value}"
                    changes.add(new Delta(Delta.Location.CELL, Delta.Type.UPDATE, s, coord, new CellInfo(oldCube.getCellByIdNoExecute(coord)), new CellInfo(newCube.getCellByIdNoExecute(colIds)), null, null))
                }
            }
            else
            {
                Map<String, Object> properCoord = newCube.getDisplayCoordinateFromIds(colIds)
                String s = "Add cell at: ${properCoord}, value: ${value}"
                changes.add(new Delta(Delta.Location.CELL, Delta.Type.ADD, s, colIds, null, new CellInfo(newCube.getCellByIdNoExecute(colIds)), null, null))
            }
        }

        Map<Set<Long>, Object> srcCellMap = oldCube.cellMap
        srcCellMap.each { ids, value ->
            Set<Long> colIds = ids as Set<Long>
            Set<Long> coord = adjustCoord(colIds, newCube.cellMap, idMap)
            if (!newCube.cellMap.containsKey(coord))
            {
                boolean allColsStillExist = true
                for (Long colId : colIds)
                {
                    Axis axis = newCube.getAxisFromColumnId(colId)
                    if (axis == null)
                    {
                        allColsStillExist = false
                        break
                    }
                }

                // Make sure all columns for this cell still exist before reporting it as removed.  Otherwise, a
                // dropped column would report a ton of removed cells.
                if (allColsStillExist)
                {
                    Map<String, Object> properCoord = newCube.getDisplayCoordinateFromIds(coord)
                    String s = "Remove cell at: ${properCoord}, value: ${value}"
                    changes.add(new Delta(Delta.Location.CELL, Delta.Type.DELETE, s, colIds, new CellInfo(oldCube.getCellByIdNoExecute(colIds)), null, null, null))
                }
            }
        }
    }

    /**
     * Get changes at the NCUBE level (name, default cell value)
     * @param newCube NCube transmitting the change (instigator)
     * @param oldCube NCube receiving the change
     * @param changes List of Deltas to be added to if needed
     */
    private static void getNCubeChanges(NCube newCube, NCube oldCube, List<Delta> changes)
    {
        if (!newCube.name.equalsIgnoreCase(oldCube.name))
        {
            String s = "Name change from '${oldCube.name}' to '${newCube.name}'"
            changes.add(new Delta(Delta.Location.NCUBE, Delta.Type.UPDATE, s, 'NAME', oldCube.name, newCube.name, null, null))
        }

        if (newCube.defaultCellValue != oldCube.defaultCellValue)
        {
            String s = "Default cell value change from '${CellInfo.formatForDisplay((Comparable) oldCube.defaultCellValue)}' to '${CellInfo.formatForDisplay((Comparable) newCube.defaultCellValue)}'"
            changes.add(new Delta(Delta.Location.NCUBE, Delta.Type.UPDATE, s, 'DEFAULT_CELL', new CellInfo(oldCube.defaultCellValue), new CellInfo(newCube.defaultCellValue), null, null))
        }
    }

    private static Set<Long> adjustCoord(Set<Long> colIds, Map cellMap, Map<Long, Long> idMap)
    {
        // 1st attempt - is it there with the exact same coordinate ids?
        if (cellMap.containsKey(colIds))
        {
            return colIds
        }

        // Is it there with substituted coordinate ids (column was matched by value, so trying the id of THAT column)
        Set<Long> coord = new LinkedHashSet<>()
        Iterator<Long> i = colIds.iterator()
        while (i.hasNext())
        {
            Long id = i.next()
            if (idMap.containsKey(id))
            {
                coord.add(idMap[id])
            }
            else
            {
                coord.add(id)
            }
        }

        return coord
    }

    /**
     * Build List of Delta objects describing the difference between the two passed in Meta-Properties Maps.
     */
    protected static List<Delta> compareMetaProperties(Map<String, Object> oldMeta, Map<String, Object> newMeta, Delta.Location location, String locName, Object helperId)
    {
        List<String> oldList = []
        oldMeta.each { String metaKey, Object value ->
            oldList.add("${metaKey}: ${value?.toString()}".toString())
        }
        List<String> newList = []
        newMeta.each { String metaKey, Object value ->
            newList.add("${metaKey}: ${value?.toString()}".toString())
        }
        Object[] oldMetaList = oldList as Object[]
        Object[] newMetaList = newList as Object[]

        List<Delta> changes = []
        Set<String> oldKeys = new CaseInsensitiveSet<>(oldMeta.keySet())
        Set<String> sameKeys = new CaseInsensitiveSet<>(newMeta.keySet())
        sameKeys.retainAll(oldKeys)

        Set<String> addedKeys  = new CaseInsensitiveSet<>(newMeta.keySet())
        addedKeys.removeAll(sameKeys)
        if (!addedKeys.empty)
        {
            for (String key : addedKeys)
            {
                Object newVal = newMeta[key]
                String s = "Add ${locName} meta-property {${key}: ${newVal}}"
                MapEntry pair = new MapEntry(key, newVal)
                changes.add(new Delta(location, Delta.Type.ADD, s, helperId, null, pair, oldMetaList, newMetaList))
            }
        }

        Set<String> deletedKeys  = new CaseInsensitiveSet<>(oldMeta.keySet())
        deletedKeys.removeAll(sameKeys)
        if (!deletedKeys.empty)
        {
            for (String metaKey: deletedKeys)
            {
                Object oldVal = oldMeta[metaKey]
                String s = "Delete ${locName} meta-property {${metaKey}: ${oldVal}}"
                MapEntry pair = new MapEntry(metaKey, oldVal)
                changes.add(new Delta(location, Delta.Type.DELETE, s, helperId, pair, null, oldMetaList, newMetaList))
            }
        }

        for (String metaKey : sameKeys)
        {
            if (!deepEquals(oldMeta[metaKey], newMeta[metaKey]))
            {
                Object oldVal = oldMeta[metaKey]
                Object newVal = newMeta[metaKey]
                String s = "Change ${locName} meta-property {${metaKey}: ${oldVal}} ==> {${metaKey}: ${newVal}}"
                MapEntry oldPair = new MapEntry(metaKey, oldVal)
                MapEntry newPair = new MapEntry(metaKey, newVal)
                changes.add(new Delta(location, Delta.Type.UPDATE, s, helperId, oldPair, newPair, oldMetaList, newMetaList))
            }
        }

        return changes
    }

    private static Column findColumn(Axis transmitterAxis, Axis receiverAxis, Column transmitterCol)
    {
        Column column = receiverAxis.getColumnById(transmitterCol.id)
        if (column)
        {
            return column
        }

        Comparable locatorKey = transmitterAxis.getValueToLocateColumn(transmitterCol)
        column = receiverAxis.findColumn(locatorKey)
        if (column && column.default)
        {   // && column.default is needed because we are locating by value and landed on the default column.
            return null
        }
        return column
    }

    /**
     * Convert a DeltaCoord to a Set<Long>.  A 'deltaCoord' is a coordinate which has String axis name
     * keys and associated values (to match against standard axes), but for Rule axes it has the Long ID
     * for the associated value.  These deltaCoord's are used during cube merging to allow coordinates from
     * one cube to bind into another cube.
     * @param deltaCoord Map<String, Object> where the String keys are axis names, and the object is the associated
     * value to bind to the axis.  For RULE axes, the associated value is a Long ID (or rule name, or null
     * for default column - note: long ID can be used for default too).
     * @return Set<Long> that can be used with any n-cube API that binds by ID (getCellById, etc.) or null
     * if the deltaCoord could not bind to this n-cube.
     */
    private static <T> Set<Long> deltaCoordToSetOfLong(NCube<T> target, final Map<String, Object> deltaCoord)
    {
        Set<Long> set = new TreeSet<>()
        for (final Axis axis : target.axes)
        {
            final Object value = deltaCoord[axis.name]
            final Column column = axis.findColumn((Comparable) value)
            if (column == null)
            {
                return null
            }
            set.add(column.id)
        }
        return new LinkedHashSet<>(set)
    }
}
