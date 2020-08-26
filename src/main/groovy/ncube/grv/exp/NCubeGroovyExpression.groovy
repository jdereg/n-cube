package ncube.grv.exp

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeAppContext
import com.cedarsoftware.ncube.NCubeInfoDto
import com.cedarsoftware.ncube.NCubeMutableClient
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.ncube.exception.RuleJump
import com.cedarsoftware.ncube.exception.RuleStop
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.TrackingMap
import groovy.transform.CompileStatic

import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_ACTIVE_RECORDS_ONLY
import static com.cedarsoftware.util.StringUtilities.createUTF8String
import static com.cedarsoftware.util.StringUtilities.equalsIgnoreCase
import static com.cedarsoftware.util.UrlUtilities.getContentFromUrl

/**
 * Base class for all GroovyExpression and GroovyMethod's within n-cube CommandCells.
 * @see com.cedarsoftware.ncube.GroovyBase
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
class NCubeGroovyExpression
{
    public Map input
    public Map output
    public NCube ncube
    private static NCubeMutableClient mutableClient = null
    private static boolean legacyNCubeGroovyExpression = false

    /**
     * Fetch the named n-cube from the NCubeRuntime.  It looks at the same
     * account, app, and version as the running n-cube.
     * @param name String n-cube name (optional, defaults to name of currently executing cube).
     * @param quiet boolean (optional, defaults to false).  Set to true if you want null returned
     * when the cube is not found (as opposed to an exception being thrown).
     * @return NCube with the given name.
     */
    NCube getCube(String name = ncube.name, boolean quiet = false)
    {
        if (equalsIgnoreCase(ncube.name, name))
        {
            return ncube
        }
        NCube cube = ncubeRuntime.getCube(ncube.applicationID, name)
        if (cube == null && !quiet)
        {
            throw new IllegalArgumentException("getCube() call within cell attempted, n-cube: ${name} not found.")
        }
        return cube
    }

    /**
     * Short-cut to fetch ApplicationID for current cell.
     */
    ApplicationID getApplicationID()
    {
        return ncube.applicationID
    }

    /**
     * Fetch all cube names in the current application.
     * @return Set<String> cube names>
     */
    Set<String> getCubeNames(boolean activeOnly = true)
    {
        List<NCubeInfoDto> searchResults = ncubeRuntime.search(ncube.applicationID, null, null, [(SEARCH_ACTIVE_RECORDS_ONLY):activeOnly])
        Set<String> names = new CaseInsensitiveSet(searchResults.size())
        searchResults.each { NCubeInfoDto dto -> names.add(dto.name) }
        return names
    }

    /**
     * Fetch cube records that match the given pattern.
     * @param namePattern String text pattern or exact file name used to filter cube name(s)
     * @param textPattern String text pattern filter cubes returned.  This is matched
     * against the JSON content (contains() search).
     * @param options Map of NCubeRuntime.SEARCH_* options. Optional.  Defaults to active records only.
     * @return Object[] of NCubeInfoDto instances.
     */
    List<NCubeInfoDto> search(String namePattern, String textPattern, Map options = [(SEARCH_ACTIVE_RECORDS_ONLY):true])
    {
        List<NCubeInfoDto> dtos = ncubeRuntime.search(ncube.applicationID, namePattern, textPattern, options)
        return dtos
    }

    /**
     * @return Map containing system parameters (e.g. branch, etc.)
     */
    Map<String, Object> getSystemParams()
    {
        return ncubeRuntime.systemParams
    }

    /**
     * Using the input Map passed in, fetch the coordinate at that location.
     * @param coord Map containing precise coordinate to use.
     * @param cubeName String n-cube name.  This argument is optional and defaults
     * to the same cube as the cell currently executing.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.  This argument is optional.
     * @return executed cell contents at the given coordinate.
     */
    def go(Map coord, String cubeName = ncube.name, def defaultValue = null)
    {
        if (coord.is(input))
        {
            coord = dupe(inputWithoutTrackingMap)
        }
        return getCube(cubeName).getCell(coord, output, defaultValue)
    }

    /**
     * Using the input Map passed in, fetch the coordinate at that location.
     * @param coord Map containing precise coordinate to use.
     * @param cube NCube the n-cube to fetch the result from - passed in to save processing time.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.  This argument is optional.
     * @return executed cell contents at the given coordinate.
     */
    def go(Map coord, NCube cube, def defaultValue = null)
    {
        if (coord.is(input))
        {
            coord = dupe(inputWithoutTrackingMap)
        }
        return cube.getCell(coord, output, defaultValue)
    }

    /**
     * Using the input Map passed in, fetch the coordinate at that location.
     * @param coord Map containing precise coordinate to use.
     * @param cubeName String n-cube name.  This argument is optional and defaults
     * to the same cube as the cell currently executing.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.
     * @param ApplicationID of a different application (reference data application for
     * example) from which the running cube exists.
     * @return executed cell contents at the given coordinate.
     */
    def go(Map coord, String cubeName, def defaultValue, ApplicationID appId)
    {
        NCube target = ncubeRuntime.getCube(appId, cubeName)
        if (target == null)
        {
            throw new IllegalArgumentException("go() attempted from cell, n-cube: ${cubeName} not found, app: ${appId}")
        }
        if (coord.is(input))
        {
            coord = dupe(inputWithoutTrackingMap)
        }
        return target.getCell(coord, output, defaultValue)
    }

    /**
     * Fetch the cell contents using the current input coordinate and specified n-cube,
     * but first apply any updates from the passed in coordinate.
     * @param coord Map containing 'updates' to the current input coordinate.
     * @param cubeName String n-cube name.  This argument is optional and defaults
     * to the same cube as the cell currently executing.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.  This argument is optional.
     * @return executed cell contents at the current input location and specified n-cube,
     * but first apply updates to the current input coordinate from the passed in coord.
     */
    def at(Map coord, String cubeName = ncube.name, def defaultValue = null)
    {
        Map copy = inputWithoutTrackingMap
        copy = dupe(copy)
        copy.putAll(coord)
        return getCube(cubeName).getCell(copy, output, defaultValue)
    }

    /**
     * Fetch the cell contents using the current input coordinate and specified n-cube,
     * but first apply any updates from the passed in coordinate.
     * @param coord Map containing 'updates' to the current input coordinate.
     * @param cubeName String n-cube name.  This argument is optional and defaults
     * to the same cube as the cell currently executing.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.  This argument is optional.
     * @return executed cell contents at the current input location and specified n-cube,
     * but first apply updates to the current input coordinate from the passed in coord.
     */
    def at(Map coord, NCube cube, def defaultValue = null)
    {
        Map copy = inputWithoutTrackingMap
        copy = dupe(copy)
        copy.putAll(coord)
        return cube.getCell(copy, output, defaultValue)
    }

    /**
     * Fetch the cell contents using the current input coordinate and specified n-cube,
     * but first apply any updates from the passed in coordinate.
     * @param coord Map containing 'updates' to the current input coordinate.
     * @param cubeName String n-cube name.  This argument is optional and defaults
     * to the same cube as the cell currently executing.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.
     * @param ApplicationID of a different application (reference data application for
     * example) from which the running cube exists.
     * @return executed cell contents at the current input location and specified n-cube,
     * but first apply updates to the current input coordinate from the passed in coord.
     */
    def at(Map coord, String cubeName, def defaultValue, ApplicationID appId)
    {
        NCube target = ncubeRuntime.getCube(appId, cubeName)
        if (target == null)
        {
            throw new IllegalArgumentException("at() attempted from cell, n-cube: ${cubeName} not found, app: ${appId}")
        }
        Map copy = inputWithoutTrackingMap
        copy = dupe(copy)
        copy.putAll(coord)
        return target.getCell(copy, output, defaultValue)
    }

    /**
     * Fetch the cell at location 'altInput', but execute with the current cell location (input).  This
     * API allows you to create reference cells and execute them from anywhere, with the context (input
     * coordinate) of the calling cell.
     * @param altInput Map coordinate of reference cell (often a cell on a default column, e.g [businessUnit:null] as input)
     * The original input is implied, so all that is typically added to altInput, is the necessary changes to input
     * to have it point to another cell.
     * @param cubeName String name of a cube (optional), when pointing to a cell in another n-cube.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.
     * @return value from executing the cell identified by current input map + modifications from altInput.
     */
    def use(Map altInput, String cubeName = ncube.name, def defaultValue = null)
    {
        Map copy = inputWithoutTrackingMap
        Map origInput = new CompactCILinkedMap(copy)
        Map modInput = dupe(copy)
        modInput.putAll(altInput)
        return getCube(cubeName).use(modInput, origInput, output, defaultValue)
    }

    /**
     * Fetch the cell at location 'altInput', but execute with the current cell location (input).  This
     * API allows you to create reference cells and execute them from anywhere, with the context (input
     * coordinate) of the calling cell.
     * @param altInput Map coordinate of reference cell (often a cell on a default column, e.g [businessUnit:null] as input)
     * The original input is implied, so all that is typically added to altInput, is the necessary changes to input
     * to have it point to another cell.
     * @param cubeName String name of a cube (optional), when pointing to a cell in another n-cube.
     * @param defaultValue Object to return when no cell exists at the target coordinate
     * and the cube does not have a cube-level default.
     * @param ApplicationID of a different application, needed only if reference cell resides in another app, for
     * example, an n-cube full of reference cells by design.
     * @return value from executing the cell identified by current input map + modifications from altInput.
     */
    def use(Map altInput, String cubeName, def defaultValue, ApplicationID appId)
    {
        NCube target = ncubeRuntime.getCube(appId, cubeName)
        if (target == null)
        {
            throw new IllegalArgumentException("use() attempted in cell, n-cube: ${cubeName} not found, app: ${appId}")
        }

        Map copy = inputWithoutTrackingMap
        Map origInput = new CompactCILinkedMap(copy)
        Map modInput = dupe(copy)
        modInput.putAll(altInput)
        return target.use(modInput, origInput, output, defaultValue)
    }

    private Map getInputWithoutTrackingMap()
    {
        Map copy = input
        while (copy instanceof TrackingMap)
        {
            copy = ((TrackingMap)copy).getWrappedMap()
        }
        return copy
    }

    private Map dupe(Map map)
    {
        return legacyNCubeGroovyExpression ? map : new CompactCILinkedMap(map)
    }

    /**
     * Filter rows of an n-cube.  Use this API to fetch a subset of an n-cube, similar to SQL SELECT.
     * @param rowAxisName String name of axis acting as the ROW axis.
     * @param colAxisName String name of axis acting as the COLUMN axis.
     * @param where String groovy statement block (or expression) written as condition in terms of the columns on the colAxisName.
     * Example: "(input.state == 'TX' || input.state == 'OH') && (input.attribute == 'fuzzy')".  This will only return rows
     * where this condition is met ('state' and 'attribute' are two column values from the colAxisName).  The values for each
     * row in the rowAxis is bound to the where expression for each row.  If the row passes the 'where' condition, it is
     * included in the output.
     * @param columnsToSearch Set which allows reducing the number of columns bound for use in the where clause.  If not
     * specified, all columns on the colAxisName can be used.  For example, if you had an axis named 'attribute', and it
     * has 10 columns on it, you could list just two (2) of the columns here, and only those columns would be placed into
     * values accessible to the where clause via input.xxx == 'someValue'.  The mapReduce() API runs faster when fewer
     * columns are included in the columnsToSearch.
     * @param columnsToReturn Set of values to indicate which columns to return.  If not specified, the entire 'row' is
     * returned.  For example, if you had an axis named 'attribute', and it has 10 columns on it, you could list just
     * two (2) of the columns here, in the returned Map of rows, only these two columns will be in the returned Map.
     * The columnsToSearch and columnsToReturn can be completely different, overlap, or not be specified.  This param
     * is similar to the 'Select List' portion of the SQL SELECT statement.  It essentially defaults to '*', but you
     * can have it return less column/value pairs in the returned Map if you add only the columns you want returned
     * here.
     * @param cubeName String name of another cube (when the reference is to an n-cube other than 'this' n-cube).  If not
     * specified, the mapReduce() is run against the cube containing 'this' cell.
     * @param appId ApplicationID of another n-cube application.  If not specified, the appId of the n-cube containing
     * 'this' cell is used.
     * @param defaultValue Object placed here will be returned if there is no cell at the location
     *                     pinpointed by the input coordinate.  Normally, the defaulValue of the
     *                     n-cube is returned, but if this parameter is passed a non-null value,
     *                     then it will be returned.  Optional.
     * @return Map of Maps - The outer Map is keyed by the column values of all row columns.  If the row Axis is a discrete
     * axis, then the keys of the map are all the values of the columns.  If a non-discrete axis is used, then the keys
     * are the name meta-key for each column.  If a non-discrete axis is used and there are no name attributes on the columns,
     * and exception will be thrown.  The 'value' associated to the key (column value or column name) is a Map record,
     * where the keys are the column values (or names) for axis named colAxisName.  The associated values are the values
     * for each cell in the same column, for when the 'where' condition holds true (groovy true).
     */
    Map mapReduce(String colAxisName, Closure where = { true }, Map options = [:], String cubeName = null, ApplicationID appId = null)
    {
        NCube target
        if (cubeName != null)
        {
            appId = appId ?: applicationID
            target = ncubeRuntime.getCube(appId, cubeName)
            if (target == null)
            {
                throw new IllegalArgumentException("mapReduce() attempted within cell, but n-cube: ${cubeName} not found in app: ${appId}")
            }
        }
        else
        {
            target = ncube
        }
        options.input = input
        options.output = output
        return target.mapReduce(colAxisName, where, options)
    }

    /**
     * Restart rule execution.  The Map contains the names of rule axes to rule names.  For any rule axis
     * specified in the map, the rule step counter will be moved (jumped) to the named rule.  More than one
     * rule axis step counter can be moved by including multiple entries in the map.
     * @param coord Map of rule axis names, to rule names.  If the map is empty, it is the same as calling
     * jump() with no args.
     */
    void jump(Map coord = [:])
    {
        input.putAll(coord)
        throw new RuleJump(input)
    }

    /**
     * Stop rule execution from going any further.
     */
    void ruleStop()
    {
        throw new RuleStop()
    }

    /**
     * Fetch the Column instance at the location along the axis specified by value.
     * @param axisName String axis name.
     * @param value Comparable value to bind to the axis.
     * @return Column instance at the specified location (value) along the specified axis (axisName).
     */
    Column getColumn(String axisName, Comparable value)
    {
        Axis axis = getAxis(axisName)
        return axis.findColumn(value)
    }

    /**
     * Fetch the Column instance at the location along the axis specified by value within
     * the named n-cube.
     * @param cubeName String n-cube name.
     * @param axisName String axis name.
     * @param value Comparable value to bind to the axis.
     * @return Column instance at the specified location (value) within the specified cube (cubeName)
     * along the specified axis (axisName).
     */
    Column getColumn(String cubeName, String axisName, Comparable value)
    {
        Axis axis = getAxis(cubeName, axisName)
        return axis.findColumn(value)
    }

    /**
     * Fetch the Axis within the current n-cube with the specified name.
     * @param axisName String axis name.
     * @return Axis instance from the current n-cube that has the specified (axisName) name.
     */
    Axis getAxis(String axisName)
    {
        Axis axis = (Axis) ncube.getAxis(axisName)
        if (axis == null)
        {
            throw new IllegalArgumentException("Axis: ${axisName} does not exist on n-cube: ${ncube.name}")
        }

        return axis
    }

    /**
     * Fetch the Axis within the passed in n-cube with the specified name.
     * @param cubeName String n-cube name.
     * @param axisName String axis name.
     * @return Axis instance from the specified n-cube (cubeName) that has the
     * specified (axisName) name.
     */
    Axis getAxis(String cubeName, String axisName)
    {
        Axis axis = (Axis) getCube(cubeName).getAxis(axisName)
        if (axis == null)
        {
            throw new IllegalArgumentException("Axis: ${axisName} does not exist on n-cube: ${cubeName}")
        }

        return axis
    }

    /**
     * Fetch content from the passed in URL.  The URL can be relative or absolute.  If it is
     * relative, then the sys.classpath cube in the same ApplicationID space will be used
     * to anchor it.
     * @param url String URL
     * @return String content fetched from the passed in URL.
     */
    String url(String url)
    {
        byte[] bytes = urlToBytes(url)
        if (bytes == null)
        {
            return null
        }
        return createUTF8String(bytes)
    }

    /**
     * Fetch content from the passed in URL.  The URL can be relative or absolute.  If it is
     * relative, then the sys.classpath cube in the same ApplicationID space will be used
     * to anchor it.
     * @param url String URL
     * @return byte[] content fetched from the passed in URL.
     */
    byte[] urlToBytes(String url)
    {
        URL actualUrl = ncubeRuntime.getActualUrl(applicationID, url, input)
        return getContentFromUrl(actualUrl, true)
    }

    NCubeRuntimeClient getNcubeRuntime()
    {
        return NCubeAppContext.ncubeRuntime
    }

    NCubeMutableClient getMutableClient()
    {
        return NCubeAppContext.ncubeMutableClient
    }

    /**
     * Check whether the 'test' input is between the supplied low and high range.
     * @return true if the 'test' is greater than or equal to the low value, and
     * less than the high value.
     */
    boolean fit(Comparable test, Comparable low, Comparable high)
    {
        return test >= low && test < high
    }

    /**
     * Check whether the 'test' input is between the supplied low and high range.
     * @return true if the 'test' is greater than or equal to the low value, and
     * less than the high value.
     */
    boolean between(Comparable test, Comparable low, Comparable high)
    {
        return test >= low && test < high
    }

    /**
     * @return long Current time in nano seconds (used to compute how long something takes to execute)
     */
    static long now()
    {
        return System.nanoTime()
    }

    /**
     * Get floating point millisecond value for how much time elapsed.
     * @param begin long value from call to now()
     * @param end long value from call to now()
     * @return double elapsed time in milliseconds.
     */
    static double elapsedMillis(long begin, long end)
    {
        return (double) (end - begin) / 1000000.0d
    }

    Object run()
    {
        throw new IllegalStateException("run() should never be called on ${getClass().name}. This can occur for a cell marked GroovyExpression which should be set to GroovyMethod. NCube: ${ncube.name}, input: ${input.toString()}")
    }

    /**
     * If true, "at" type methods on NCubeGroovyExpression will add their coords to their input map.
     * This is legacy behavior that is probably not desirable going forward. We're allowing it for backwards compatibility.
     * @param legacySupport boolean
     */
    static void setLegacyNCubeGroovyExpression(boolean legacySupport)
    {
        legacyNCubeGroovyExpression = legacySupport
    }
}