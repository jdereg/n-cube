package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point2D
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.SafeSimpleDateFormat
import groovy.transform.CompileStatic

import java.text.DecimalFormat
import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.cedarsoftware.util.Converter.*
import static com.cedarsoftware.util.StringUtilities.*

/**
 * Get information about a cell (makes it a uniform query-able object).  Optional method
 * exists to collapse types for UI.<br><br>
 *
 * <b>Valid cell types:</b>
 * string, double, long, boolean , bigdec, int, bigint, date, binary, exp, method,
 * template, string, byte, short, float, point2d, point3d, latlon, range, rangeset
 *
 * Use the constructor CellInfo(cell) to initialize the CellInfo from a standard cell
 * from an n-cube.  The cell would have been fetched from a getCellNoExecute() API.
 * Any cell fetched this way, placed into a CellInfo(value), will allow the cell to
 * be iterrogated for it's value (in String format), dataType name (in String format),
 * isUrl (true if value is a URL), and isCached (boolean if the cell was marked to be
 * cached).
 *
 * Call cellInfo.recreate() to recreate the exact value that was passed to the constructor
 * as mentioned in the above paragraph.  For example, CellInfo info = new CellInfo(cell)
 * where cell came from ncube.getCellNoExecute().  Next, to get the cell value back,
 * call info.recreate().  This will return the value that was stored in the n-cube cell.
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
class CellInfo
{
    public String value
    public String dataType
    public boolean isUrl
    public boolean isCached
    static final SafeSimpleDateFormat dateFormat = new SafeSimpleDateFormat('yyyy-MM-dd')
    static final SafeSimpleDateFormat dateTimeFormat = new SafeSimpleDateFormat('yyyy-MM-dd HH:mm:ss')
    private static final UNSUPPORTED_TYPE = 'bogusType'
    private static final Pattern DECIMAL_REGEX = ~/[.]/
    private static final Pattern HEX_DIGIT = ~/[0-9a-fA-F]+/
    private static final Map<String, Closure> typeConversion = [:]

    static
    {
        Closure stringToString = { Object val, String type, boolean cache -> return val }
        typeConversion[null] = stringToString
        typeConversion[''] = stringToString
        typeConversion['string'] = stringToString
        typeConversion['boolean'] = { Object val, String type, boolean cache ->
            String bool = (String)val
            if ('true'.equalsIgnoreCase(bool)) return true
            if ('false'.equalsIgnoreCase(bool)) return false
            throw new IllegalArgumentException("Boolean must be 'true' or 'false'.  Case does not matter.")
        }
        typeConversion['byte'] = { Object val, String type, boolean cache -> return convertToByte(val) }
        typeConversion['short'] = { Object val, String type, boolean cache -> return convertToShort(val) }
        typeConversion['int'] = { Object val, String type, boolean cache -> return convertToInteger(val) }
        typeConversion['long'] = { Object val, String type, boolean cache -> return convertToLong(val) }
        typeConversion['double'] = { Object val, String type, boolean cache -> return convertToDouble(val) }
        typeConversion['float'] = { Object val, String type, boolean cache -> return convertToFloat(val) }
        typeConversion['exp'] = { Object val, String type, boolean cache -> return new GroovyExpression((String)val, null, cache) }
        typeConversion['method'] = { Object val, String type, boolean cache -> return new GroovyMethod((String) val, null, cache) }
        typeConversion['template'] = { Object val, String type, boolean cache -> return new GroovyTemplate((String)val, null, cache) }
        Closure stringToDate = { Object val, String type, boolean cache ->
            try
            {
                Date date = convertToDate(val)
                return (date == null) ? val : date
            }
            catch (Exception ignored)
            {
                throw new IllegalArgumentException("Could not parse as a date: ${val}")
            }
        }
        typeConversion['date'] = stringToDate
        typeConversion['datetime'] = stringToDate   // synonym for 'date'
        typeConversion['binary'] = { Object val, String type, boolean cache ->  // convert hex string "10AF3F" as byte[]
            String hex = (String)val
            if (hex.length() % 2 != 0)
            {
                throw new IllegalArgumentException('Binary (hex) values must have an even number of digits.')
            }
            if (!HEX_DIGIT.matcher(hex).matches())
            {
                throw new IllegalArgumentException('Binary (hex) values must contain only the numbers 0 thru 9 and letters A thru F.')
            }
            return decode((String) val)
        }
        typeConversion['bigint'] = { Object val, String type, boolean cache -> return convertToBigInteger(val) }
        typeConversion['bigdec'] = { Object val, String type, boolean cache -> return convertToBigDecimal(val) }
        typeConversion['latlon'] = { Object val, String type, boolean cache ->
            Matcher m = Regexes.valid2Doubles.matcher((String) val)
            if (!m.matches())
            {
                throw new IllegalArgumentException(String.format('Invalid Lat/Long value (%s)', val))
            }

            return new LatLon(convertToDouble(m.group(1)), convertToDouble(m.group(2)))
        }
        typeConversion['point2d'] = { Object val, String type, boolean cache ->
            Matcher m = Regexes.valid2Doubles.matcher((String) val)
            if (!m.matches())
            {
                throw new IllegalArgumentException(String.format('Invalid Point2D value (%s)', val))
            }
            return new Point2D(convertToDouble(m.group(1)), convertToDouble(m.group(2)))
        }
        typeConversion['point3d'] = { Object val, String type, boolean cache ->
            Matcher m = Regexes.valid3Doubles.matcher((String) val)
            if (!m.matches())
            {
                throw new IllegalArgumentException(String.format('Invalid Point3D value (%s)', val))
            }
            return new Point3D(convertToDouble(m.group(1)),
                    convertToDouble(m.group(2)),
                    convertToDouble(m.group(3)))
        }
    }

    private static final ThreadLocal<DecimalFormat> decimalIntFormat = new ThreadLocal<DecimalFormat>() {
        DecimalFormat initialValue()
        {
            return new DecimalFormat('#,##0')
        }
    }

    private static final ThreadLocal<DecimalFormat> decimalFormat = new ThreadLocal<DecimalFormat>() {
        DecimalFormat initialValue()
        {
            return new DecimalFormat('#,##0.0##############')
        }
    }

    /**
     * @param type String datatype name (see comment at top of class)
     * @param value String value (if boolean, 'true', if numeric, then number quoted, if expression,
     * then expression content, etc.
     * @param isUrl boolean indicating whether or not this cell represents a URL (relative or full) to it's content
     * @param isCached boolean indicate whether or not this cell is supposed to be cached after retrieval.
     */
    CellInfo(String type, String value, Object isUrl, Object isCached)
    {
        this.dataType = type
        this.value = value
        this.isUrl = booleanValue(isUrl)
        this.isCached = booleanValue(isCached)
    }

    /**
     * Construct a cell info from the contents of a cell.  If the cell value was obtained from
     * getCellNoExecute(), then perfect reconstruction is possible.  If the value 'cell' was
     * obtained from ncube.getCell(), then it will not be able to reconstruct a CommandCell,
     * a URL cell, as the value would be the result of executing the CommandCell.
     */
    CellInfo(Object cell)
    {
        isUrl = false
        isCached = false
        value = null
        dataType = null
        createFrom(cell)
    }

    private void createFrom(Object cell)
    {
        if (cell == null || cell instanceof String)
        {
            value = (String) cell
            dataType = 'string'
        }
        else if (cell instanceof Long)
        {
            value = cell.toString()
            dataType = 'long'
        }
        else if (cell instanceof Boolean)
        {
            value = cell.toString()
            dataType = 'boolean'
        }
        else if (cell instanceof GroovyExpression)
        {
            GroovyExpression exp = (GroovyExpression) cell
            isUrl = hasContent(exp.url)
            value = isUrl ? exp.url : exp.cmd
            dataType = 'exp'
            isCached = exp.cacheable
        }
        else if (cell instanceof CellInfo)
        {   // clone
            CellInfo cellInfo = (CellInfo) cell
            isUrl = cellInfo.isUrl
            value = cellInfo.value
            dataType = cellInfo.dataType
            isCached = cellInfo.isCached
        }
        else if (cell instanceof Byte)
        {
            value = cell.toString()
            dataType = 'byte'
        }
        else if (cell instanceof Short)
        {
            value = cell.toString()
            dataType = 'short'
        }
        else if (cell instanceof Integer)
        {
            value = cell.toString()
            dataType = 'int'
        }
        else if (cell instanceof Date)
        {
            value = formatForDisplay((Date)cell)
            dataType = 'date'
        }
        else if (cell instanceof Double)
        {
            value = formatForEditing(cell)
            dataType = 'double'
        }
        else if (cell instanceof Float)
        {
            value = formatForEditing(cell)
            dataType = 'float'
        }
        else if (cell instanceof BigDecimal)
        {
            BigDecimal bigDec = cell as BigDecimal
            value = bigDec.stripTrailingZeros().toPlainString()
            dataType = 'bigdec'
        }
        else if (cell instanceof BigInteger)
        {
            value = cell.toString()
            dataType = 'bigint'
        }
        else if (cell instanceof byte[])
        {
            value = encode((byte[])cell)
            dataType = 'binary'
        }
        else if (cell instanceof Point2D)
        {
            value = cell.toString()
            dataType = 'point2d'
        }
        else if (cell instanceof Point3D)
        {
            value = cell.toString()
            dataType = 'point3d'
        }
        else if (cell instanceof LatLon)
        {
            value = cell.toString()
            dataType = 'latlon'
        }
        else if (cell instanceof GroovyMethod)
        {
            GroovyMethod method = (GroovyMethod)cell
            isUrl = hasContent(method.url)
            value = isUrl ? method.url : method.cmd
            dataType = 'method'
            isCached = method.cacheable
        }
        else if (cell instanceof StringUrlCmd)
        {
            StringUrlCmd strCmd = (StringUrlCmd)cell
            value = strCmd.url
            dataType = 'string'
            isUrl = true
            isCached = strCmd.cacheable
        }
        else if (cell instanceof BinaryUrlCmd)
        {
            BinaryUrlCmd binCmd = (BinaryUrlCmd)cell
            value = binCmd.url
            dataType = 'binary'
            isUrl = true
            isCached = binCmd.cacheable
        }
        else if (cell instanceof GroovyTemplate)
        {
            GroovyTemplate templateCmd = (GroovyTemplate)cell
            isUrl = hasContent(templateCmd.url)
            value = isUrl ? templateCmd.url : templateCmd.cmd
            dataType = 'template'
            isCached = templateCmd.cacheable
        }
        else if (cell instanceof Range)
        {
            Range range = (Range)cell
            isUrl = false
            value = formatForEditing(range)
            dataType = 'range'
            isCached = false
        }
        else if (cell instanceof RangeSet)
        {
            RangeSet set = (RangeSet)cell
            isUrl = false
            StringBuilder builder = new StringBuilder()
            final int len = set.size()
            for (int i = 0; i < len; i++)
            {
                if (i != 0)
                {
                    builder.append(', ')
                }
                Object val = set.get(i)
                if (val instanceof Range)
                {
                    Range range = (Range) val
                    boolean needsQuoted = range.low instanceof String
                    builder.append('[')
                    if (needsQuoted)
                    {
                        builder.append('"')
                    }
                    builder.append(formatForEditing(range.low))
                    if (needsQuoted)
                    {
                        builder.append('"')
                    }
                    builder.append(', ')
                    if (needsQuoted)
                    {
                        builder.append('"')
                    }
                    builder.append(formatForEditing(range.high))
                    if (needsQuoted)
                    {
                        builder.append('"')
                    }
                    builder.append(']')
                }
                else
                {
                    boolean needsQuoted = val instanceof String
                    if (needsQuoted)
                    {
                        builder.append('"')
                    }
                    builder.append(formatForEditing(val))
                    if (needsQuoted)
                    {
                        builder.append('"')
                    }
                }
            }
            value = builder.toString()
            dataType = 'rangeset'
            isCached = false
        }
        else
        {
            throw new IllegalArgumentException("Unknown cell value type, value: ${cell.toString()}, class: ${cell.class.name}")
        }
    }

    /**
     * Fetch the datatype of the passed in cell.  This is a cell from n-cube, not CellInfo.  It could have been
     * obtained from ncube.getCellNoExecute() in which case the answer will be perfectly accurate.  If the
     * passed in 'cell' was obtained from ncube.getCell() then it will reflect the more primitive cell type.  For
     * example, if the cell was a GroovyExpression, then ncube.getCell() would have returned the result of the
     * expression, and if primitive or one of the types supported by CellInfo, the string name of the primitive
     * will be returned.  If the execution of the cell returned a complex type, and that value is passed into this
     * method, an exception is thrown indicating an unsupported type, and the 'section' String name will be used
     * in the exception message.
     */
    static String getType(Object cell, String section)
    {
        String type = getType(cell)
        if (UNSUPPORTED_TYPE == type)
        {
            throw new IllegalArgumentException("Unsupported type ${cell.class.name} found in ${section}")
        }
        return type
    }

    /**
     * Support the 'as' operator so that the following expression works:<br>
     * CellInfo cellInfo = new CellInfo('hey')<br>
     * Map cellInfoMap = cellInfo as Map<br>
     * @param c Class to convert CellInfo to
     * @return CellInfo converted into an instance of the passed in class (c), or
     * throw an IllegalArgumentException if it cannot be coerced into the type.
     */
    Object asType(Class c)
    {
        if (Map.class.isAssignableFrom(c))
        {
            Map ret = [type: dataType]
            if (isUrl)
            {
                ret.url = value
            }
            else
            {
                ret.value = value
            }
            if (isCached)
            {   // Only add 'cache' to Map if cache=true
                ret.cache = true
            }
            return ret
        }
        else if (CellInfo.isAssignableFrom(c))
        {
            return this
        }
        else
        {
            throw new IllegalArgumentException("Unknown type to convert CellInfo to: ${c.name}")
        }
    }

    /**
     * Fetch the datatype of the passed in cell.  This is a cell from n-cube, not CellInfo.  It could have been
     * obtained from ncube.getCellNoExecute() in which case the answer will be perfectly accurate.  If the
     * passed in 'cell' was obtained from ncube.getCell() then it will reflect the more primitive cell type.  For
     * example, if the cell was a GroovyExpression, then ncube.getCell() would have returned the result of the
     * expression, and if primitive or one of the types supported by CellInfo, the string name of the primitive
     * will be returned.  If the execution of the cell returned a complex type, and that value is passed into this
     * method, a constant CellInfo.UNSUPPORTED_TYPE will be returned.
     */
    static String getType(Object cell)
    {
        if (cell == null) {
            return 'string'
        }

        if (cell instanceof String) {
            return 'string'
        }

        if (cell instanceof Double) {
            return 'double'
        }

        if (cell instanceof Long) {
            return 'long'
        }

        if (cell instanceof Boolean) {
            return 'boolean'
        }

        if (cell instanceof BigDecimal) {
            return 'bigdec'
        }

        if (cell instanceof Integer) {
            return 'int'
        }

        if (cell instanceof BigInteger) {
            return 'bigint'
        }

        if (cell instanceof Date) {
            return 'date'
        }

        if (cell instanceof BinaryUrlCmd || cell instanceof byte[]) {
            return 'binary'
        }

        if (cell instanceof GroovyExpression || cell instanceof Collection || cell.class.array) {
            return 'exp'
        }

        if (cell instanceof GroovyMethod) {
            return 'method'
        }

        if (cell instanceof GroovyTemplate) {
            return 'template'
        }

        if (cell instanceof StringUrlCmd) {
            return 'string'
        }

        if (cell instanceof Byte) {
            return 'byte'
        }

        if (cell instanceof Short) {
            return 'short'
        }

        if (cell instanceof Float) {
            return 'float'
        }

        if (cell instanceof Point2D)
        {
            return 'point2d'
        }

        if (cell instanceof Point3D)
        {
            return 'point3d'
        }

        if (cell instanceof LatLon)
        {
            return 'latlon'
        }

        if (cell instanceof Range)
        {
            return 'range'
        }

        if (cell instanceof RangeSet)
        {
            return 'rangeset'
        }

        return UNSUPPORTED_TYPE
    }

    /**
     * @return the content that would be placed into the cell as opposed to a CellInfo.  For example, if
     * the CellInfo was a simple boolean (value='true', isUrl=false, isCached=false, datatype='boolean'), then
     * Boolean.TRUE or Boolean.FALSE would be returned.  This method always returns a value that if placed
     * back into the CellInfo(cell) constructor, would return the equivalent CellInfo.
     */
    Object recreate()
    {
        return parseJsonValue(isUrl ? null : value, isUrl ? value : null, dataType, isCached)
    }

    /**
     * Collapse: byte, short, int ==> long
     * Collapse: float ==> double
     * Collapse: BigInteger ==> BigDecimal
     */
    void collapseToUiSupportedTypes()
    {
        if ('byte' == dataType || 'short' == dataType || 'int' == dataType)
        {
            dataType = 'long'
        }
        else if ('float' == dataType)
        {
            dataType = 'double'
        }
        else if ('bigint' == dataType)
        {
            dataType = 'bigdec'
        }
    }

    /**
     * Convert a set of the following parameters into a value (smaller in size than CellInfo), that can be
     * placed into an n-cube cell.  This value can be return to the original constituent pieces by using
     * the CellInfo(cell) constructor.  This method would convert the parameters (val='true', url=null,
     * type=null (or boolean), cache=true|false) into Boolean.TRUE or Boolean.FALSE, which is a much
     * smaller value.  Yet, if the boolean was passed to CellInfo(true), it would be reanimated with these
     * constituent parts set.  All cell datatypes support their version of this process. More complicated
     * cells like a CommandCell, convert from these constituent parts to an appropriate CommandCell instance
     * like GroovyExpression, for example.
     * @param val Object Could be a simple primitive, or a String value of a primitive, expression, etc.
     * This value can be null in which case 'url' should not be, unless the actual value of the cell is
     * intended to be null.
     * @param url Object If this is null, then value holds the value for the cell, otherwise this is the
     * URL to the cell contents.  It can be relative, in which case a cube with the name 'sys.classpath'
     * in the same Application will be used to supply the root for the relative URL.
     * @param type String name of the data type of the cell, 'int', 'long', 'boolean', 'exp', etc.  See
     * comments at top of this Class for all available cell types.
     * @param cache boolean indicating whether the retrieved value should be cached on subsequent accesses
     * by n-cube.
     * @return Object which will be a smaller representation (and one that n-cube expects) from the generic
     * specifiers.  This return value from this method can be reconstituted into the original pieces by
     * using the CellInfo(cell) constructor with the return value of this method.
     */
    static Object parseJsonValue(final Object val, final String url, final String type, boolean cache)
    {
        if (url != null)
        {
            if ('exp' == type)
            {
                return new GroovyExpression(null, url, cache)
            }
            else if ('method' == type)
            {
                return new GroovyMethod(null, url, cache)
            }
            else if ('template' == type)
            {
                return new GroovyTemplate(null, url, cache)
            }
            else if ('string' == type)
            {
                return new StringUrlCmd(url, cache)
            }
            else if ('binary'.equalsIgnoreCase(type))
            {
                return new BinaryUrlCmd(url, cache)
            }
            else
            {
                throw new IllegalArgumentException("url can only be specified with 'exp', 'method', 'template', 'string', or 'binary' types")
            }
        }

        return parseJsonValue(type, val, cache)
    }

    protected static Object parseJsonValue(String type, Object val, boolean cache)
    {
        if ('null' == val || val == null)
        {
            return null
        }
        else if (val instanceof Long)
        {
            if ('long' == type || type == null || '' == type)
            {
                return val
            }
            else if ('int' == type)
            {
                return ((Long)val).intValue()
            }
            else if ('bigint' == type)
            {
                return new BigInteger(val.toString())
            }
            else if ('byte' == type)
            {
                return ((Long)val).byteValue()
            }
            else if ('short' == type)
            {
                return (((Long)val).shortValue())
            }
            else if ('bigdec' == type)
            {
                return new BigDecimal((long)val)
            }
            return val
        }
        else if (val instanceof Boolean)
        {
            return val
        }
        else if (val instanceof Double)
        {
            if ('double' == type || type == null || '' == type)
            {
                return val
            }
            else if ('bigdec' == type)
            {
                return new BigDecimal(val)
            }
            else if ('float' == type)
            {
                return ((Double)val).floatValue()
            }
            return val
        }
        else if (val instanceof String)
        {
            val = ((String)val).trim()
            Closure method = typeConversion[type]
            if (method == null)
            {
                throw new IllegalArgumentException("Unknown value: ${type} for 'type' field")
            }
            return method(val, type, cache)
        }
        else if (val.class.array)
        {   // Legacy support - remove once we drop support for array type (can be done using GroovyExpression).
            StringBuilder exp = new StringBuilder()
            exp.append('[')
            Object[] values = val as Object[]
            int i=0
            for (Object value : values)
            {
                i++
                Object o = parseJsonValue(value, null, type, cache)
                exp.append(javaToGroovySource(o))
                if (i < values.length)
                {
                    exp.append(',')
                }
            }
            exp.append('] as Object[]')
            return new GroovyExpression(exp.toString(), null, cache)
        }
        else
        {
            throw new IllegalArgumentException("Error reading value of type '${val.class.name}' - Simple JSON format for NCube only supports Long, Double, String, String Date, Boolean, or null")
        }
    }

    /**
     * Convert Java data-type to a Groovy Source equivalent
     * @param o Java primitive type
     * @return Groovy source code equivalent of passed in value.  For example, if a BigInteger is passed in,
     * the value will be return as a String with a "G" at the end.
     */
    protected static String javaToGroovySource(Object o)
    {
        StringBuilder builder = new StringBuilder()
        if (o instanceof String)
        {
            builder.append("'")
            builder.append(o.toString())
            builder.append("'")
        }
        else if (o instanceof GroovyExpression)
        {
            builder.append("'")
            builder.append(((GroovyExpression) o).cmd)
            builder.append("'")
        }
        else if (o instanceof Boolean)
        {
            builder.append(o ? 'true' : 'false')
        }
        else if (o instanceof Double)
        {
            builder.append(formatForEditing(o))
            builder.append('d')
        }
        else if (o instanceof Integer)
        {
            builder.append(o)
            builder.append('i')
        }
        else if (o instanceof Long)
        {
            builder.append(o)
            builder.append('L')
        }
        else if (o instanceof BigDecimal)
        {
            builder.append(((BigDecimal)o).stripTrailingZeros().toPlainString())
            builder.append('G')
        }
        else if (o instanceof BigInteger)
        {
            builder.append(o)
            builder.append('G')
        }
        else if (o instanceof Byte)
        {
            builder.append(o)
            builder.append(' as Byte')
        }
        else if (o instanceof Float)
        {
            builder.append(formatForEditing(o))
            builder.append('f')
        }
        else if (o instanceof Short)
        {
            builder.append(o)
            builder.append(' as Short')
        }
        else
        {
            throw new IllegalArgumentException("Unknown Groovy Type : ${o.class.name}")
        }
        return builder.toString()
    }

    /**
     * Convert internal value to 'pretty' value for screen.  Doubles, BigDecimal, etc. become strings,
     * Date converted to string, all else are .toString() on the underlying value.
     * @param val Comparable to be shown to user
     * @return String version of passed in value
     */
    static String formatForDisplay(Comparable val)
    {
        if (val instanceof Double || val instanceof Float)
        {   // Adds commas to make it easy to read
            return decimalFormat.get().format(val)
        }
        else if (val instanceof BigDecimal)
        {
            BigDecimal x = val as BigDecimal
            String s = x.stripTrailingZeros().toPlainString()
            if (s.contains("."))
            {
                String[] pieces = DECIMAL_REGEX.split(s)
                return decimalIntFormat.get().format(new BigInteger(pieces[0])) + '.' + pieces[1]
            }
            else
            {
                return decimalIntFormat.get().format(val)
            }
        }
        else if (val instanceof Number)
        {
            return decimalIntFormat.get().format(val)
        }
        else if (val instanceof Date)
        {
            return getDateAsString((Date)val)
        }
        else if (val == null)
        {
            return 'Default'
        }
        else
        {
            return val.toString()
        }
    }

    /**
     * Get string value of passed in value in an editor-friendly string.
     * For example, a double would be '1234.123' as opposed to '1,234.123' (no comma).
     */
    static String formatForEditing(Object val)
    {
        if (val instanceof Date)
        {
            return '"' + getDateAsString((Date)val) + '"'
        }
        else if (val instanceof Double || val instanceof Float || val instanceof BigDecimal)
        {
            return convertToString(val)
        }
        else if (val instanceof Range)
        {
            Range range = (Range)val
            return "${formatForEditing(range.low)}, ${formatForEditing(range.high)}"
        }
        return val.toString()
    }

    private static String getDateAsString(Date date)
    {
        Calendar cal = Calendar.instance
        cal.clear()
        cal.time = date
        if (cal.get(Calendar.HOUR) == 0 && cal.get(Calendar.MINUTE) == 0 && cal.get(Calendar.SECOND) == 0)
        {
            return dateFormat.format(date)
        }
        return dateTimeFormat.format(date)
    }

    static boolean booleanValue(Object o)
    {
        if (o instanceof Boolean)
        {
            return (Boolean) o
        }
        else if (o instanceof String)
        {
            String s = (String) o

            if ('true'.equalsIgnoreCase(s))
            {
                return true
            }
        }

        return false
    }

    boolean equals(Object o)
    {
        if (this.is(o))
        {
            return true
        }
        if (!(o instanceof CellInfo))
        {
            return false
        }

        CellInfo cellInfo = (CellInfo) o

        if (isUrl != cellInfo.isUrl)
        {
            return false
        }
        if (isCached != cellInfo.isCached)
        {
            return false
        }
        if (value != null ? value != cellInfo.value : cellInfo.value != null)
        {
            return false
        }
        return !(dataType != null ? dataType != cellInfo.dataType : cellInfo.dataType != null)
    }

    int hashCode()
    {
        int result = value != null ? value.hashCode() : 0
        result = 31 * result + (dataType != null ? dataType.hashCode() : 0)
        result = 31 * result + (isUrl ? 1 : 0)
        result = 31 * result + (isCached ? 1 : 0)
        return result
    }
}
