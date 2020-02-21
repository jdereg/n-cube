package com.cedarsoftware.ncube.formatters

import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.CellInfo
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.CommandCell
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.Range
import com.cedarsoftware.ncube.RangeSet
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point2D
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic

import static com.cedarsoftware.util.StringUtilities.encode

/**
 * Format an NCube into an JSON document
 *
 * @author Ken Partlow (kpartlow@gmail.com), John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain axis copy of the License at
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
class JsonFormatter extends BaseJsonFormatter implements NCubeFormatter
{
    public static final String DEFAULT_COLUMN_PREFIX = 'd3fault_c0lumn_'
    private static final String MAX_INT = Long.toString(Integer.MAX_VALUE)

    JsonFormatter() {}

    JsonFormatter(OutputStream stream) { super(stream) }

    /**
     * Use this API to generate JSON view of this NCube.
     */
    String format(NCube ncube, Map<String, Object> options = null)
    {
        if (!(builder instanceof StringWriter))
        {
            throw new IllegalStateException("Builder is not a StringWriter.  Use formatCube(ncube) to write to your stream.")
        }

        formatCube(ncube, options)
        return builder.toString()
    }

    void formatCube(NCube ncube, Map<String, Object> options)
    {
        if (ncube == null)
        {
            throw new IllegalArgumentException("Cube to format cannot be null")
        }
        if (options == null)
        {
            options = [:]
        }

        String name = ncube.name
        try
        {
            startObject()
            writeObjectKeyValue("ncube", name, true)
            Object defCellValue = ncube.defaultCellValue

            if (defCellValue != null)
            {
                String valType = CellInfo.getType(defCellValue, "defaultCell")
                if (valType != null)
                {
                    writeObjectKeyValue(NCube.DEFAULT_CELL_VALUE_TYPE, valType, true)
                }
                if (defCellValue instanceof CommandCell)
                {
                    CommandCell cmd = (CommandCell) defCellValue

                    if (cmd.cacheable)
                    {   // Only write 'cache' when the cache value is true.  Leave false be default.
                        writeObjectKeyValue(NCube.DEFAULT_CELL_VALUE_CACHE, true, true)
                    }
                    if (cmd.url != null)
                    {
                        writeObjectKeyValue(NCube.DEFAULT_CELL_VALUE_URL, cmd.url, true)
                    }
                    else
                    {
                        writeObjectKeyValue(NCube.DEFAULT_CELL_VALUE, cmd.cmd, true)
                    }
                }
                else
                {
                    writeObjectKeyValue(NCube.DEFAULT_CELL_VALUE, defCellValue, true)
                }
            }

            if (!ncube.metaProperties.isEmpty())
            {
                writeMetaProperties(ncube.metaProperties)
                comma()
            }
            writeAxes(ncube.axes, options)
            writeCells((Map)ncube.cellMap, options)
            endObject()
            closeStream()
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Unable to format NCube '${name}' into JSON", e)
        }
    }

    private void writeMetaProperties(Map<String, Object> metaProps) throws IOException
    {
        Iterator<Map.Entry<String, Object>> i = metaProps.entrySet().iterator()
        while (i.hasNext())
        {
            Map.Entry<String, Object> entry = i.next()
            final String key = entry.key
            Object value = entry.value

            if (value == null || value instanceof String || value instanceof Boolean)
            {   // Allows for simple key ==> value associations to be written when value is JSON supported type
                writeObjectKeyValue(key, value, false)
            }
            else if (value instanceof CellInfo)
            {   // If meta-prop value already a cell info, then write it out as-is
                CellInfo cell = (CellInfo) value
                writeObjectKey(key)
                startObject()
                writeType(cell.dataType)
                writeCellValue(cell.recreate())
                endObject()
            }
            else
            {
                // Convert unknown value to CellInfo (handles, all primitives, CommandCells, and Point2D, Point3D, and LatLon)
                CellInfo cell = new CellInfo(value)
                writeObjectKey(key)
                startObject()
                writeType(cell.dataType)
                writeCellValue(cell.recreate())
                endObject()
            }

            if (i.hasNext())
            {
                comma()
            }
        }
    }

    void writeAxes(List<Axis> axes, Map<String, Object> options) throws IOException
    {
        if (axes.empty)
        {   // Make sure the formatter writes valid JSON
            options.indexFormat ? append('"axes":{},') : append('"axes":[],')
            return
        }

        writeObjectKey("axes")

        if (options.indexFormat)
        {
            startObject()
            Iterator<Axis> i = axes.iterator()
            while (i.hasNext())
            {
                Axis axis = i.next()
                copyDefaultColumnMetaPropsToAxis(axis.defaultColumn, axis)
                writeIndexAxis(axis, options)
                removeDefaultColumnMetaPropsFromAxis(axis)
                if (i.hasNext())
                {
                    comma()
                }
            }
            endObject()
        }
        else
        {
            startArray()
            Iterator<Axis> i = axes.iterator()
            while (i.hasNext())
            {
                Axis axis = i.next()
                copyDefaultColumnMetaPropsToAxis(axis.defaultColumn, axis)
                writeAxis(axis, options)
                removeDefaultColumnMetaPropsFromAxis(axis)
                if (i.hasNext())
                {
                    comma()
                }
            }
            endArray()
        }
        comma()
    }

    /**
     * Copy the meta-properties from the default column to the Axis.  Since the default column is not
     * persisted, they are moved here, and then moved back to the default column on save / load.
     */
    private static void copyDefaultColumnMetaPropsToAxis(Column defCol, Axis axis)
    {
        if (defCol == null)
        {
            return
        }

        Map<String, Object> props = defCol.metaProperties
        props.each { String key, Object value ->
            axis.setMetaProperty("${DEFAULT_COLUMN_PREFIX}${key}", value)
        }
    }

    /**
     * Remove the default column's meta-properties from the Axis meta-properties.
     * They were put there temporarily, only during writing of the Axis JSON.
     */
    private static void removeDefaultColumnMetaPropsFromAxis(Axis axis)
    {
        Map<String, Object> copy = new CaseInsensitiveMap<>(axis.metaProperties)
        Iterator<String> i = copy.keySet().iterator()
        while (i.hasNext())
        {
            String key = i.next()
            if (key.startsWith(DEFAULT_COLUMN_PREFIX))
            {
                axis.removeMetaProperty(key)
            }
        }
    }

    // default is false, so no need to write those out.
    void writeAxis(Axis axis, Map<String, Object> options) throws IOException
    {
        startObject()

        // required inputs
        writeAxisGuts(axis, options)
        endObject()
    }

    // default is false, so no need to write those out.
    void writeIndexAxis(Axis axis, Map<String, Object> options) throws IOException
    {
        // required inputs
        writeObjectKey(axis.name.toLowerCase())
        startObject()
        writeAxisGuts(axis, options)
        endObject()
    }

    private void writeAxisGuts(Axis axis, Map<String, Object> options)
    {
        writeObjectKeyValue("id", axis.id, true)
        writeObjectKeyValue("name", axis.name, true)
        writeObjectKeyValue("hasDefault", axis.hasDefaultColumn(), true)
        if (!axis.metaProperties.isEmpty())
        {
            if (axis.reference)
            {
                writeObjectKeyValue("isRef", true, true)
            }
            writeMetaProperties(axis.metaProperties)
            comma()
        }

        if (options.indexFormat || !axis.reference)
        {   // indexFormat (with or without reference axis) or regular format without reference axis
            writeObjectKeyValue("type", axis.type.name(), true)
            writeObjectKeyValue("valueType", axis.valueType.name(), true)

            //  optional inputs that can use defaults
            writeObjectKeyValue("preferredOrder", axis.columnOrder, true)
            writeObjectKeyValue("fireAll", axis.fireAll, true)
            writeColumns(axis.columns, options)
        }
        else
        {   // regular format with reference axis
            append('"columns":[')
            boolean first = true
            Iterator<Column> i = axis.columnsWithoutDefault.iterator()
            while (i.hasNext())
            {
                Column column = i.next()
                Map<String, Object> metaProps = column.metaProperties
                if (!metaProps.isEmpty())
                {
                    if (first)
                    {
                        first = false
                    }
                    else
                    {
                        comma()
                    }
                    append("""{"id":${column.id},""")
                    writeMetaProperties(metaProps)
                    append('}')
                }
            }
            append(']')
        }
    }

    void writeColumns(List<Column> columns, Map<String, Object> options) throws IOException
    {
        append('"columns":')
        options.indexFormat ? startObject() : startArray()

        boolean firstPass = true

        for (Column column : columns)
        {
            if (!column.default)
            {
                if (!firstPass)
                {
                    comma()
                }
                writeColumn(column, options)
                firstPass = false
            }
        }

        options.indexFormat ? endObject() : endArray()
    }

    void writeColumn(Column column, Map<String, Object> options) throws IOException
    {
        String columnType = getColumnType(column.value)
        if (options.indexFormat)
        {
            writeObjectKey(String.valueOf(column.id))
            startObject()
        }
        else
        {
            startObject()

            //  Check to see if id exists anywhere. then optimize
            writeId(column.id, true)
        }
        writeType(columnType)
        if (!column.metaProperties.isEmpty())
        {
            writeMetaProperties(column.metaProperties)
            comma()
        }
        if (column.value instanceof CommandCell)
        {
            writeCommandCell((CommandCell) column.value)
        }
        else
        {
            writeObjectKeyValue("value", column.value, false)
        }
        endObject()
    }

    void writeCommandCell(CommandCell cmd) throws IOException
    {
        if (cmd.cacheable)
        {
            writeObjectKeyValue("cache", cmd.cacheable, true)
        }
        if (cmd.url != null)
        {
            writeObjectKeyValue("url", cmd.url, false)
        }
        else
        {
            writeObjectKeyValue("value", cmd.cmd, false)
        }
    }

    /**
     * According to parseJsonValue reading in, if your item is one of the following end types it does not need to
     * specify the end type:  String, Long, Boolean, Double.  These items will all be picked up automatically
     * so to save on those types I don't write out the type.
     * @param type Type to write, if null don't write anything because its axis default type
     */
    void writeType(String type) throws IOException
    {
        if (type == null)
        {
            return
        }

        writeObjectKeyValue("type", type, true)
    }

    void writeCells(Map<Set<Long>, Object> cells, Map<String, Object> options) throws IOException
    {
        append('"cells":')
        if (cells == null || cells.isEmpty() || options.nocells)
        {
            options.indexFormat ? append('{}') : append("[]")
            return
        }

        if (options.indexFormat)
        {   // {"1000000000001_2000000000001":{"type":"string", "value":10}}
            startObject()
            Iterator<Map.Entry<Set<Long>, Object>> i = cells.entrySet().iterator()
            while (i.hasNext())
            {
                // Write key, e.g. "1000000000001_2000000000001"
                Map.Entry<Set<Long>, Object> entry = i.next()
                append('"')
                Iterator<Long> j = entry.key.iterator()
                while (j.hasNext())
                {
                    append(j.next())
                    if (j.hasNext())
                    {
                        append('_')
                    }
                }
                append('":')

                Object value = entry.value
                startObject()
                writeType(CellInfo.getType(value, "cell"))
                writeCellValue(value)
                endObject()

                if (i.hasNext())
                {
                    comma()
                }
            }
            endObject()
        }
        else
        {
            startArray()
            Iterator<Map.Entry<Set<Long>, Object>> i = cells.entrySet().iterator()
            while (i.hasNext())
            {
                writeCell(i.next())
                if (i.hasNext())
                {
                    comma()
                }
            }
            endArray()
        }
    }

    private void writeCell(Map.Entry<Set<Long>, Object> cellEntry) throws IOException
    {
        startObject()
        writeIds(cellEntry.key)
        writeType(CellInfo.getType(cellEntry.value, "cell"))
        writeCellValue(cellEntry.value)
        endObject()
    }

    private void writeCellValue(Object value)
    {
        if (value instanceof CommandCell)
        {
            writeCommandCell((CommandCell)value)
        }
        else
        {
            writeObjectKeyValue("value", value, false)
        }
    }

    void writeIds(Set<Long> colIds)
    {
        append('"id":')
        startArray()

        boolean firstPass = true

        for (Long colId : colIds)
        {
            final String idAsString = Long.toString(colId)
            if (!idAsString.endsWith(MAX_INT))
            {
                writeIdValue(colId, !firstPass)
                firstPass = false
            }
        }

        endArray()
        comma()
    }

    void writeId(Long longId, boolean addComma) throws IOException
    {
        writeObjectKeyValue("id", longId, addComma)
    }

    void writeIdValue(Long longId, boolean addComma)
    {
        if (addComma)
        {
            comma()
        }

        append(longId)
    }

    static String getColumnType(Object o)
    {
        if (o instanceof Range || o instanceof RangeSet)
        {
            return null
        }

        return CellInfo.getType(o, "column")
    }

    protected void writeObjectValue(Object o) throws IOException
    {
        if (o == null)
        {
            builder.append("null")
        }
        else if (o instanceof String)
        {
            StringWriter w = new StringWriter()
            JsonWriter.writeJsonUtf8String(o.toString(), w)
            builder.append(w.toString())
        }
        else if (o instanceof BigInteger)
        {
            BigInteger i = (BigInteger)o
            builder.append('"')
            builder.append(i.toString())
            builder.append('"')
        }
        else if (o instanceof BigDecimal)
        {
            BigDecimal d = (BigDecimal)o
            builder.append('"')
            builder.append(d.stripTrailingZeros().toPlainString())
            builder.append('"')
        }
        else if (o instanceof Number)
        {
            builder.append(o.toString())
        }
        else if (o instanceof Date)
        {
            builder.append('"')
            builder.append(dateFormat.format(o))
            builder.append('"')
        }
        else if (o instanceof LatLon)
        {
            LatLon l = (LatLon)o
            builder.append('"')
            builder.append(l.toString())
            builder.append('"')
        }
        else if (o instanceof Point2D)
        {
            Point2D pt = (Point2D)o
            builder.append('"')
            builder.append(pt.toString())
            builder.append('"')
        }
        else if (o instanceof Point3D)
        {
            Point3D pt = (Point3D)o
            builder.append('"')
            builder.append(pt.toString())
            builder.append('"')
        }
        else if (o instanceof Range)
        {
            Range r = (Range)o
            startArray()
            writeObjectValue(r.low)
            comma()
            writeObjectValue(r.high)
            comma()
            writeObjectValue(r.priority)
            endArray()
        }
        else if (o instanceof RangeSet)
        {
            RangeSet r = (RangeSet)o
            Iterator i = r.iterator()
            startArray()
            boolean firstPass = true
            while (i.hasNext())
            {
                if (!firstPass)
                {
                    comma()
                }
                writeObjectValue(i.next())
                firstPass = false
            }
            endArray()
        }
        else if (o instanceof byte[])
        {
            builder.append('"')
            builder.append(encode((byte[])o))
            builder.append('"')
        }
        else if (o.class.array)
        {
            throw new IllegalArgumentException("Cell cannot be an array (except byte[]). Use Groovy Expression to make cell an array, a List, or a Map, etc.")
        }
        else
        {
            builder.append(o.toString())
        }
    }
}