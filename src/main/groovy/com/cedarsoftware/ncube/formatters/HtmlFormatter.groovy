package com.cedarsoftware.ncube.formatters

import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.AxisType
import com.cedarsoftware.ncube.CellInfo
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.CommandCell
import com.cedarsoftware.ncube.GroovyBase
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.RangeSet
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point2D
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic

import java.lang.reflect.Array

import static com.cedarsoftware.util.StringUtilities.encode
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.io.MetaUtils.isPrimitive
import static java.lang.Math.abs

/**
 * Format an NCube into an HTML document
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
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
class HtmlFormatter implements NCubeFormatter
{
    String[] _headers

    HtmlFormatter(String... headers)
    {
        _headers = headers
    }

    /**
     * Calculate important values needed to display an NCube.
     *
     * @return Object[], where element 0 is a List containing the axes
     * where the first axis (element 0) is the axis to be displayed at the
     * top and the rest are the axes sorted smallest to larges.  Element 1
     * of the returned object array is the height of the cells (how many
     * rows it would take to display the entire ncube). Element 2 is the
     * width of the cell matrix (the number of columns would it take to display
     * the cell portion of the NCube).
     */
    protected Object[] getDisplayValues(NCube ncube)
    {
        if (_headers == null)
        {
            _headers = [] as String[]
        }
        Map<String, Object> headerStrings = new CaseInsensitiveMap()
        for (String header : _headers)
        {
            headerStrings[header] = null
        }
        // Step 1. Sort axes from smallest to largest.
        // Hypercubes look best when the smaller axes are on the inside, and the larger axes are on the outside.
        List<Axis> axes = new ArrayList<>(ncube.axes)
        Collections.sort(axes, new Comparator<Axis>() {
            int compare(Axis a1, Axis a2)
            {
                a2.size() - a1.size()
            }
        })

        // Step 2.  Now find an axis that is a good candidate for the single (top) axis.  This would be an axis
        // with the number of columns closest to 12.
        int smallestDelta = Integer.MAX_VALUE
        int candidate = -1
        int count = 0

        for (Axis axis : axes)
        {
            if (headerStrings.keySet().contains(axis.name))
            {
                candidate = count
                break
            }
            int delta = abs(axis.size() - 12)
            if (delta < smallestDelta)
            {
                smallestDelta = delta
                candidate = count
            }
            count++
        }

        // Step 3. Compute cell area size
        Axis top = axes.remove(candidate)
        axes.add(0, top)   // Top element is now first.
        top = axes.remove(0)   // Grab 1st (candidate axis) one more time
        if (top.type == AxisType.RULE)
        {   // If top is a rule axis, place it last.  It is recognized that there could
            // be more than one rule axis, and there could also be a single rule axis, in
            // which this is a no-op.
            axes.add(top)
        }
        else
        {
            axes.add(0, top)
        }
        long width = axes[0].size()
        long height = 1
        final int len = axes.size()

        for (int i = 1; i < len; i++)
        {
            height = axes[i].size() * height
        }

        [axes, height, width] as Object[]
    }

    /**
     * Use this API to generate an HTML view of this NCube.
     * matches one of the passed in headers, will be the axis chosen to be displayed at the top.
     *
     * @return String containing an HTML view of this NCube.
     */
    String format(NCube ncube)
    {
        if (ncube.axes.size() < 1)
        {
            return getNoAxisHtml()
        }

        final StringBuilder s = new StringBuilder()
        final Object[] displayValues = getDisplayValues(ncube)
        final List<Axis> axes = (List<Axis>) displayValues[0]
        final long height = displayValues[1] as long
        final long width = displayValues[2] as long

        s.append(getHtmlPreamble())

        // Top row (special case)
        final Axis topAxis = axes[0]
        final List<Column> topColumns = topAxis.columns
        final String topAxisName = topAxis.name

        if (axes.size() == 1)
        {   // Ensure that one dimension is vertically down the page
            s.append("""\
 <th data-id="a${topAxis.id}" class="th-ncube ncube-head">
  <div class="btn-group axis-menu" data-id="${topAxisName}">
   <button type="button" class="btn-sm btn-primary dropdown-toggle axis-btn" data-toggle="dropdown">
    <span>${topAxisName}</span><span class="caret"></span>
   </button></th>
  </div>
 <th class="th-ncube ncube-dead">${ncube.name}</th>
</tr>""")

            for (int i = 0; i < width; i++)
            {
                s.append('<tr>\n')
                Column column = topColumns[i]
                s.append(""" <th data-id="${column.id}" data-axis="${topAxisName}" class="th-ncube ${getColumnCssClass(column)}">""")
                buildColumnGuts(s, column)
                s.append('</th>\n')
                buildCell(ncube, s, [column.id] as Set, i % 2i == 0)
                s.append('</tr>\n')
            }
        }
        else
        {   // 2D+ shows as one column on the X axis and all other dimensions on the Y axis.
            int deadCols = axes.size() - 1i
            if (deadCols > 0)
            {
                s.append(""" <th class="th-ncube ncube-dead" colspan="${deadCols}">${ncube.name}</th>""")
            }
            s.append("""\
  <th data-id="a${topAxis.id}" class="th-ncube ncube-head" colspan="${topAxis.size()}">
   <div class="btn-group axis-menu" data-id="${topAxisName}">
    <button type="button" class="btn-sm btn-primary dropdown-toggle axis-btn" data-toggle="dropdown">
     <span>${topAxisName}</span><span class="caret"></span>
    </button>
   </div>
  </th>
 </tr>
 <tr>  
""")
            Map<String, Long> rowspanCounter = [:]
            Map<String, Long> rowspan = [:]
            Map<String, Long> columnCounter = [:]
            Map<String, List<Column>> columns = [:]

            final int axisCount = axes.size()

            for (int i = 1; i < axisCount; i++)
            {
                final Axis axis = axes[i]
                final String axisName = axis.name
                s.append("""\
 <th data-id="a${axis.id}" class="th-ncube ncube-head">
  <div class="btn-group axis-menu" data-id="${axisName}">
   <button type="button" class="btn-sm btn-primary dropdown-toggle axis-btn" data-toggle="dropdown">
    <span>${axisName}</span><span class="caret"></span>
   </button>
  </div>
 </th>
 """)
                long colspan = 1

                for (int j = i + 1; j < axisCount; j++)
                {
                    colspan *= axes[j].size()
                }

                rowspan[axisName] = colspan
                rowspanCounter[axisName] = 0L
                columnCounter[axisName] = 0L
                columns[axisName] = axis.columns
            }

            for (Column column : topColumns)
            {
                s.append(""" <th data-id="${column.id}" data-axis="${topAxisName}" class="th-ncube-top ${getColumnCssClass(column)}">""")
                buildColumnGuts(s, column)
                s.append('</th>\n')
            }

            final int topColumnSize = topColumns.size()

            if (topAxis.size() != topColumnSize)
            {
                s.append(""" <th class="th-ncube-top ${getColumnCssClass(topAxis.defaultColumn)}">Default</th>""")
            }

            s.append('</tr>\n')
            Map<String, Long> colIds = [:]

            // The left column headers and cells
            for (long h = 0; h < height; h++)
            {
                s.append('<tr>\n')
                // Column headers for the row
                for (int i = 1; i < axisCount; i++)
                {
                    final Axis axis = axes[i]
                    final String axisName = axis.name
                    long count = rowspanCounter[axisName]

                    if (count == 0L)
                    {
                        long colIdx = columnCounter[axisName]
                        final Column column = columns[axisName][(int)colIdx]
                        colIds[axisName] = column.id    // snag column.id for cell coordinate later
                        final long span = rowspan[axisName]

                        // Use column's ID as TH element's ID
                        s.append(""" <th data-id="${column.id}" data-axis="${axisName}" class="th-ncube """)
                        s.append(getColumnCssClass(column))
                        if (span != 1)
                        {   // Need to show rowspan attribute
                            s.append('" rowspan="')
                            s.append(span)
                        }
                        s.append('">')
                        buildColumnGuts(s, column)
                        s.append('</th>\n')

                        // Increment column counter
                        colIdx++
                        if (colIdx >= axis.size())
                        {
                            colIdx = 0L
                        }
                        columnCounter[axisName] = colIdx
                    }
                    // Increment row counter (counts from 0 to rowspan of subordinate axes)
                    count++
                    if (count >= rowspan[axisName])
                    {
                        count = 0L
                    }
                    rowspanCounter[axisName] = count
                }

                // Cells for the row
                for (int i = 0; i < width; i++)
                {
                    // Keep replacing the column ID for the top row portion of the coordinate (Set of IDs)
                    colIds[topAxisName] = topColumns[i].id
                    // Other coordinate values are set above this for-loop
                    buildCell(ncube, s, colIds.values() as Set, h % 2 == 0L)
                }

                s.append('</tr>\n')
            }
        }

        s.append('</table>\n</body>\n</html>')
        s.toString()
    }

    private static void buildColumnGuts(StringBuilder s, Column column)
    {
        final boolean isCmd = column.value instanceof CommandCell
        final boolean isUrlCmd = isCmd && hasContent(((CommandCell) column.value).url)

        addColumnPrefixText(s, column)
        if (isUrlCmd)
        {
            s.append('<a href="#">')
        }
        s.append(column.default ? "Default" : escapeHTML(column.toString()))
        if (isUrlCmd)
        {
            s.append('</a>')
        }
    }

    private static void addColumnPrefixText(StringBuilder s, Column column)
    {
        if (column.value instanceof CommandCell)
        {
            String name = column.getMetaProperty("name") as String
            if (hasContent(name))
            {
                s.append("""<span class="rule-name">${name}</span><hr class="hr-rule"/>""")
            }
        }
    }


    private static String getHtmlPreamble()
    {
        """\
<!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="UTF-8">
 <title>NCube: </title>
 <style>
.table-ncube {
  border-collapse:collapse;
  border:1px solid lightgray;
  font-family: "arial","helvetica", sans-serif;
  font-size: small;
  padding: 2px;
}

.td-ncube .th-ncube .th-ncube-top {
  border:1px solid lightgray;
  font-family: "arial","helvetica", sans-serif;
  font-size: small;
  padding: 2px;
}

.td-ncube {
  color: black;
  background: white;
  text-align: center;
}

.th-ncube {
  padding-left: 4px !important;
  padding-right: 4px !important;
  color: white;
  font-weight: normal;
}

.th-ncube-top {
  color: white;
  text-align: center;
  font-weight: normal;
}

.td-ncube:hover { background: #E0F0FF }
.th-ncube:hover { background: #A2A2A2 }
.th-ncube-top:hover { background: #A2A2A2 }
.ncube-num { text-align: right; }
.ncube-dead { background: #6495ED; }
.ncube-head { background: #4D4D4D; }

.column {
  background: #929292;
  padding: 2px;
  margin: 2px;
  white-space: pre;
}

.column-code {
  text-align: left;
  vertical-align: top;
  font-family:menlo,"Lucida Console",monaco,monospace,courier;
}

.column-url {
  text-align: left;
  vertical-align: top;
}

.hr-rule {
  margin:1px;
  border-style:dashed;
  border-color:darkgray;
}

.null-cell {
    font-style: oblique;
    color: darkgray !important;
}

.cell {
  color: black;
  background: white;
  text-align: center;
  vertical-align: middle;
  white-space: pre;
  padding: 2px;
  margin: 2px;
  word-break: normal;
  word-wrap: normal;
}

.rule-name  {
  background:#555;
  color:#e8e8e8;
  border-radius:3px;
  font-family: "arial","helvetica", sans-serif;
  padding-left:5px;
  padding-right:5px;
  padding-top:1px;
  padding-bottom:1px
}

.def-cell-color { color:#ccc; }
.def-cell-col-color { color:#b4cdcd; }

.cell-url {
  color: red;
  text-align: left;
  vertical-align: middle;
}

.cell-code {
  text-align: left;
  vertical-align: top;
  font-family:menlo,"Lucida Console",monaco,monospace,courier;
}

.odd-row { background-color: #e0e0e0 !important; }
.odd-row:hover { background-color: #E0F0FF !important; }
.cell-selected { background-color: #D0E0FF !important; }
.cell-selected:hover { background-color: #E0F0FF !important; }
th.ncube-dead:hover { background: #76A7FF; }
.th-ncube a, .th-ncube-top a { color: #d0ffff; }
.th-ncube > a:hover, .th-ncube-top > a:hover { color: lightcyan; }
 </style>
</head>
<body>
<table class="table-ncube" border="1">
<tr>
"""
    }

    private static String getNoAxisHtml()
    {
"""\
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>Empty NCube</title>
  </head>
  <body/>
</html>
"""
    }

    private static String getColumnCssClass(Column col)
    {
        if (col.value instanceof CommandCell)
        {
            CommandCell cmd = (CommandCell) col.value
            if (hasContent(cmd.url))
            {
                return 'column column-url'
            }
            else if (cmd instanceof GroovyBase)
            {
                return 'column column-code'
            }
        }
        return 'column'
    }

    private static void buildCell(NCube ncube, StringBuilder s, Set<Long> coord, boolean odd)
    {
        final String oddRow = odd ? '' : 'odd-row'
        s.append(""" <td data-id="${makeCellId(coord)}" class="td-ncube ${oddRow} """)

        if (ncube.containsCellById(coord))
        {   // Populated cell
            final Object cell = ncube.getCellByIdNoExecute(coord)
            if (cell instanceof CommandCell)
            {
                final CommandCell cmd = cell as CommandCell
                if (hasContent(cmd.url))
                {
                    s.append("""cell cell-url"><a href="#">${cmd.url}</a>""")
                }
                else
                {
                    s.append('cell cell-code">')
                    s.append(escapeHTML(getCellValueAsString(cell)))
                }
            }
            else if (cell == null)
            {
                s.append('cell null-cell">null')
            }
            else
            {
                s.append('cell">')
                s.append(escapeHTML(getCellValueAsString(cell)))
            }
        }
        else
        {   // Handle default cells (n-cube or column)
            def defColVal = ncube.getColumnDefault(coord)
            String colorClass
            if (defColVal == null)
            {
                colorClass = 'def-cell-color'
                defColVal = ncube.defaultCellValue
            }
            else
            {
                colorClass = 'def-cell-col-color'
            }

            if (defColVal instanceof CommandCell)
            {
                final CommandCell cmd = defColVal as CommandCell
                if (hasContent(cmd.url))
                {
                    s.append("""cell cell-url"><a class="${colorClass}" href="#">${cmd.url}</a>""")
                }
                else
                {
                    s.append("""cell cell-code ${colorClass}">""")
                    s.append(escapeHTML(getCellValueAsString(defColVal)))
                }
            }
            else if (defColVal != null)
            {   // not null
                s.append("""cell ${colorClass}">""")
                s.append(escapeHTML(getCellValueAsString(defColVal)))
            }
            else
            {
                s.append('cell">')
            }
        }
        s.append('</td>\n')
    }

    /**
     * @return passed in value as String to be displayed inside HTML cell.
     */
    static String getCellValueAsString(Object cellValue)
    {
        if (cellValue == null)
        {
            return 'null'
        }
        boolean isArray = cellValue.class.array

        if (cellValue instanceof Date || cellValue instanceof String || cellValue instanceof Number)
        {
            return CellInfo.formatForDisplay((Comparable) cellValue)
        }
        else if (cellValue instanceof CommandCell)
        {
            CommandCell cmd = cellValue as CommandCell
            if (cmd.url == null)
            {
                return (cellValue as CommandCell).cmd
            }
            else
            {
                return cmd.url
            }
        }
        else if (cellValue instanceof Boolean || cellValue instanceof Character)
        {
            return String.valueOf(cellValue)
        }
        else if (cellValue instanceof Point2D || cellValue instanceof Point3D || cellValue instanceof LatLon)
        {
            return cellValue.toString()
        }
        else if (cellValue instanceof Range)
        {
            return cellValue.toString()
        }
        else if (cellValue instanceof RangeSet)
        {
            return cellValue.toString()
        }
        else if (cellValue instanceof byte[])
        {
            return encode(cellValue as byte[])
        }
        else if (isArray && isPrimitive(cellValue.class.componentType))
        {
            final StringBuilder str = new StringBuilder()
            str.append('[')
            final int len = Array.getLength(cellValue)
            final int len1 = len - 1

            for (int i = 0; i < len; i++)
            {
                Object elem = Array.get(cellValue, i)
                str.append(elem.toString())
                if (i < len1)
                {
                    str.append(', ')
                }
            }
            str.append(']')
            return str.toString()
        }
        else if (isArray && ([] as Object[]).class == cellValue.class)
        {
            final StringBuilder str = new StringBuilder()
            str.append('[')
            final int len = Array.getLength(cellValue)
            final int len1 = len - 1

            for (int i = 0; i < len; i++)
            {
                Object elem = Array.get(cellValue, i)
                str.append(escapeHTML(getCellValueAsString(elem)))  // recursive
                if (i < len1)
                {
                    str.append(', ')
                }
            }
            str.append(']')
            return str.toString()
        }
        else
        {
            try
            {
                return JsonWriter.objectToJson(cellValue)
            }
            catch (IOException e)
            {
                throw new IllegalStateException('Error with simple JSON format', e)
            }
        }
    }

    static String escapeHTML(String s)
    {
        if (s == null)
        {
            return ''
        }
        int len = s.length()
        StringBuilder out = new StringBuilder(len)

        for (int i = 0; i < len; i++)
        {
            char c = s.charAt(i)
            if (c > 127 || c == ('"' as char) || c == ('<' as char) || c == ('>' as char) || c == ('&' as char))
            {
                out.append('&#')
                out.append(c as int)
                out.append(';')
            }
            else
            {
                out.append(c)
            }
        }
        return out.toString()
    }

    private static String makeCellId(Collection<Long> colIds)
    {
        final StringBuilder s = new StringBuilder()
        final Iterator<Long> i = colIds.iterator()

        while (i.hasNext())
        {
            s.append(i.next())
            if (i.hasNext())
            {
                s.append('_')
            }
        }

        s.toString()
    }
}
