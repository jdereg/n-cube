package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.AxisOverlapException
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic
import org.junit.Test

import java.security.SecureRandom

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.ncube.ReferenceAxisLoader.*
import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotEquals
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * NCube Axis Tests
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestAxis extends NCubeBaseTest
{
    private static boolean isValidPoint(Axis axis, Comparable value)
    {
        try
        {
            axis.addColumn value
            return true
        }
        catch (AxisOverlapException ignored)
        {
            return false
        }
    }

    @Test
    void testAxisNameChange()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)
        axis.name = 'bar'
        assert 'bar' == axis.name
    }

    @Test
    void testRemoveMetaPropertyWhenMetaPropertiesAreNull()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)
        assertNull axis.removeMetaProperty('foo')
    }

    @Test
    void testRemoveMetaProperty()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)

        Map map = [foo:'bar','bar':'baz'] as Map
        axis.addMetaProperties map

        assert 'bar' == axis.getMetaProperty('foo')
        assert 'bar' == axis.removeMetaProperty('foo')
        assertNull axis.getMetaProperty('foo')
    }

    @Test
    void testClearMetaProperties()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)

        Map map = new HashMap()
        map.put('foo', 'bar')
        map.put('bar', 'baz')
        axis.addMetaProperties(map)

        assert 'bar' == axis.getMetaProperty('foo')
        assert 'baz' == axis.getMetaProperty('bar')

        axis.clearMetaProperties()

        assertNull axis.getMetaProperty('foo')
        assertNull axis.getMetaProperty('bar')
        assertNull axis.removeMetaProperty('foo')
    }

    @Test
    void testGetMetaPropertyWhenMetaPropertiesAreNull()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)
        assertNull axis.getMetaProperty('foo')
    }


    @Test
    void testConvertStringToColumnValueWithEmptyString()
    {
        Axis axis = new Axis('test axis', AxisType.DISCRETE, AxisValueType.LONG, true)
        assert 0L == axis.standardizeColumnValue('')
    }

    @Test
    void testConvertStringToColumnValueWithNull()
    {
        Axis axis = new Axis('test axis', AxisType.DISCRETE, AxisValueType.LONG, true)
        assert null == axis.standardizeColumnValue(null)
    }

    @Test(expected = IllegalArgumentException.class)
    void testConvertStringToColumnValueWihInvalidRangeDefinition()
    {
        Axis axis = new Axis('test axis', AxisType.SET, AxisValueType.LONG, true)
        axis.standardizeColumnValue('[[5]]')
    }

    @Test(expected = IllegalArgumentException.class)
    void testConvertStringToColumnValueWihRangeException()
    {
        Axis axis = new Axis('test axis', AxisType.SET, AxisValueType.LONG, true)
        axis.standardizeColumnValue('[null]')
    }

    @Test
    void testStandardizeColumnValueErrorHandling()
    {
        Axis states = NCubeBuilder.statesAxis
        assert null == states.standardizeColumnValue(null)
    }

    @Test
    void testCONDITIONnoSort()
    {
        Axis axis = new Axis('sorted', AxisType.RULE, AxisValueType.EXPRESSION, true, Axis.SORTED)
        assert axis.columnOrder == Axis.DISPLAY

        axis = new Axis('sorted', AxisType.RULE, AxisValueType.BIG_DECIMAL, true, Axis.DISPLAY)
        assert axis.valueType == AxisValueType.EXPRESSION

        axis = new Axis('sorted', AxisType.RULE, AxisValueType.EXPRESSION, false, Axis.DISPLAY)
        try
        {
            axis.addColumn 10
            fail 'should not make it here'
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('rule axis')
            assert e.message.toLowerCase().contains('commandcell')
        }

        axis = new Axis('sorted', AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY)
        assert null == axis.findColumn(null)
    }

    @Test
    void testAxisValueOverlap()
    {
        Axis axis = new Axis('test axis', AxisType.DISCRETE, AxisValueType.LONG, true)
        axis.addColumn 0
        axis.addColumn 10
        axis.addColumn 100

        assert isValidPoint(axis, -1)
        assert !isValidPoint(axis, 0)
        assert !isValidPoint(axis, 10)
        assert isValidPoint(axis, 11)
        assert !isValidPoint(axis, 100)
        assert isValidPoint(axis, 101)

        try
        {
            axis.addColumn(new Range(3, 9))
            fail 'should not make it here'
        }
        catch (IllegalArgumentException expected)
        {
            expected.message.toLowerCase().contains("unsupported value type")
        }

        axis = new Axis('test axis', AxisType.DISCRETE, AxisValueType.STRING, true)
        axis.addColumn 'echo'
        axis.addColumn 'juliet'
        axis.addColumn 'tango'

        assert isValidPoint(axis, 'alpha')
        assert !isValidPoint(axis, 'echo')
        assert !isValidPoint(axis, 'juliet')
        assert isValidPoint(axis, 'kilo')
        assert !isValidPoint(axis, 'tango')
        assert isValidPoint(axis, 'uniform')

        try
        {
            axis.addColumn new Range(3, 9)
            fail 'should not make it here'
        }
        catch (IllegalArgumentException expected)
        {
            expected.message.toLowerCase().contains("unsupported value type")
        }
    }

    @Test
    void testRangeOrderSorted()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, false, Axis.SORTED)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new Range(0L, 18L)
        assert cols[1].value == new Range(18L, 30L)
        assert cols[2].value == new Range(65L, 80L)
        assert cols.size() == 3
    }

    @Test
    void testRangeOrderDisplay()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, false, Axis.DISPLAY)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new Range(65L, 80L)
        assert cols[1].value == new Range(18L, 30L)
        assert cols[2].value == new Range(0L, 18L)
        assert cols.size() == 3
    }

    @Test
    void testRangeOrderSortedDefault()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true, Axis.SORTED)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new Range(0L, 18L)
        assert cols[1].value == new Range(18L, 30L)
        assert cols[2].value == new Range(65L, 80L)
        assert cols[3].value == null
        assert cols.size() == 4
    }

    @Test
    void testRangeOrderDisplayDefault()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new Range(65L, 80L)
        assert cols[1].value == new Range(18L, 30L)
        assert cols[2].value == new Range(0L, 18L)
        assert cols[3].value == null
        assert cols.size() == 4
    }

    @Test
    void testRangeOrderSortedWithoutDefault()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true, Axis.SORTED)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columnsWithoutDefault
        assert cols[0].value == new Range(0L, 18L)
        assert cols[1].value == new Range(18L, 30L)
        assert cols[2].value == new Range(65L, 80L)
        assert cols.size() == 3
    }

    @Test
    void testRangeOrderDisplayWithoutDefault()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columnsWithoutDefault
        assert cols[0].value == new Range(65L, 80L)
        assert cols[1].value == new Range(18L, 30L)
        assert cols[2].value == new Range(0L, 18L)
        assert cols.size() == 3
    }

    @Test
    void testRangeSetOrderSorted()
    {
        Axis axis = new Axis("Age", AxisType.SET, AxisValueType.LONG, false, Axis.SORTED)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(18)
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new RangeSet(new Range(0L, 18L))
        assert cols[1].value == new RangeSet(18L)
        assert cols[2].value == new RangeSet(new Range(65L, 80L))
        assert cols.size() == 3
    }

    @Test
    void testRangeSetOrderDisplay()
    {
        Axis axis = new Axis("Age", AxisType.SET, AxisValueType.LONG, false, Axis.DISPLAY)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(18)
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new RangeSet(new Range(65L, 80L))
        assert cols[1].value == new RangeSet(18L)
        assert cols[2].value == new RangeSet(new Range(0L, 18L))
        assert cols.size() == 3
    }

    @Test
    void testRangeSetOrderSortedDefault()
    {
        Axis axis = new Axis("Age", AxisType.SET, AxisValueType.LONG, true, Axis.SORTED)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(18)
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new RangeSet(new Range(0L, 18L))
        assert cols[1].value == new RangeSet(18L)
        assert cols[2].value == new RangeSet(new Range(65L, 80L))
        assert cols[3].value == null
        assert cols.size() == 4
    }

    @Test
    void testRangeSetOrderDisplayDefault()
    {
        Axis axis = new Axis("Age", AxisType.SET, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(18)
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columns
        assert cols[0].value == new RangeSet(new Range(65L, 80L))
        assert cols[1].value == new RangeSet(18L)
        assert cols[2].value == new RangeSet(new Range(0L, 18L))
        assert cols[3].value == null
        assert cols.size() == 4
    }

    @Test
    void testRangeSetOrderSortedWithoutDefault()
    {
        Axis axis = new Axis("Age", AxisType.SET, AxisValueType.LONG, true, Axis.SORTED)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(18)
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columnsWithoutDefault
        assert cols[0].value == new RangeSet(new Range(0L, 18L))
        assert cols[1].value == new RangeSet(18L)
        assert cols[2].value == new RangeSet(new Range(65L, 80L))
        assert cols.size() == 3
    }

    @Test
    void testRangeSetOrderDisplayWithoutDefault()
    {
        Axis axis = new Axis("Age", AxisType.SET, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(new Range(65, 80))
        axis.addColumn(18)
        axis.addColumn(new Range(0, 18))

        List<Column> cols = axis.columnsWithoutDefault
        assert cols[0].value == new RangeSet(new Range(65L, 80L))
        assert cols[1].value == new RangeSet(18L)
        assert cols[2].value == new RangeSet(new Range(0L, 18L))
        assert cols.size() == 3
    }

    @Test
    void testDiscreteOrderSorted()
    {
        Axis axis = new Axis("Age", AxisType.DISCRETE, AxisValueType.LONG, false, Axis.SORTED)
        axis.addColumn(65)
        axis.addColumn(18)
        axis.addColumn(0)

        List<Column> cols = axis.columns
        assert cols[0].value == 0L
        assert cols[1].value == 18L
        assert cols[2].value == 65L
        assert cols.size() == 3
    }

    @Test
    void testDiscreteOrderDisplay()
    {
        Axis axis = new Axis("Age", AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY)
        axis.addColumn(65)
        axis.addColumn(18)
        axis.addColumn(0)

        List<Column> cols = axis.columns
        assert cols[0].value == 65L
        assert cols[1].value == 18L
        assert cols[2].value == 0L
        assert cols.size() == 3
    }

    @Test
    void testDiscreteOrderSortedDefault()
    {
        Axis axis = new Axis("Age", AxisType.DISCRETE, AxisValueType.LONG, true, Axis.SORTED)
        axis.addColumn(65)
        axis.addColumn(18)
        axis.addColumn(0)

        List<Column> cols = axis.columns
        assert cols[0].value == 0L
        assert cols[1].value == 18L
        assert cols[2].value == 65L
        assert cols[3].value == null
        assert cols.size() == 4
    }

    @Test
    void testDiscreteOrderDisplayDefault()
    {
        Axis axis = new Axis("Age", AxisType.DISCRETE, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(65)
        axis.addColumn(18)
        axis.addColumn(0)

        List<Column> cols = axis.columns
        assert cols[0].value == 65L
        assert cols[1].value == 18L
        assert cols[2].value == 0L
        assert cols[3].value == null
        assert cols.size() == 4
    }

    @Test
    void testDiscreteOrderSortedWithoutDefault()
    {
        Axis axis = new Axis("Age", AxisType.DISCRETE, AxisValueType.LONG, true, Axis.SORTED)
        axis.addColumn(65)
        axis.addColumn(18)
        axis.addColumn(0)

        List<Column> cols = axis.columnsWithoutDefault
        assert cols[0].value == 0L
        assert cols[1].value == 18L
        assert cols[2].value == 65L
        assert cols.size() == 3
    }

    @Test
    void testDiscreteOrderDisplayWithoutDefault()
    {
        Axis axis = new Axis("Age", AxisType.DISCRETE, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(65)
        axis.addColumn(18)
        axis.addColumn(0)

        List<Column> cols = axis.columnsWithoutDefault
        assert cols[0].value == 65L
        assert cols[1].value == 18L
        assert cols[2].value == 0L
        assert cols.size() == 3
    }

    @Test
    void testRangeOverlap()
    {
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true)
        axis.addColumn(new Range(0, 18))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(65, 80))

        assertFalse(isValidRange(axis, new Range(17, 20)))
        assertFalse(isValidRange(axis, new Range(18, 20)))
        assertTrue(isValidRange(axis, new Range(30, 65)))
        assertFalse(isValidRange(axis, new Range(40, 50)))
        assertTrue(isValidRange(axis, new Range(80, 100)))
        assertFalse(isValidRange(axis, new Range(-150, 150)))
        assertTrue(axis.size() == 6)

        // Edge and Corner cases
        try
        {
            axis.addColumn(17)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('only add range value')
        }

        try
        {
            axis.addColumn(new Range(-10, -10))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('low and high must be different')
        }

        // Using Long's as Dates (Longs, Date, or Calendar allowed)
        axis = new Axis("Age", AxisType.RANGE, AxisValueType.DATE, true)
        axis.addColumn(new Range(0L, 18L))
        axis.addColumn(new Range(18L, 30L))
        axis.addColumn(new Range(65L, 80L))

        assertFalse(isValidRange(axis, new Range(17L, 20L)))
        assertFalse(isValidRange(axis, new Range(18L, 20L)))
        assertTrue(isValidRange(axis, new Range(30L, 65L)))
        assertFalse(isValidRange(axis, new Range(40L, 50L)))
        assertTrue(isValidRange(axis, new Range(80L, 100L)))
        assertFalse(isValidRange(axis, new Range(-150L, 150L)))
        assertTrue(axis.size() == 6)

        // Edge and Corner cases
        try
        {
            axis.addColumn(17)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('only add range value')
        }

        try
        {
            axis.addColumn(new Range(-10L, -10L))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('low and high must be different')
        }

        axis = new Axis("Age", AxisType.RANGE, AxisValueType.DOUBLE, true)
        axis.addColumn(new Range(0, 18))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(65, 80))

        assertFalse(isValidRange(axis, new Range(17, 20)))
        assertFalse(isValidRange(axis, new Range(18, 20)))
        assertTrue(isValidRange(axis, new Range(30, 65)))
        assertFalse(isValidRange(axis, new Range(40, 50)))
        assertTrue(isValidRange(axis, new Range(80, 100)))
        assertFalse(isValidRange(axis, new Range(-150, 150)))
        assertTrue(axis.size() == 6)

        // Edge and Corner cases
        try
        {
            axis.addColumn(17)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('only add range value')
        }

        try
        {
            axis.addColumn(new Range(-10, -10))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('low and high must be different')
        }

        axis = new Axis("Age", AxisType.RANGE, AxisValueType.BIG_DECIMAL, true)
        axis.addColumn(new Range(0, 18))
        axis.addColumn(new Range(18, 30))
        axis.addColumn(new Range(65, 80))

        assertFalse(isValidRange(axis, new Range(17, 20)))
        assertFalse(isValidRange(axis, new Range(18, 20)))
        assertTrue(isValidRange(axis, new Range(30, 65)))
        assertFalse(isValidRange(axis, new Range(40, 50)))
        assertTrue(isValidRange(axis, new Range(80, 100)))
        assertFalse(isValidRange(axis, new Range(-150, 150)))
        assertTrue(axis.size() == 6)

        // Edge and Corner cases
        try
        {
            axis.addColumn(17)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('only add range value')
        }

        try
        {
            axis.addColumn(new Range(-10, -10))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('low and high must be different')
        }
    }

    @Test
    void testAxisInsertAtFront()
    {
        Axis states = new Axis('States', AxisType.SET, AxisValueType.STRING, false, Axis.SORTED)
        RangeSet set = new RangeSet('GA')
        set.add 'OH'
        set.add 'TX'
        states.addColumn set
        set = new RangeSet('AL')
        set.add 'WY'
        states.addColumn set
        assert states.size() == 2
        Column col = states.columns[0]
        assert col.value == set      // added first (because of SORTED)
    }

    @Test
    void testAxisLongType()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY)
        axis.addColumn 1
        axis.addColumn 2L
        axis.addColumn 3 as Byte
        axis.addColumn 4 as Short
        axis.addColumn '5'
        axis.addColumn new BigDecimal('6')
        axis.addColumn new BigInteger('7')
        assert AxisType.DISCRETE.equals(axis.type)
        assert AxisValueType.LONG.equals(axis.valueType)
        assert axis.size() == 7

        assert axis.columns[0].value instanceof Long
        assert axis.columns[1].value instanceof Long
        assert axis.columns[2].value instanceof Long
        assert axis.columns[3].value instanceof Long
        assert axis.columns[4].value instanceof Long
        assert axis.columns[5].value instanceof Long
        assert axis.columns[6].value instanceof Long

        assert axis.columns[0].value == 1
        assert axis.columns[1].value == 2
        assert axis.columns[2].value == 3
        assert axis.columns[3].value == 4
        assert axis.columns[4].value == 5
        assert axis.columns[5].value == 6
        assert axis.columns[6].value == 7
    }

    @Test
    void testAddingNullToAxis()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)
        axis.addColumn((Comparable)null)    // Add default column
        assert axis.hasDefaultColumn()
        try
        {
            axis.addColumn((Comparable)null)
            fail 'should throw exception'
        }
        catch (IllegalArgumentException expected)
        {
            assert expected.message.contains('not')
            assert expected.message.contains('add')
            assert expected.message.contains('default')
            assert expected.message.contains('already')
        }
        axis.deleteColumn null
        assert axis.defaultColumn == null
    }

    @Test
    void testAxisGetValues()
    {
        NCube ncube = new NCube('foo')
        ncube.addAxis NCubeBuilder.longDaysOfWeekAxis
        ncube.addAxis NCubeBuilder.longMonthsOfYear
        ncube.addAxis NCubeBuilder.getOddAxis(true)
        Axis axis = (Axis) ncube.axes.get(0)
        List values = axis.columns
        assert values.size() == 7
        assert TestNCube.countMatches(ncube.toHtml(), '<tr') == 44
    }

    @Test
    void testAxisCaseInsensitivity()
    {
        NCube<String> ncube = new NCube<String>('TestAxisCase')
        Axis gender = NCubeBuilder.getGenderAxis true
        ncube.addAxis gender
        Axis gender2 = new Axis('gender', AxisType.DISCRETE, AxisValueType.STRING, true)

        try
        {
            ncube.addAxis gender2
            fail 'should throw exception'
        }
        catch (IllegalArgumentException expected)
        {
            assert expected.message.contains('axis')
            assert expected.message.contains('already')
            assert expected.message.contains('exists')
        }

        Map coord = [gendeR:null] as Map
        ncube.setCell '1', coord
        assert '1'.equals(ncube.getCell(coord))

        coord.GendeR = 'Male'
        ncube.setCell '2', coord
        assert '2'.equals(ncube.getCell(coord))

        coord.GENdeR = 'Female'
        ncube.setCell '3', coord
        assert '3'.equals(ncube.getCell(coord))

        Axis axis = ncube.getAxis 'genDER'
        assert axis.name == 'Gender'
        ncube.deleteAxis 'GeNdEr'
        assert ncube.numDimensions == 0
    }

    @Test
    void testRangeSetAxisErrors()
    {
        Axis age = new Axis('Age', AxisType.SET, AxisValueType.LONG, true)
        RangeSet set = new RangeSet(1)
        set.add 3.0
        set.add new Range(10, 20)
        set.add 25
        age.addColumn set

        set = new RangeSet(2)
        set.add 20L
        set.add 35 as Byte
        age.addColumn set

        try
        {
            set = new RangeSet(12)
            age.addColumn(set)
            fail('should throw exception')
        }
        catch (AxisOverlapException expected)
        {
            assert expected.message.contains('RangeSet')
            assert expected.message.contains('overlap')
            assert expected.message.contains('exist')
        }

        try
        {
            set = new RangeSet(15)
            age.addColumn set
            fail 'should throw exception'
        }
        catch (AxisOverlapException expected)
        {
            assert expected.message.contains('RangeSet')
            assert expected.message.contains('overlap')
            assert expected.message.contains('exist')
        }

        try
        {
            set = new RangeSet(new Comparable() { int compareTo(Object o) { return 0 }}) // not a valid type for a LONG axis
            age.addColumn set
            fail()
        }
        catch (Exception expected)
        {
            assert expected instanceof IllegalArgumentException
            assert expected.message.toLowerCase().contains('unsupported value type')
        }

        RangeSet a = new RangeSet()
        RangeSet b = new RangeSet()
        assert a.compareTo(b) == 0
    }

    @Test
    void testDeleteColumnFromRangeSetAxis()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube4.json')
        ncube.deleteColumn('code', 'b')
        Axis axis = ncube.getAxis('code') as Axis
        assert axis.id != 0
        assert axis.columns.size() == 2
        axis.deleteColumn('o')
        assert axis.columns.size() == 1
        assert axis.size() == 1
        assertNull axis.deleteColumnById(9)
    }

    @Test
    void testDupeIdsOnAxis()
    {
        try
        {
            ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'idBasedCubeError2.json')
            fail('should not make it here')
        }
        catch (AxisOverlapException e)
        {
            assert e.message.toLowerCase().contains('range overlap')
        }
    }

    @Test
    void testAddDefaultToNearestAxis()
    {
        Axis nearest = new Axis('points', AxisType.NEAREST, AxisValueType.COMPARABLE, false)
        try
        {
            nearest.addColumn((Comparable)null)
            fail 'should not make it here'
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains("cannot add default column")
            assert e.message.toLowerCase().contains("to nearest axis")
        }
    }

    @Test
    void testMetaProperties()
    {
        Axis c = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, true)
        assertNull c.metaProperties.get('foo')

        c.clearMetaProperties()
        c.setMetaProperty('foo', 'bar')
        assert 'bar' == c.metaProperties.get('foo')

        c.clearMetaProperties()
        assertNull c.metaProperties.get('foo')

        c.clearMetaProperties()
        c.addMetaProperties([BaZ:'qux'] as Map)
        assert 'qux' == c.metaProperties.get('baz')
    }

    @Test
    void testToString()
    {
        Axis axis = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false)
        assert 'Axis: foo [DISCRETE, LONG, no-default-column, sorted]' == axis.toString()

        Axis c = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, true)
        assertNull c.metaProperties.get('foo')
        c.setMetaProperty 'foo', 'bar'

        String x = c.toString().toLowerCase()
        assert x.contains('discrete, string, default-column, sorted')
        assert x.contains('[foo:bar]')
    }

    @Test
    void testConvertDiscreteColumnValue()
    {
        // Strings
        Axis states = NCubeBuilder.statesAxis
        assert states.standardizeColumnValue('OH') == 'OH'

        // Longs
        Axis longs = new Axis('longs', AxisType.DISCRETE, AxisValueType.LONG, false)
        assert -1L == longs.standardizeColumnValue('-1')
        assert 0L == longs.standardizeColumnValue('0')
        assert 1L == longs.standardizeColumnValue('1')
        assert 12345678901234L == longs.standardizeColumnValue('12345678901234')
        assert -12345678901234L == longs.standardizeColumnValue('-12345678901234')
        try
        {
            longs.standardizeColumnValue '-12345.678901234-foo'
            fail 'should not make it here'
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('could not be converted')
        }

        // BigDecimals
        Axis bigDec = new Axis('bigDec', AxisType.DISCRETE, AxisValueType.BIG_DECIMAL, false)
        assert -1g == bigDec.standardizeColumnValue('-1')
        assert 0g == bigDec.standardizeColumnValue('0')
        assert 1g == bigDec.standardizeColumnValue('1')
        assert 12345678901234g == bigDec.standardizeColumnValue('12345678901234')
        assert -12345678901234g ==  bigDec.standardizeColumnValue('-12345678901234')
        assert -12345.678901234g == bigDec.standardizeColumnValue('-12345.678901234')

        // Doubles
        Axis doubles = new Axis('bigDec', AxisType.DISCRETE, AxisValueType.DOUBLE, false)
        assertEquals(-1.0d, (double) doubles.standardizeColumnValue('-1'), 0.000001d)
        assertEquals(0.0d, (double) doubles.standardizeColumnValue('0'), 0.000001d)
        assertEquals(1.0d, (double) doubles.standardizeColumnValue('1'), 0.00001d)
        assertEquals(12345678901234.0d, (double) doubles.standardizeColumnValue('12345678901234'), 0.00001d)
        assertEquals(-12345678901234.0d, (double) doubles.standardizeColumnValue('-12345678901234'), 0.00001d)
        assertEquals(-12345.678901234d, (double) doubles.standardizeColumnValue('-12345.678901234'), 0.00001d)

        // Dates
        Axis dates = new Axis('Dates', AxisType.DISCRETE, AxisValueType.DATE, false)
        Calendar cal = Calendar.instance
        cal.clear()
        cal.set(2014, 0, 18, 0, 0, 0)
        assert dates.standardizeColumnValue('1/18/2014') == cal.time
        cal.clear()
        cal.set(2014, 6, 9, 13, 10, 58)
        assert dates.standardizeColumnValue('2014 Jul 9 13:10:58') == cal.time
        try
        {
            dates.standardizeColumnValue('2014 Ju1y 9 13:10:58')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('could not be converted')
        }

        // Expression
        Axis exp = new Axis('Condition', AxisType.RULE, AxisValueType.EXPRESSION, false, Axis.DISPLAY)
        assert new GroovyExpression('println \'Hello\'', null, false) == exp.standardizeColumnValue('println \'Hello\'')

        // Comparable (this allows user to create Java Comparable object instances as Column values!
        Axis comp = new Axis('Comparable', AxisType.DISCRETE, AxisValueType.COMPARABLE, false)
        cal.clear()
        cal.set 2014, 0, 18, 16, 26, 0
        String json = JsonWriter.objectToJson cal
        assert cal == comp.standardizeColumnValue(json)
    }

    @Test
    void testRangeParsing()
    {
        Axis axis = new Axis('ages', AxisType.RANGE, AxisValueType.LONG, true, Axis.SORTED)
        Range range = (Range) axis.standardizeColumnValue('10,20')
        assert 10L == range.low
        assert 20L == range.high

        range = (Range) axis.standardizeColumnValue('  10 ,\t20  \n')
        assert 10L == range.low
        assert 20L == range.high

        axis = new Axis('ages', AxisType.RANGE, AxisValueType.DATE, false)
        range = (Range) axis.standardizeColumnValue('12/25/2014, 12/25/2016')
        Calendar calendar = Calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == range.low
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == range.high

        range = (Range) axis.standardizeColumnValue('Dec 25 2014, 12/25/2016')
        calendar = calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == range.low
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == range.high

        range = (Range) axis.standardizeColumnValue('Dec 25 2014, Dec 25 2016')
        calendar = calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == range.low
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == range.high

        range = (Range) axis.standardizeColumnValue('12/25/2014, Dec 25 2016')
        calendar = calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == range.low
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == range.high
    }

    @Test
    void testRangeWithBrackets()
    {
        Axis axis = new Axis('brackets', AxisType.RANGE, AxisValueType.LONG, true, Axis.SORTED)
        Range range = (Range) axis.standardizeColumnValue('[10,20]')
        assert 10L == range.low
        assert 20L == range.high

        range = (Range) axis.standardizeColumnValue('  [  10 ,  20  ]  ')
        assert 10L == range.low
        assert 20L == range.high
    }

    @Test
    void testDiscreteSetParsing()
    {
        Axis axis = new Axis('ages', AxisType.SET, AxisValueType.LONG, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('10,20')
        assert 10L == set.get(0)
        assert 20L == set.get(1)

        set = (RangeSet) axis.standardizeColumnValue('  10 ,\t20  \n')
        assert 10L == set.get(0)
        assert 20L == set.get(1)

        // Support no outer brackets
        axis.standardizeColumnValue('10, 20')
        assert 10L == set.get(0)
        assert 20L == set.get(1)

        axis = new Axis('ages', AxisType.SET, AxisValueType.DATE, false)
        set = (RangeSet) axis.standardizeColumnValue(' "12/25/2014", "12/25/2016"')
        Calendar calendar = Calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == set.get(0)
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == set.get(1)

        set = (RangeSet) axis.standardizeColumnValue(' "Dec 25th 2014", "Dec 25th 2016"')
        calendar = calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == set.get(0)
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == set.get(1)
    }

    @Test
    void testRangeSetParsing()
    {
        Axis axis = new Axis('ages', AxisType.SET, AxisValueType.LONG, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('[10,20]')
        Range range = (Range) set.get(0)
        assert 10L == range.low
        assert 20L == range.high

        set = (RangeSet) axis.standardizeColumnValue(' [  10 ,\t20  \n] ')
        range = (Range) set.get(0)
        assert 10L == range.low
        assert 20L == range.high

        axis = new Axis('ages', AxisType.SET, AxisValueType.DATE, false)
        set = (RangeSet) axis.standardizeColumnValue('[ "12/25/2014", "12/25/2016"]')
        range = (Range) set.get(0)
        Calendar calendar = Calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == range.low
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == range.high
    }

    @Test
    void testRangeAndDiscreteSetParsing()
    {
        Axis axis = new Axis('ages', AxisType.SET, AxisValueType.LONG, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('[10,20], 1979')
        Range range = (Range) set.get(0)
        assert 10L == range.low
        assert 20L == range.high
        assert 1979L == set.get(1)

        set = (RangeSet) axis.standardizeColumnValue(' [  10 ,\t20  \n] , 1979 ')
        range = (Range) set.get(0)
        assert 10L == range.low
        assert 20L == range.high
        assert 1979L == set.get(1)

        axis = new Axis('ages', AxisType.SET, AxisValueType.DATE, false)
        set = (RangeSet) axis.standardizeColumnValue('[ "12/25/2014", "12/25/2016"], "12/25/2020"')
        range = (Range) set.get(0)
        Calendar calendar = Calendar.instance
        calendar.clear()
        calendar.set 2014, 11, 25
        assert calendar.time == range.low
        calendar.clear()
        calendar.set 2016, 11, 25
        assert calendar.time == range.high
        calendar.clear()
        calendar.set 2020, 11, 25
        assert calendar.time == set.get(1)
    }

    @Test
    void testRangeAndDiscreteSetParsing2()
    {
        Axis axis = new Axis('ages', AxisType.SET, AxisValueType.BIG_DECIMAL, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('[10,20], 1979')
        Range range = (Range) set.get(0)
        assert 10g == range.low
        assert 20g == range.high
        assert 1979g == set.get(1)

        set = (RangeSet) axis.standardizeColumnValue(' [  10.0 ,\t20  \n] , 1979 ')
        range = (Range) set.get(0)
        assert 10g == range.low
        assert 20g == range.high
        assert 1979g == set.get(1)
    }

    @Test
    void testNearestWithDoubles()
    {
        Axis axis = new Axis('loc', AxisType.NEAREST, AxisValueType.COMPARABLE, false)
        LatLon latlon = (LatLon) axis.standardizeColumnValue('1.0, 2.0')
        assertEquals 1.0d, latlon.lat, 0.00001d
        assertEquals 2.0d, latlon.lon, 0.00001d

        latlon = (LatLon) axis.standardizeColumnValue('1,2')
        assertEquals 1.0d, latlon.lat, 0.00001d
        assertEquals 2.0d, latlon.lon, 0.00001d

        latlon = (LatLon) axis.standardizeColumnValue('-1,-2')
        assertEquals(-1.0d, latlon.lat, 0.00001d)
        assertEquals(-2.0d, latlon.lon, 0.001d)

        axis = new Axis('loc', AxisType.NEAREST, AxisValueType.COMPARABLE, false)
        Point3D pt3d = (Point3D) axis.standardizeColumnValue('1.0, 2.0, 3.0')
        assertEquals(1.0d, pt3d.x, 0.00001d)
        assertEquals(2.0d, pt3d.y, 0.00001d)
        assertEquals(3.0d, pt3d.z, 0.00001d)
    }

    @Test
    void testAddAxisSameWayAsUI()
    {
        Axis axis = new Axis('loc', AxisType.SET, AxisValueType.LONG, true)
        Axis axis2 = new Axis('loc', AxisType.SET, AxisValueType.LONG, false)
        Column colAdded = axis2.addColumn('[1, 2]')
        colAdded.id = -1
        axis.updateColumns(axis2.columns)

        assert 2 == axis.columns.size()
        Column col = axis.columnsWithoutDefault.get(0)
        RangeSet rs = new RangeSet(new Range(1L, 2L))
        assert rs == col.value
    }

    @Test
    void testUpdateColumnWithMetaPropertyName()
    {
        Axis axis1 = new Axis('loc', AxisType.SET, AxisValueType.LONG, true)
        axis1.addColumn('[1, 2]')
        Axis axis2 = new Axis('loc', AxisType.SET, AxisValueType.LONG, true)
        axis2.addColumn('[1, 2]')
        List<Column> cols = axis2.columnsWithoutDefault
        cols.get(0).id = axis1.columnsWithoutDefault.get(0).id
        cols.get(0).setMetaProperty('name', 'cheese')
        cols.get(0).setMetaProperty('foo', 'bar')
        axis1.updateColumns(axis2.columns)

        assert 2 == axis1.columns.size()
        Column col = axis1.columnsWithoutDefault.get(0)
        assert 'cheese' == col.getMetaProperty('name')
        assert 'bar' == col.getMetaProperty('foo')
    }

    @Test
    void testRemoveSetColumnWithMultipleRanges()
    {
        Axis axis = new Axis('loc', AxisType.SET, AxisValueType.LONG, false)
        RangeSet rs = new RangeSet()
        rs.add(new Range(10, 20))
        rs.add(new Range(30, 40))
        axis.addColumn(rs)
        rs = new RangeSet()
        rs.add(new Range(50, 60))
        axis.addColumn(rs)
        assert 2 == axis.columns.size()
        assert 3 == axis.rangeToCol.asMapOfRanges().size()
        axis.deleteColumn(15)
        assert 1 == axis.rangeToCol.asMapOfRanges().size()
        assert 1 == axis.columns.size()
    }

    @Test
    void testRemoveSetColumnWithMultipleDiscretes()
    {
        Axis axis = new Axis('loc', AxisType.SET, AxisValueType.LONG, false)
        RangeSet rs = new RangeSet()
        rs.add 20
        rs.add 30
        axis.addColumn rs
        rs = new RangeSet()
        rs.add 50
        axis.addColumn rs
        assert 2 == axis.columns.size()
        axis.deleteColumn 30
        assert 1 == axis.columns.size()
    }

    @Test
    void testAddAxisBadColumnIds()
    {
        Axis axis = new Axis('loc', AxisType.SET, AxisValueType.LONG, true)
        Axis axis2 = new Axis('loc', AxisType.SET, AxisValueType.LONG, true)
        axis2.addColumn('[1, 2]')
        try
        {
            axis.updateColumns(axis2.columns)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.contains('added')
            assert e.message.contains('negative')
            assert e.message.contains('values')
        }
    }

    @Test
    void testParseBadRange()
    {
        Axis axis = new Axis('foo', AxisType.RANGE, AxisValueType.LONG, false)
        try
        {
            axis.standardizeColumnValue('this is not a range')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.contains('not')
            assert e.message.contains('range')
        }
    }

    @Test
    void testParseBadSet()
    {
        Axis axis = new Axis('foo', AxisType.SET, AxisValueType.LONG, false)
        try
        {
            axis.standardizeColumnValue('[null, false]')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains("range value cannot be null")
        }

        try
        {
            axis.standardizeColumnValue('null, false')
            fail 'should not make it here'
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains("set cannot have null value inside")
        }
    }

    @Test
    void testFindNonExistentRuleName()
    {
        Axis axis = new Axis('foo', AxisType.RULE, AxisValueType.EXPRESSION, false, Axis.DISPLAY)
        try
        {
            axis.getRuleColumnsStartingAt('foo')
            fail 'should not make it here'
        }
        catch (CoordinateNotFoundException e)
        {
            assert "Rule named 'foo' matches no column names on the rule axis 'foo', and there is no default column." == e.message
            assert !e.cubeName
            assert !e.coordinate
            assert 'foo' == e.axisName
            assert 'foo' == e.value
        }
    }

    @Test
    void testFindRuleNameUsingNonString()
    {
        Axis axis = new Axis('foo', AxisType.RULE, AxisValueType.EXPRESSION, false, Axis.DISPLAY)
        try
        {
            axis.findColumn 25
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.contains('rule')
            assert e.message.toLowerCase().contains('only')
            assert e.message.contains('located')
            assert e.message.contains('name')
        }
    }

    @Test
    void testGetColumnsAndCoordinateFromIds()
    {
        NCube cube = NCubeBuilder.testNCube3D_Boolean

        Axis trailor = cube.getAxis("Trailers")
        Column t = trailor.findColumn("M2A")

        Axis vehicles = cube.getAxis("Vehicles")
        Column v = vehicles.findColumn("van")

        Axis bu = cube.getAxis("BU")
        Column b = bu.findColumn("SHS")

        Set<Long> longCoord = new LinkedHashSet<>()
        longCoord.add(t.id as Long)
        longCoord.add(v.id as Long)
        longCoord.add(b.id as Long)

        // Make sure all columns are bound correctly
        def coord = new CaseInsensitiveMap()
        Set<Long> boundCols = cube.ensureFullCoordinate(longCoord)
        for (Long colId : boundCols)
        {
            assertTrue(colId == t.id || colId == v.id || colId == b.id)
        }

        for (Map.Entry<String, CellInfo> entry : coord.entrySet())
        {
            CellInfo info = entry.value
            assertTrue("M2A".equals(info.value) || "van".equals(info.value) || "SHS".equals(info.value))
        }
    }

    @Test
    void testSha1NotSensitiveToAxisNameCase()
    {
        NCube cube1 = new NCube("foo")
        NCube cube2 = new NCube("foo")
        NCube cube3 = new NCube("foo")
        Axis axis1 = new Axis("state", AxisType.DISCRETE, AxisValueType.BIG_DECIMAL, true, Axis.SORTED, cube1.maxAxisId)
        Axis axis2 = new Axis("STATE", AxisType.DISCRETE, AxisValueType.BIG_DECIMAL, true, Axis.SORTED, cube2.maxAxisId)
        Axis axis3 = new Axis("state", AxisType.DISCRETE, AxisValueType.BIG_DECIMAL, true, Axis.SORTED, cube3.maxAxisId)
        cube1.addAxis(axis1)
        cube2.addAxis(axis2)
        assertEquals(cube1.sha1(), cube2.sha1())

        cube3.addAxis(axis3)
        assertEquals(cube1.sha1(), cube3.sha1())
    }

    @Test
    void testNoDefaultColumn()
    {
        NCube<Boolean> ncube = NCubeBuilder.testNCube3D_Boolean

        def coord = [:]
        coord.put("Trailers", "S1A")
        coord.put("Vehicles", "car")
        coord.put("BU", "Agri")
        Boolean v = ncube.getCell(coord)
        assertNull(v)
        ncube.setCell(true, coord)
        v = ncube.getCell(coord)
        assertTrue(v)
        ncube.toHtml() // Use to test 3D visually

        try
        {
            coord.put("BU", "bogus")
            ncube.getCell(coord)
            fail("should throw exception")
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains("alue"))
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("found"))
            assertTrue(e.message.contains("axis"))
            assertEquals(ncube.name, e.cubeName)
            assertEquals(coord, e.coordinate)
            assertEquals("BU", e.axisName)
            assertEquals("bogus", e.value)
        }
    }

    @Test
    void testDefaultColumn()
    {
        NCube<Boolean> ncube = new NCube<Boolean>("Test.Default.Column")
        Axis axis = NCubeBuilder.getGenderAxis(true)
        ncube.addAxis(axis)

        def male = [:]
        male.put("Gender", "Male")
        def female = [:]
        female.put("Gender", "Female")
        Map nullGender = new HashMap()
        nullGender.put("Gender", null)

        ncube.setCell(true, male)
        ncube.setCell(false, female)
        ncube.setCell(true, nullGender)

        assertTrue(ncube.getCell(male))
        assertFalse(ncube.getCell(female))
        assertTrue(ncube.getCell(nullGender))

        ncube.setCell(false, male)
        ncube.setCell(true, female)
        ncube.setCell(null, nullGender)

        assertFalse(ncube.getCell(male))
        assertTrue(ncube.getCell(female))
        assertNull(ncube.getCell(nullGender))

        def coord = [:]
        coord.put("Gender", "missed")
        ncube.setCell(true, coord)
        coord.put("Gender", "yes missed")
        assertTrue(ncube.getCell(coord))
        assertTrue(TestNCube.countMatches(ncube.toHtml(), "<tr") == 4)
    }

    @Test
    void testNearestDate()
    {
        Axis points = new Axis("Date", AxisType.NEAREST, AxisValueType.DATE, false)
        points.addColumn(convertToDate("2000/01/01"))
        points.addColumn(convertToDate("2016/06/06"))
        points.addColumn(convertToDate("1970/01/01"))
        points.addColumn(convertToDate("2005/05/31"))
        points.addColumn(convertToDate("1991/10/05"))

        Column col = points.findColumn(convertToDate("1930/07/09"))
        assert col.toString() == '1970-01-01'

        col = points.findColumn(convertToDate("1969/12/31"))
        assert col.toString() == '1970-01-01'

        col = points.findColumn(convertToDate("1970/01/01"))
        assert col.toString() == '1970-01-01'

        col = points.findColumn(convertToDate("1970/01/02"))
        assert col.toString() == '1970-01-01'

        col = points.findColumn(convertToDate("1980/11/17"))
        assert col.toString() == '1970-01-01'

        col = points.findColumn(convertToDate("1980/11/18"))
        assert col.toString() == '1991-10-05'

        col = points.findColumn(convertToDate("2010/08/10"))
        assert col.toString() == '2005-05-31'

        col = points.findColumn(convertToDate("2016/06/05"))
        assert col.toString() == '2016-06-06'

        col = points.findColumn(convertToDate("2016/06/06"))
        assert col.toString() == '2016-06-06'

        col = points.findColumn(convertToDate("2016/06/07"))
        assert col.toString() == '2016-06-06'

        col = points.findColumn(convertToDate("2316/12/25"))
        assert col.toString() == '2016-06-06'
    }

    @Test
    void testNearestLogarithmic()
    {
        Axis points = new Axis("Point", AxisType.NEAREST, AxisValueType.DOUBLE, false)
        points.addColumn(100d)
        points.addColumn(10000d)
        points.addColumn(10d)
        points.addColumn(1000d)
        points.addColumn(0d)

        Column col = points.findColumn(-123456789012345678L)
        assert col.value == 0d

        col = points.findColumn(-1)
        assert col.value == 0d

        col = points.findColumn(-0.000001)
        assert col.value == 0d

        col = points.findColumn(0)
        assert col.value == 0d

        col = points.findColumn(0.000001)
        assert col.value == 0d

        col = points.findColumn(1)
        assert col.value == 0d

        col = points.findColumn(4)
        assert col.value == 0d

        col = points.findColumn(5)
        assert col.value == 0d

        col = points.findColumn(5.00001d)
        assert col.value == 10d

        col = points.findColumn(6)
        assert col.value == 10d

        col = points.findColumn(9)
        assert col.value == 10d

        col = points.findColumn(10)
        assert col.value == 10d

        col = points.findColumn(11)
        assert col.value == 10d

        col = points.findColumn(9999.99999d)
        assert col.value == 10000d

        col = points.findColumn(10000)
        assert col.value == 10000d

        col = points.findColumn(10000.0000001d)
        assert col.value == 10000d

        col = points.findColumn(123456789012345678L)
        assert col.value == 10000d
    }

    @Test
    void testNearestAxisTypePoint3D()
    {
        NCube<String> ncube = new NCube<String>("Nearest3D")

        Axis points = new Axis("Point", AxisType.NEAREST, AxisValueType.COMPARABLE, false)
        points.addColumn(new Point3D(0.0, 0.0, 0.0))
        points.addColumn(new Point3D(1.0, 0.0, 0.0))
        points.addColumn(new Point3D(0.0, 1.0, 0.0))
        points.addColumn(new Point3D(-1.0, 0.0, 0.0))
        points.addColumn(new Point3D(0.0, -1.0, 0.0))
        points.addColumn(new Point3D(0.0, 0.0, 1.0))
        points.addColumn(new Point3D(0.0, 0.0, -1.0))
        ncube.addAxis(points)

        Map coord = [Point:new Point3D(0.0, 0.0, 0.0)] as Map
        ncube.setCell("0.0, 0.0, 0.0", coord)
        coord.Point = new Point3D(1.0, 0.0, 0.0)
        ncube.setCell("1.0, 0.0, 0.0", coord)
        coord.Point = new Point3D(0.0, 1.0, 0.0)
        ncube.setCell("0.0, 1.0, 0.0", coord)
        coord.Point = new Point3D(-1.0, 0.0, 0.0)
        ncube.setCell("-1.0, 0.0, 0.0", coord)
        coord.Point = new Point3D(0.0, -1.0, 0.0)
        ncube.setCell("0.0, -1.0, 0.0", coord)
        coord.Point = new Point3D(0.0, 0.0, 1.0)
        ncube.setCell("0.0, 0.0, 1.0", coord)
        coord.Point = new Point3D(0.0, 0.0, -1.0)
        ncube.setCell("0.0, 0.0, -1.0", coord)

        coord.Point = new Point3D(0.0, 0.0, 0.0)
        String s = ncube.getCell(coord)
        assertTrue("0.0, 0.0, 0.0".equals(s))

        coord.Point = new Point3D(-0.1, 0.1, 0.1)
        s = ncube.getCell(coord)
        assertTrue("0.0, 0.0, 0.0".equals(s))

        coord.Point = new Point3D(0.49, 0.49, 0.49)
        s = ncube.getCell(coord)
        assertTrue("0.0, 0.0, 0.0".equals(s))

        coord.Point = new Point3D(2.0, 100.0, 3.0)
        s = ncube.getCell(coord)
        assertTrue("0.0, 1.0, 0.0".equals(s))

        coord.Point = new Point3D(0.1, -0.2, -63.0)
        s = ncube.getCell(coord)
        assertTrue("0.0, 0.0, -1.0".equals(s))

        Point3D p1 = new Point3D(1.0, 2.0, 3.0)
        s = p1.toString()
        assertEquals("1.0, 2.0, 3.0", s)
        assertFalse(p1.equals("string"))
        Point3D p2 = new Point3D(1.0, 2.0, 3.0)
        assertTrue(p1.compareTo(p2) == 0)

        assertTrue(TestNCube.countMatches(ncube.toHtml(), "<tr") == 8)
    }

    @Test
    void testAxisProps()
    {
        Axis axis1 = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY)
        Axis axis2 = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY)

        assert axis1.areAxisPropsEqual(axis1)
        assert axis1.areAxisPropsEqual(axis2)
        assert !axis1.areAxisPropsEqual('fudge')

        Axis axis3 = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.SORTED)
        assert !axis1.areAxisPropsEqual(axis3)

        Axis axis4 = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY)
        assert axis1.areAxisPropsEqual(axis4)

        Axis axis5 = new Axis('foo', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY)
        assert axis1.areAxisPropsEqual(axis5)
        axis5.setMetaProperty 'foo', 'bar'
        assert axis1.areAxisPropsEqual(axis5) // Ensuring meta-props are not part of arePropsEquals()

        Axis axis6 = new Axis('foot', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY)
        assert !axis1.areAxisPropsEqual(axis6)

        Axis axis7 = new Axis('foo', AxisType.RANGE, AxisValueType.STRING, false, Axis.DISPLAY)
        assert !axis1.areAxisPropsEqual(axis7)

        Axis axis8 = new Axis('foo', AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY)
        assert !axis1.areAxisPropsEqual(axis8)
    }

    @Test
    void testUpdateColumn()
    {
        Axis dow = NCubeBuilder.shortDaysOfWeekAxis
        Column wed = dow.findColumn("Wed")
        dow.updateColumn(wed.id, "aWed")
        wed = dow.columns.get(2)
        assertEquals(wed.value, "aWed")

        Column mon = dow.findColumn("Mon")
        dow.updateColumn(mon.id, "aMon")
        mon = dow.columns.get(0)
        assertEquals(mon.value, "aMon")

        Column sun = dow.findColumn("Sun")
        dow.updateColumn(sun.id, "aSun")
        sun = dow.columns.get(6)
        assertEquals(sun.value, "aSun")

        List<Column> cols = dow.columnsWithoutDefault
        assertEquals(cols.get(0).value, "aMon")
        assertEquals(cols.get(2).value, "aWed")
        assertEquals(cols.get(6).value, "aSun")

        assertEquals(1, cols.get(4).compareTo(new Column(null, dow.nextColId)))
    }

    @Test
    void testUpdateColumnsFrontMiddleBack()
    {
        Axis axis = new Axis('Age', AxisType.RANGE, AxisValueType.LONG, false, Axis.SORTED, 1)
        Column col1 = axis.addColumn(new Range(5, 10))
        Column col2 = axis.addColumn(new Range(20, 30))
        Column col3 = axis.addColumn(new Range(30, 40))

        Axis axis2 = new Axis('Age', AxisType.RANGE, AxisValueType.LONG, false, Axis.SORTED, 1)
        axis2.addColumn(new Range(5, 10), '', col1.id)
        axis2.addColumn(new Range(20, 30), '', col2.id)
        axis2.addColumn(new Range(30, 40), '', col3.id)

        Column newCol = axis.createColumnFromValue(new Range(10, 20), null)
        newCol = axis.addColumnInternal(newCol)
        newCol.id = -newCol.id

        axis2.updateColumns(axis.columns)
        assert 4 == axis2.columns.size()

        newCol = axis.createColumnFromValue(new Range(0, 5), null)
        newCol = axis.addColumnInternal(newCol)
        newCol.id = -newCol.id

        axis2.updateColumns(axis.columns)
        assert 5 == axis2.columns.size()

        newCol = axis.createColumnFromValue(new Range(40, 50), null)
        axis.addColumnInternal(newCol)
        newCol.id = -newCol.id

        axis2.updateColumns(axis.columns)
        assert 6 == axis2.columns.size()

        for (Column column : axis2.columns)
        {
            assert column.id >= 0
        }

        // Test remove via updateColumns()
        axis = new Axis('Age', AxisType.RANGE, AxisValueType.LONG, false, Axis.SORTED, 1)
        axis.addColumn(new Range(5, 10), '', col1.id)
        axis.addColumn(new Range(20, 30), '', col2.id)
        axis.addColumn(new Range(30, 40), '', col3.id)

        axis2.updateColumns(axis.columns)
        assert 3 == axis2.size()
    }

    @Test
    void testUpdateColumnsOverlapCheck()
    {
        Axis axis = new Axis('Age', AxisType.RANGE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        Column col1 = axis.addColumn new Range('2', '4')
        Column col2 = axis.addColumn new Range('4', '6')
        Column col3 = axis.addColumn new Range('6', '8')
        Column col4 = axis.addColumn new Range('0', '2')

        Axis axis2 = new Axis('Age', AxisType.RANGE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        axis2.addColumn(new Range('2', '4'), '', col1.id)
        axis2.addColumn(new Range('4', '6'), '', col2.id)
        axis2.addColumn(new Range('6', '8'), '', col3.id)
        axis2.addColumn(new Range('0', '2'), '', col4.id)

        Column newCol = axis.createColumnFromValue(new Range('8', '10'), null) // String axis

        try
        {
            axis.addColumnInternal(newCol)
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        newCol = axis.createColumnFromValue(new Range('8', '9'), null)      // String axis
        newCol.id = -newCol.id
        axis2.updateColumns(axis.columns)
    }

    @Test
    void testUpdateColumnsOverlapFail()
    {
        Axis axis = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        Column mon = axis.addColumn('Mon')
        Column tue = axis.addColumn('Tue')
        Column wed = axis.addColumn('Wed')
        Column thu = axis.addColumn('Thu')
        Column fri = axis.addColumn('Fri')
        Column sat = axis.addColumn('Sat')
        Column sun = axis.addColumn('Sun')

        List<Column> columnList = axis.columns
        axis.deleteColumn('Wed')
        Column repeatedColumn = axis.addColumn('Wed')
        repeatedColumn.id = -repeatedColumn.id
        columnList << repeatedColumn

        Axis axis2 = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        axis2.addColumn('Mon', '', mon.id)
        axis2.addColumn('Tue', '', tue.id)
        axis2.addColumn('Wed', '', wed.id)
        axis2.addColumn('Thu', '', thu.id)
        axis2.addColumn('Fri', '', fri.id)
        axis2.addColumn('Sat', '', sat.id)
        axis2.addColumn('Sun', '', sun.id)

        try
        {
            axis2.updateColumns(columnList)
            fail()
        }
        catch (AxisOverlapException e)
        {
            String msg = e.message
            assert msg.contains('matches a value already on axis')
            assert msg.contains('days')
            assert msg.contains('Wed')
        }
    }

    @Test
    void testUpdateColumnsUpdatedValueFail()
    {
        Axis axis = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        Column mon = axis.addColumn('Mon')
        Column tue = axis.addColumn('Tue')
        Column wed = axis.addColumn('Wed')
        axis.findColumn('Mon').value = 'Wed'

        Axis axis2 = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        axis2.addColumn('Mon', '', mon.id)
        axis2.addColumn('Tue', '', tue.id)
        axis2.addColumn('Wed', '', wed.id)

        try
        {
            axis2.updateColumns(axis.columns)
            fail()
        }
        catch (AxisOverlapException e)
        {
            String msg = e.message
            assert msg.contains('matches a value already on axis')
            assert msg.contains('days')
            assert msg.contains('Wed')
        }
    }

    @Test
    void testUpColumnsMaintainsOrder()
    {
        Axis axis = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        Column mon = axis.addColumn 'Mon'
        Column tue = axis.addColumn 'Tue'
        Column wed = axis.addColumn 'Wed'
        wed.id = -wed.id
        Column thu = axis.addColumn 'Thu'
        Column fri = axis.addColumn 'Fri'
        Column sat = axis.addColumn 'Sat'
        Column sun = axis.addColumn 'Sun'

        // Mon/Sat backwards
        // Wed missing
        // Bogus column added (named 'Whoops')
        // Fix these problems with updateColumns (simulate user moving columns in NCE)
        Axis axis2 = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        axis2.addColumn('Sat', '', sat.id)
        axis2.addColumn('Tue', '', tue.id)
        axis2.addColumn('Wed', '', wed.id)
        axis2.addColumn('Thu', '', thu.id)
        axis2.addColumn('Fri', '', fri.id)
        axis2.addColumn('Mon', '', mon.id)
        axis2.addColumn('Sun', '', sun.id)
        axis2.addColumn('Whoops')
        axis2.deleteColumn('Wed')

        axis2.updateColumns(axis.columns)
        assert 7 == axis2.size()
        assert 'Mon' == axis2.columns[0].value
        assert 'Tue' == axis2.columns[1].value
        assert 'Wed' == axis2.columns[2].value
        assert 'Thu' == axis2.columns[3].value
        assert 'Fri' == axis2.columns[4].value
        assert 'Sat' == axis2.columns[5].value
        assert 'Sun' == axis2.columns[6].value
    }

    @Test
    void testUpColumnsMaintainsOrderWithDefault()
    {
        Axis axis = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY, 1)
        Column mon = axis.addColumn 'Mon'
        Column tue = axis.addColumn 'Tue'
        Column wed = axis.addColumn 'Wed'
        wed.id = -wed.id
        Column thu = axis.addColumn 'Thu'
        Column fri = axis.addColumn 'Fri'
        Column sat = axis.addColumn 'Sat'
        Column sun = axis.addColumn 'Sun'

        // Mon/Sat backwards
        // Wed missing
        // Bogus column added (named 'Whoops')
        // Fix these problems with updateColumns (simulate user moving columns in NCE)
        Axis axis2 = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY, 1)
        axis2.addColumn('Sat', '', sat.id)
        axis2.addColumn('Tue', '', tue.id)
        axis2.addColumn('Wed', '', wed.id)
        axis2.addColumn('Thu', '', thu.id)
        axis2.addColumn('Fri', '', fri.id)
        axis2.addColumn('Mon', '', mon.id)
        axis2.addColumn('Sun', '', sun.id)
        axis2.addColumn('Whoops')
        axis2.deleteColumn('Wed')

        axis2.updateColumns(axis.columns)
        assert 8 == axis2.size()
        assert 'Mon' == axis2.columns[0].value
        assert 'Tue' == axis2.columns[1].value
        assert 'Wed' == axis2.columns[2].value
        assert 'Thu' == axis2.columns[3].value
        assert 'Fri' == axis2.columns[4].value
        assert 'Sat' == axis2.columns[5].value
        assert 'Sun' == axis2.columns[6].value
        assertNull axis2.columns[7].value
        assert Integer.MAX_VALUE == axis2.columns.get(7).displayOrder
    }

    @Test
    void testUpColumnsMaintainsIgnoresDefault()
    {
        Axis axis = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY, 1)
        Column mon = axis.addColumn 'Mon'
        Column tue = axis.addColumn 'Tue'
        Column wed = axis.addColumn 'Wed'
        wed.id = -wed.id
        Column thu = axis.addColumn 'Thu'
        Column fri = axis.addColumn 'Fri'
        Column sat = axis.addColumn 'Sat'
        Column sun = axis.addColumn 'Sun'

        // Mon/Sat backwards
        // Wed missing
        // Bogus column added (named 'Whoops')
        // Fix these problems with updateColumns (simulate user moving columns in NCE)
        Axis axis2 = new Axis('days', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY, 1)
        axis2.addColumn('Sat', '', sat.id)
        axis2.addColumn('Tue', '', tue.id)
        axis2.addColumn('Wed', '', wed.id)
        axis2.addColumn('Thu', '', thu.id)
        axis2.addColumn('Fri', '', fri.id)
        axis2.addColumn('Mon', '', mon.id)
        axis2.addColumn('Sun', '', sun.id)
        axis2.addColumn('Whoops')
        axis2.deleteColumn('Wed')

        axis2.updateColumns(axis.columns)
        assert 7 == axis2.size()
        assert 'Mon' == axis2.columns[0].value
        assert 'Tue' == axis2.columns[1].value
        assert 'Wed' == axis2.columns[2].value
        assert 'Thu' == axis2.columns[3].value
        assert 'Fri' == axis2.columns[4].value
        assert 'Sat' == axis2.columns[5].value
        assert 'Sun' == axis2.columns[6].value
    }

    @Test
    void testProveDefaultLast()
    {
        Axis axis = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.SORTED)
        axis.addColumn("alpha")
        axis.addColumn("charlie")
        axis.addColumn("bravo")
        List<Column> cols = axis.columns
        assertEquals(cols.get(0).value, "alpha")
        assertEquals(cols.get(1).value, "bravo")
        assertEquals(cols.get(2).value, "charlie")
        assertEquals(cols.get(3).value, null)

        axis = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, false, Axis.SORTED)
        axis.addColumn("alpha")
        axis.addColumn("charlie")
        axis.addColumn("bravo")
        cols = axis.columns
        assertEquals(3, cols.size())
        assertEquals(cols.get(0).value, "alpha")
        assertEquals(cols.get(1).value, "bravo")
        assertEquals(cols.get(2).value, "charlie")

        axis = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY)
        axis.addColumn("alpha")
        axis.addColumn("charlie")
        axis.addColumn("bravo")
        cols = axis.columns
        assertEquals(cols.get(0).value, "alpha")
        assertEquals(cols.get(1).value, "charlie")
        assertEquals(cols.get(2).value, "bravo")
        assertEquals(cols.get(3).value, null)

        axis = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, false, Axis.DISPLAY)
        axis.addColumn("alpha")
        axis.addColumn("charlie")
        axis.addColumn("bravo")
        cols = axis.columns
        assertEquals(3, cols.size())
        assertEquals(cols.get(0).value, "alpha")
        assertEquals(cols.get(1).value, "charlie")
        assertEquals(cols.get(2).value, "bravo")
    }

    @Test
    void testUpdateColumnsNotLosingCellsInDefaultColumn()
    {
        NCube ncube = NCubeBuilder.discrete1DEmptyWithDefault
        ncube.setCell('Ohio', [state:'OH'] as Map)
        ncube.setCell('Texas', [state:'TX'] as Map)
        ncube.setCell('Alabama', [state:'AL'] as Map)
        assert 'Alabama' == ncube.getCell([state:'WY'] as Map)          // WY hits Default

        List columns = ncube.getAxis('state').columnsWithoutDefault
        ncube.updateColumns('state', columns)

        assert 'Ohio' == ncube.getCell([state:'OH'] as Map)
        assert 'Texas' == ncube.getCell([state:'TX'] as Map)
        assert 'Alabama' == ncube.getCell([state:'WY'] as Map)          // WY hits Default
    }

    @Test
    void testMaxAxisId()  throws Exception
    {
        NCube cube = new NCube("fourD")
        assertEquals(0, cube.maxAxisId)

        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.SORTED, cube.maxAxisId + 1)
        cube.addAxis(axis1)
        assertEquals(1, cube.maxAxisId)

        Axis axis2 = new Axis("bar", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.SORTED, cube.maxAxisId + 1)
        cube.addAxis(axis2)
        assertEquals(2, cube.maxAxisId)

        Axis axis3 = new Axis("baz", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.SORTED, cube.maxAxisId + 1)
        cube.addAxis(axis3)
        assertEquals(3, cube.maxAxisId)

        Axis axis4 = new Axis("qux", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.SORTED, cube.maxAxisId + 1)
        cube.addAxis(axis4)
        assertEquals(4, cube.maxAxisId)
    }

    @Test
    void testEqualsAxisNameMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        Axis axis2 = new Axis("foot", AxisType.DISCRETE, AxisValueType.STRING, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube2.addAxis(axis2)

        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsAxisMetaMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        axis1.setMetaProperty("fingers", 4)
        axis1.setMetaProperty("thumb", 1)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube2.addAxis(axis2)
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsAxisTypeMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.RANGE, AxisValueType.STRING, true)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube2.addAxis(axis2)
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsAxisValueTypeMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.LONG, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube2.addAxis(axis2)
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsAxisDefaultMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, false)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube2.addAxis(axis2)
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsColumnCountMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, false)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, false)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube1.addColumn("foo", "qux")

        cube2.addAxis(axis2)
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsColumnTypeMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, false)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube1.addColumn("foo", "qux")

        cube2.addAxis(axis2)
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsColumnValueMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube1.addColumn("foo", "baz")

        cube2.addAxis(axis2)
        cube2.addColumn("foo", "qux")
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testEqualsColumnMetaPropertiesMismatch()  throws Exception
    {
        Axis axis1 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        Axis axis2 = new Axis("foo", AxisType.DISCRETE, AxisValueType.STRING, true)
        NCube cube1 = new NCube("bar")
        NCube cube2 = new NCube("bar")

        cube1.addAxis(axis1)
        cube1.addColumn("foo", "baz")
        Column col = axis1.findColumn("baz")
        col.setMetaProperty("Glock", "23")

        cube2.addAxis(axis2)
        cube2.addColumn("foo", "baz")
        assertNotEquals(cube1, cube2)
    }

    @Test
    void testRangeOverlap2()
    {
        Axis axis = new Axis("numbers", AxisType.RANGE, AxisValueType.LONG, true, Axis.DISPLAY)
        axis.addColumn(new Range(10, 20))
        axis.addColumn(new Range(30, 40))
        axis.addColumn(new Range(50, 60))
        axis.addColumn(new Range(70, 80))
        axis.addColumn(new Range(90, 100))

        try
        {
            axis.addColumn(new Range(0, 11))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(45, 65))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(41, 51))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(0, 100))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(99, 101))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        axis.addColumn(new Range(0, 10))
        axis.addColumn(new Range(41, 50))
        axis.addColumn(new Range(100, 200))
    }

    @Test
    void testRangeSetOverlap3()
    {
        Axis axis = new Axis("numbers", AxisType.SET, AxisValueType.LONG, true, Axis.DISPLAY)
        RangeSet one = new RangeSet(10)
        one.add(new Range(13, 20))

        RangeSet two = new RangeSet(30)
        two.add(new Range(33, 40))

        RangeSet three = new RangeSet(50)
        three.add(new Range(53, 60))

        RangeSet four = new RangeSet(70)
        four.add(new Range(73, 80))

        RangeSet five = new RangeSet(90)
        five.add(new Range(93, 100))

        axis.addColumn(one)
        axis.addColumn(two)
        axis.addColumn(three)
        axis.addColumn(four)
        axis.addColumn(five)
        try
        {
            axis.addColumn(new Range(0, 11))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(12, 21))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(41, 51))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(0, 110))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(89, 92))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        try
        {
            axis.addColumn(new Range(99, 101))
            fail()
        }
        catch (AxisOverlapException ignore)
        { }

        axis.addColumn(new Range(0, 10))
        axis.addColumn(new Range(11, 12))
        axis.addColumn(new Range(41, 50))
        axis.addColumn(new Range(91, 93))
        axis.addColumn(new Range(100, 200))
    }

    @Test
    void testLargeNumberOfRangeSetColumns()
    {
        NCube ncube = new NCube("BigDaddy")
        Axis axis = new Axis("numbers", AxisType.SET, AxisValueType.LONG, true, Axis.DISPLAY)
        ncube.addAxis(axis)
        def coord = [:]

        int largeNumber = 1000000
        long start = System.nanoTime()
        for (int i = 0; i < largeNumber; i += 10)
        {
            RangeSet set = new RangeSet(i)
            Range range = new Range(i + 1, i + 4)
            set.add(range)
            axis.addColumn(set)
            coord.put("numbers", i)
            ncube.setCell(i * 2, coord)
        }

        long stop = System.nanoTime()

        double diff = (stop - start) / 1000.0  // usec
        println("build " + (largeNumber / 10) + " SET columns = " + (diff / 1000.0) + " ms")

        start = System.nanoTime()
        for (int i = 0; i < largeNumber; i += 10)
        {
            coord.numbers = i
            Integer ans = (Integer) ncube.getCell(coord)
            assertEquals(i * 2, ans.intValue())
        }
        stop = System.nanoTime()

        diff = (stop - start) / 1000.0  // usec
        println("lookup " + (largeNumber / 10) + " times large number of SET columns = " + (diff / 1000.0) + " ms")
    }

    @Test
    void testLargeNumberOfDiscreteColumns()
    {
        NCube ncube = new NCube("BigDaddy")
        Axis axis = new Axis("numbers", AxisType.DISCRETE, AxisValueType.LONG, true, Axis.DISPLAY)
        ncube.addAxis(axis)
        def coord = [:]

        int largeNumber = 100000
        long start = System.nanoTime()
        for (int i = 0; i < largeNumber; i ++)
        {
            axis.addColumn(i)
        }

        for (int i = 0; i < largeNumber; i ++)
        {
            coord.put("numbers", i)
            ncube.setCell(i * 2, coord)
        }

        long stop = System.nanoTime()

        double diff = (stop - start) / 1000.0  // usec
        println("build " + largeNumber + " DISCRETE columns = " + (diff / 1000.0) + " ms")

        start = System.nanoTime()
        for (int i = 0; i < largeNumber; i++)
        {
            coord.numbers = i
            axis.findColumn(i)
            Integer ans = (Integer) ncube.getCell(coord)
            assertEquals(i * 2, ans.intValue())
        }
        stop = System.nanoTime()

        diff = (stop - start) / 1000.0  // usec
        println("lookup " + largeNumber + " times large number of DISCRETE columns = " + (diff / 1000.0) + " ms")
    }

    @Test
    void testIntSetParsing()
    {
        Axis axis = new Axis('ages', AxisType.SET, AxisValueType.LONG, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('10, 20, [50, 90], 100')
        assert set.size() == 4
        assert set.get(0) == 10
        assert set.get(1) == 20
        assert set.get(2) == new Range(50L, 90L)
        assert set.get(3) == 100

        set = (RangeSet) axis.standardizeColumnValue('10, 20, [50, 90]')
        assert set.size() == 3
        assert set.get(0) == 10
        assert set.get(1) == 20
        assert set.get(2) == new Range(50L, 90L)

        set = (RangeSet) axis.standardizeColumnValue('[50, 90], 94')
        assert set.size() == 2
        assert set.get(0) == new Range(50L, 90L)
        assert set.get(1) == 94

        set = (RangeSet) axis.standardizeColumnValue('[50, 90]')
        assert set.size() == 1
        assert set.get(0) == new Range(50L, 90L)

        set = (RangeSet) axis.standardizeColumnValue('[50, 90], [20, 60]')
        assert set.size() == 2
        assert set.get(0) == new Range(50L, 90L)
        assert set.get(1) == new Range(20L, 60L)

        set = (RangeSet) axis.standardizeColumnValue('[50, 90], 789, [20, 60]')
        assert set.size() == 3
        assert set.get(0) == new Range(50L, 90L)
        assert set.get(1) == 789
        assert set.get(2) == new Range(20L, 60L)
    }

    @Test
    void testFloatSetParsing()
    {
        Axis axis = new Axis('ages', AxisType.SET, AxisValueType.BIG_DECIMAL, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('10.1, 20, [50, 90.5], 100.1')
        assert set.size() == 4
        assert set.get(0) == 10.1d
        assert set.get(1) == 20
        assert set.get(2) == new Range(50.0, 90.5)
        assert set.get(3) == 100.1d
    }

    @Test
    void testDateSetParsing()
    {
        Axis axis = new Axis('dates', AxisType.SET, AxisValueType.DATE, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('"10 Dec 1995", "1995/12/25", ["1996 dec 17", "2001-01-31"], "Jun 10th 2010"')
        assert set.size() == 4
        assert set.get(0) == convertToDate("10 Dec 1995")
        assert set.get(1) == convertToDate("25 Dec 1995")
        assert set.get(2) == new Range(convertToDate("1996 dec 17"), convertToDate('2001-01-31'))
        assert set.get(3) == convertToDate("Jun 10th 2010")
    }

    @Test
    void testStringSetParsing()
    {
        Axis axis = new Axis('strings', AxisType.SET, AxisValueType.STRING, true, Axis.SORTED)
        RangeSet set = (RangeSet) axis.standardizeColumnValue('"10 Dec 1995", "1995/12/25", ["1996 dec 17", "2001-01-31"], "Jun 10th 2010"')
        assert set.size() == 4
        assert set.get(0) == "10 Dec 1995"
        assert set.get(1) == "1995/12/25"
        assert set.get(2) == new Range("1996 dec 17", '2001-01-31')
        assert set.get(3) == "Jun 10th 2010"

        set = (RangeSet) axis.standardizeColumnValue('  "The quick", "brown fox", [ "jumps over", "the lazy dog"], "I\'m dead serious", "\\"this is quoted\\""')
        assert set.size() == 5
        assert set.get(0) == "The quick"
        assert set.get(1) == "brown fox"
        assert set.get(2) == new Range('jumps over', 'the lazy dog')
        assert set.get(3) == "I'm dead serious"
        assert set.get(4) == '"this is quoted"'
    }

    @Test
    void testRuleConditionParsing()
    {
        Axis axis = new Axis('rule', AxisType.RULE, AxisValueType.EXPRESSION, true)
        GroovyExpression exp = (GroovyExpression) axis.standardizeColumnValue("true")
        assert "true".equals(exp.cmd)
        assert exp.url == null

        exp = (GroovyExpression) axis.standardizeColumnValue("cache|true")
        assert 'true'.equals(exp.cmd)
        assert  null == exp.url
        assert exp.cacheable

        // These values allow a single-line edit widget to feed a GroovyExpression with all capabilities.
        exp = (GroovyExpression) axis.standardizeColumnValue("url|http://www.foxnews.com")
        assert "http://www.foxnews.com".equals(exp.url)
        assert !exp.cacheable

        exp = (GroovyExpression) axis.standardizeColumnValue("url|cache|http://www.foxnews.com")
        assert "http://www.foxnews.com".equals(exp.url)
        assert exp.cmd == null
        assert exp.cacheable

        exp = (GroovyExpression) axis.standardizeColumnValue("cache|url|http://www.foxnews.com")
        assert "http://www.foxnews.com".equals(exp.url)
        assert exp.cmd == null
        assert exp.cacheable
    }

    @Test
    void testAddAxisWithSameIdTwice()
    {
        Axis axis1 = new Axis('state', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.SORTED, 1)
        Axis axis2 = new Axis('bu', AxisType.DISCRETE, AxisValueType.STRING, false, Axis.SORTED, 1)
        NCube ncube = new NCube('test')
        ncube.addAxis(axis1)
        try
        {
            ncube.addAxis(axis2)
        }
        catch (IllegalArgumentException ignore)
        { }
    }

    @Test
    void testReferenceAxisNoDefaultAndBreakReference()
    {
        NCube one = NCubeBuilder.discrete1DAlt
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('state').size() == 2
        ncubeRuntime.addCube(one)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'SimpleDiscrete'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        Axis axis = new Axis('stateSource', 1, false, refAxisLoader)
        assert axis.referenceCubeName == 'SimpleDiscrete'
        assert axis.referenceAxisName == 'state'
        NCube two = new NCube('Mongo')
        two.addAxis(axis)

        two.setCell('a', [stateSource:'OH'] as Map)
        two.setCell('b', [stateSource:'TX'] as Map)

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 2
        assert 'a' == reload.getCell([stateSource:'OH'] as Map)
        assert 'b' == reload.getCell([stateSource:'TX'] as Map)
        assert reload.getAxis('stateSource').reference

        // Break reference and verify broken
        reload.breakAxisReference('stateSource')
        assert reload.numCells == 2
        assert 'a' == reload.getCell([stateSource:'OH'] as Map)
        assert 'b' == reload.getCell([stateSource:'TX'] as Map)
        assert !reload.getAxis('stateSource').reference
    }

    @Test
    void testReferenceAxisToReferenceAxis()
    {
        NCube one = NCubeBuilder.discrete1DAlt
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('state').size() == 2
        ncubeRuntime.addCube(one)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'SimpleDiscrete'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        Axis axis = new Axis('stateSource', 1, false, refAxisLoader)
        NCube two = new NCube('Mongo')
        two.applicationID = ApplicationID.testAppId
        two.addAxis(axis)

        two.setCell('a', [stateSource:'OH'] as Map)
        two.setCell('b', [stateSource:'TX'] as Map)
        ncubeRuntime.addCube(two)

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 2
        assert 'a' == reload.getCell([stateSource:'OH'] as Map)
        assert 'b' == reload.getCell([stateSource:'TX'] as Map)
        assert reload.getAxis('stateSource').reference

        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'Mongo'
        args[REF_AXIS_NAME] = 'stateSource'

        ReferenceAxisLoader refAxisLoader3 = new ReferenceAxisLoader('Three', 'stateClone', args)
        axis = new Axis('stateClone', 1, false, refAxisLoader3)
        NCube three = new NCube('Three')
        three.addAxis(axis)

        three.setCell('a', [stateClone:'OH'] as Map)
        three.setCell('b', [stateClone:'TX'] as Map)

        json = three.toFormattedJson()
        reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 2
        assert 'a' == reload.getCell([stateClone:'OH'] as Map)
        assert 'b' == reload.getCell([stateClone:'TX'] as Map)
        assert reload.getAxis('stateClone').reference
    }

    @Test
    void testReferenceAxisAddedDefault()
    {
        NCube one = NCubeBuilder.discrete1DAlt
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('state').size() == 2
        ncubeRuntime.addCube(one)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'SimpleDiscrete'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        Axis axis = new Axis('stateSource', 1, true, refAxisLoader)
        NCube two = new NCube('Mongo')
        two.addAxis(axis)

        two.setCell('a', [stateSource:'OH'] as Map)
        two.setCell('b', [stateSource:'TX'] as Map)
        two.setCell('c', [stateSource:'AZ'] as Map)         // Hits Default axis

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 3
        assert 'a' == reload.getCell([stateSource:'OH'] as Map)
        assert 'b' == reload.getCell([stateSource:'TX'] as Map)
        assert 'c' == reload.getCell([stateSource:'AZ'] as Map)
        assert 'c' == reload.getCell([stateSource:'blah'] as Map)
    }

    @Test
    void testReferenceAxisWithDefault()
    {
        NCube one = NCubeBuilder.discrete1DEmptyWithDefault
        one.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(one)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'SimpleDiscrete'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        Axis axis = new Axis('stateSource', 1, true, refAxisLoader)
        NCube two = new NCube('Mongo')
        two.addAxis(axis)

        two.setCell('a', [stateSource:'OH'] as Map)
        two.setCell('b', [stateSource:'TX'] as Map)
        two.setCell('c', [stateSource:'AZ'] as Map)         // Hits Default axis

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 3
        assert 'a' == reload.getCell([stateSource:'OH'] as Map)
        assert 'b' == reload.getCell([stateSource:'TX'] as Map)
        assert 'c' == reload.getCell([stateSource:'AZ'] as Map)
        assert 'c' == reload.getCell([stateSource:'blah'] as Map)
    }

    @Test
    void testReferenceAxisDeleteColumn()
    {
        NCube one = NCubeBuilder.discrete1DEmptyWithDefault
        one.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(one)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'SimpleDiscrete'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        Axis axis = new Axis('stateSource', 1, true, refAxisLoader)
        NCube two = new NCube('Mongo')
        two.addAxis(axis)

        two.setCell('a', [stateSource:'OH'] as Map)
        two.setCell('b', [stateSource:'TX'] as Map)
        two.setCell('c', [stateSource:'AZ'] as Map)         // Hits Default axis

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)

        reload.deleteColumn('stateSource', 'Default')
        Axis reloadAxis = reload.getAxis('stateSource')

        // default column and related cells should be deleted
        assertFalse(reloadAxis.hasDefaultColumn())
        assert 2 == reloadAxis.columns.size()
        assert 2 == reload.numCells
        try
        {
            reload.getCell([stateSource:'AZ'] as Map)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            String message = e.message
            assertContainsIgnoreCase(message, 'value', 'AZ', 'not found', 'stateSource', 'Mongo')
        }

        try
        {
            reload.deleteColumn('stateSource', 'OH')
            fail()
        }
        catch (IllegalStateException e)
        {
            String message = e.message
            assert message.contains('You cannot delete non-default columns from a reference Axis, axis: stateSource')
        }

        // column should not be deleted
        assert 2 == reloadAxis.columns.size()
        assert 2 == reload.numCells
        assert 'a' == reload.getCell([stateSource:'OH'] as Map)
    }

    @Test
    void testReferenceAxisWithTransform()
    {
        NCube one = NCubeBuilder.discrete1DLong
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('code').size() == 4
        ncubeRuntime.addCube(one)

        NCube transform = NCubeBuilder.transformMultiply
        transform.applicationID = ApplicationID.testAppId
        assert transform.getAxis('transform').size() == 4
        ncubeRuntime.addCube(transform)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'discreteLong'
        args[REF_AXIS_NAME] = 'code'
        args[TRANSFORM_APP] = appId.app
        args[TRANSFORM_VERSION] = appId.version
        args[TRANSFORM_STATUS] = appId.status
        args[TRANSFORM_BRANCH] = appId.branch
        args[TRANSFORM_CUBE_NAME] = 'multiplier'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('TestTransform', 'age', args)
        Axis axis = new Axis('age', 1, false, refAxisLoader)
        NCube two = new NCube('TestTransform')
        two.addAxis(axis)
        two.setCell('a', [age:2] as Map)
        assert 'a' == two.getCell([age:2] as Map)

        two.setCell('b', [age:4] as Map)
        assert 'b' == two.getCell([age:4] as Map)

        two.setCell('c', [age:6] as Map)
        assert 'c' == two.getCell([age:6] as Map)

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 3

        // 1, 2, 3 was transformed to 2, 4, 6
        assert 'a' == reload.getCell([age:2] as Map)
        assert 'b' == reload.getCell([age:4] as Map)
        assert 'c' == reload.getCell([age:6] as Map)

        json = reload.toString()
        assert !json.contains('"columns":{')
        json = reload.toFormattedJson([indexFormat:true] as Map)
        assert json.contains('"columns":{')
    }

    @Test
    void testReferenceAxisRemoveTransform()
    {
        NCube one = NCubeBuilder.discrete1DLong
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('code').size() == 4
        ncubeRuntime.addCube(one)

        NCube transform = NCubeBuilder.transformMultiply
        transform.applicationID = ApplicationID.testAppId
        assert transform.getAxis('transform').size() == 4
        ncubeRuntime.addCube(transform)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'discreteLong'
        args[REF_AXIS_NAME] = 'code'
        args[TRANSFORM_APP] = appId.app
        args[TRANSFORM_VERSION] = appId.version
        args[TRANSFORM_STATUS] = appId.status
        args[TRANSFORM_BRANCH] = appId.branch
        args[TRANSFORM_CUBE_NAME] = 'multiplier'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('TestTransform', 'age', args)
        Axis axis = new Axis('age', 1, false, refAxisLoader)
        NCube two = new NCube('TestTransform')
        two.addAxis(axis)
        two.setCell('a', [age:2] as Map)
        assert 'a' == two.getCell([age:2] as Map)

        two.setCell('b', [age:4] as Map)
        assert 'b' == two.getCell([age:4] as Map)

        two.setCell('c', [age:6] as Map)
        assert 'c' == two.getCell([age:6] as Map)

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 3

        // 1, 2, 3 was transformed to 2, 4, 6
        assert 'a' == reload.getCell([age:2] as Map)
        assert 'b' == reload.getCell([age:4] as Map)
        assert 'c' == reload.getCell([age:6] as Map)

        two.removeAxisReferenceTransform('age')
        json = two.toFormattedJson()
        reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 0

        // should have original columns
        reload.setCell('a', [age:1] as Map)
        assert 'a' == reload.getCell([age:1] as Map)

        reload.setCell('b', [age:2] as Map)
        assert 'b' == reload.getCell([age:2] as Map)

        reload.setCell('c', [age:3] as Map)
        assert 'c' == reload.getCell([age:3] as Map)

    }

    @Test
    void testReferenceAxisCubeNotExists()
    {
        NCube one = NCubeBuilder.discrete1DAlt
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('state').size() == 2
        ncubeRuntime.addCube(one)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'SimpleDiscreteNotExisting'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        try
        {
            Axis ignore = new Axis('stateSource', 1, false, refAxisLoader)
            fail(ignore.name)
        }
        catch (IllegalStateException e)
        {
            String message = e.message
            assert message.contains('Unable to load')
            assert message.contains('reference axis')
            assert message.contains('impleDiscreteNotExisting')
        }

        args[REF_CUBE_NAME] = 'SimpleDiscrete'
        args[REF_AXIS_NAME] = 'stateNotThere'
        refAxisLoader = new ReferenceAxisLoader('Mongo', 'stateSource', args)
        try
        {
            Axis ignore = new Axis('stateSource', 1, false, refAxisLoader)
            fail(ignore.name)
        }
        catch (IllegalStateException e)
        {
            String message = e.message
            assert message.contains('Unable to load')
            assert message.contains('reference axis')
            assert message.contains('stateNotThere')
            assert message.contains('not found')
        }
    }

    @Test
    void testNonExistingTransformCube()
    {
        NCube one = NCubeBuilder.discrete1DLong
        one.applicationID = ApplicationID.testAppId
        assert one.getAxis('code').size() == 4
        ncubeRuntime.addCube(one)

        NCube transform = NCubeBuilder.transformMultiply
        transform.applicationID = ApplicationID.testAppId
        assert transform.getAxis('transform').size() == 4
        ncubeRuntime.addCube(transform)

        Map<String, Object> args = [:]

        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'discreteLong'
        args[REF_AXIS_NAME] = 'code'
        args[TRANSFORM_APP] = appId.app
        args[TRANSFORM_VERSION] = appId.version
        args[TRANSFORM_STATUS] = appId.status
        args[TRANSFORM_BRANCH] = appId.branch
        args[TRANSFORM_CUBE_NAME] = 'multiplierNotThere'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader('TestTransform', 'age', args)
        try
        {
            Axis ignore = new Axis('age', 1, false, refAxisLoader)
            fail(ignore.name)
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'unable to load', 'TestTransform', 'reference axis', 'failed to load transform')
        }

        args[TRANSFORM_CUBE_NAME] = 'discreteLong'  // this cube does not have correct axes
        refAxisLoader = new ReferenceAxisLoader('TestTransform', 'age', args)
        try
        {
            Axis ignore = new Axis('age', 1, false, refAxisLoader)
            fail(ignore.name)
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'unable to load', 'TestTransform', 'reference axis: age', 'must have two discrete')
        }
    }

    @Test
    void testReferenceIntoHigherDimensionCube()
    {
        NCube one = NCubeBuilder.discrete1DLong
        one.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(one)
        NCube two = NCubeBuilder.get5DTestCube()
        two.applicationID = ApplicationID.testAppId
        ncubeRuntime.addCube(two)

        Map args = [:] as Map
        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'testMerge'
        args[REF_AXIS_NAME] = 'state'

        // stateSource instead of 'state' to prove the axis on the referring cube does not have to have the same name
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(one.name, 'stateRef', args)
        Axis state = new Axis('stateRef', 2, false, refAxisLoader)
        one.addAxis(state)

        Map coord = [:] as Map
        coord.code ='1'
        coord.stateRef = 'OH'
        one.setCell('1.OH', coord)

        coord.code ='2'
        one.setCell('2.OH', coord)

        coord.code ='3'
        one.setCell('3.OH', coord)

        coord.code ='1'
        coord.stateRef = 'TX'
        one.setCell('1.TX', coord)

        coord.code ='2'
        one.setCell('2.TX', coord)

        coord.code ='3'
        one.setCell('3.TX', coord)

        String json = one.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        assert reload.numCells == 6

        coord.code ='1'
        coord.stateRef = 'OH'
        assert '1.OH' == one.getCell(coord)

        coord.code ='2'
        assert '2.OH' == one.getCell(coord)

        coord.code ='3'
        assert '3.OH' == one.getCell(coord)

        coord.code ='1'
        coord.stateRef = 'TX'
        assert '1.TX' == one.getCell(coord)

        coord.code ='2'
        assert '2.TX' == one.getCell(coord)

        coord.code ='3'
        assert '3.TX' == one.getCell(coord)
    }

    @Test
    void testRefAxisWithMetaProps()
    {
        NCube one = NCubeBuilder.discrete1DLong
        one.applicationID = ApplicationID.testAppId
        Axis code = one.getAxis('code')
        code.setMetaProperty('a', 'alpha')
        code.setMetaProperty('b', new GroovyExpression('return 7', null, false))
        code.setMetaProperty('c', 'charlie')
        Column col2 = one.findColumn('code', 2)
        col2.setMetaProperty('a', '1')
        col2.setMetaProperty('b', 2)
        Column colDef = code.defaultColumn
        colDef.setMetaProperty('foo', 'bar')
        colDef.setMetaProperty('baz', 'qux')
        ncubeRuntime.addCube(one)
        NCube two = new NCube('two')
        two.applicationID = ApplicationID.testAppId

        Map args = [:] as Map
        ApplicationID appId = ApplicationID.testAppId
        args[REF_TENANT] = appId.tenant
        args[REF_APP] = appId.app
        args[REF_VERSION] = appId.version
        args[REF_STATUS] = appId.status
        args[REF_BRANCH] = appId.branch
        args[REF_CUBE_NAME] = 'discreteLong'
        args[REF_AXIS_NAME] = 'code'

        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(one.name, 'code', args)
        code =  new Axis('code', 1L, true, refAxisLoader)
        code.setMetaProperty('b', new GroovyExpression('return 1.0', null, false))
        code.setMetaProperty('d', 'delta')
        two.addAxis(code)
        col2 = code.findColumn(2)
        col2.removeMetaProperty('b')
        col2.setMetaProperty('c', new GroovyExpression('return true', null, false))
        colDef = code.defaultColumn
        colDef.setMetaProperty('foo', 'bart')
        colDef.removeMetaProperty('baz')
        colDef.setMetaProperty('monkey', 'socks')
        ncubeRuntime.addCube(two)

        String json = two.toFormattedJson()
        NCube reload = NCube.fromSimpleJson(json)
        code = reload.getAxis('code')

        Map meta = code.metaProperties
        assert 'alpha' == meta.get('a')
        assert new GroovyExpression('return 1.0', null, false) == meta.get('b')
        assert 'charlie' == meta.get('c')
        assert 'delta' == meta.get('d')
        col2 = code.findColumn(2)
        assert '1' == col2.getMetaProperty('a')
        assert 2 == col2.getMetaProperty('b')
        assert new GroovyExpression('return true', null, false) == col2.getMetaProperty('c')

        colDef = code.defaultColumn
        assert 'bart' == colDef.getMetaProperty('foo')
        assert 'socks' == colDef.getMetaProperty('monkey')
    }

    @Test
    void testAddColumnWithSuggestedIdThatCollides()
    {
        Axis state = NCubeBuilder.statesAxis
        assert state.size() == 50
        Column oh = state.findColumn('OH')
        long ohId = oh.id
        Column dc = new Column('DC', oh.id)
        Column colAdded = state.addColumn(dc)
        dc = state.findColumn('DC')
        assert dc.id != ohId
        assert dc.value == colAdded.value
        assert state.size() == 51
    }

    @Test
    void testAddColumnWithSuggestedIdThatDoesNotCollides()
    {
        SecureRandom random = new SecureRandom()
        Axis state = NCubeBuilder.statesAxis
        assert state.size() == 50
        Column oh = state.findColumn('OH')
        long startId = random.nextInt(Axis.MAX_COLUMN_ID as Integer)
        Column dc = new Column('DC', startId)
        state.addColumn(dc)
        dc = state.findColumn('DC')
        assert oh.id != dc.id
        assert dc.value == 'DC'
        assert dc.id % Axis.MAX_COLUMN_ID == startId
        assert state.size() == 51
    }

    private static boolean isValidRange(Axis axis, Range range)
    {
        try
        {
            axis.addColumn(range)
            return true
        }
        catch (AxisOverlapException ignore)
        {
            return false
        }
    }
}
