package com.cedarsoftware.ncube


import com.cedarsoftware.ncube.exception.AxisOverlapException
import com.cedarsoftware.ncube.exception.CommandCellException
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.exception.InvalidCoordinateException
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point2D
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.ncube.util.VersionComparator
import com.cedarsoftware.util.CaseInsensitiveMap
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import ncube.grv.exp.NCubeGroovyExpression
import org.junit.Test

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.util.TestUtil.assertContainsIgnoreCase
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertSame
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * NCube tests.
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
class TestNCube extends NCubeBaseTest
{
    private static final boolean _debug = false

    @Test
    void testGroovyVersion()
    {
        println '----------------------------------'
        println "---> Groovy version: ${GroovySystem.version}"
        println '----------------------------------'
    }

    @Test
    void testPopulateProductLineCube()
    {
        NCube<Object> ncube = new NCube<>("ProductLine")
        ncubeRuntime.addCube(ncube)

        Axis prodLine = new Axis("PROD_LINE", AxisType.DISCRETE, AxisValueType.STRING, false)
        prodLine.addColumn("CommAuto")
        prodLine.addColumn("CommGL")
        prodLine.addColumn("CommIM")
        prodLine.addColumn("SBPProperty")
        ncube.addAxis(prodLine)

        Axis bu = new Axis("BU", AxisType.DISCRETE, AxisValueType.STRING, true)
        ncube.addAxis(bu)

        NCube<String> commAuto = new NCube<>("CommAuto")
        ncubeRuntime.addCube(commAuto)
        Axis caAttr = new Axis("Attribute", AxisType.DISCRETE, AxisValueType.STRING, false)
        caAttr.addColumn("busType")
        caAttr.addColumn("riskType")
        caAttr.addColumn("longNm")
        caAttr.addColumn("policySymbol")
        commAuto.addAxis(caAttr)

        NCube<String> commGL = new NCube<>("CommGL")
        ncubeRuntime.addCube(commGL)
        Axis glAttr = new Axis("Attribute", AxisType.DISCRETE, AxisValueType.STRING, false)
        glAttr.addColumn("busType")
        glAttr.addColumn("riskType")
        glAttr.addColumn("longNm")
        glAttr.addColumn("policySymbol")
        commGL.addAxis(glAttr)

        NCube<String> commIM = new NCube<>("CommIM")
        ncubeRuntime.addCube(commIM)
        Axis imAttr = new Axis("Attribute", AxisType.DISCRETE, AxisValueType.STRING, false)
        imAttr.addColumn("busType")
        imAttr.addColumn("riskType")
        imAttr.addColumn("longNm")
        imAttr.addColumn("policySymbol")
        imAttr.addColumn("parentRiskType")
        commIM.addAxis(imAttr)

        NCube<String> commSBP = new NCube<>("SBPProperty")
        ncubeRuntime.addCube(commSBP)
        Axis sbpAttr = new Axis("Attribute", AxisType.DISCRETE, AxisValueType.STRING, false)
        sbpAttr.addColumn("busType")
        sbpAttr.addColumn("riskType")
        sbpAttr.addColumn("longNm")
        sbpAttr.addColumn("policySymbol")
        sbpAttr.addColumn("busLobCd")
        commSBP.addAxis(sbpAttr)

        // Add cells to main table
        def coord = [:]
        coord.put("BU", null)    // default column
        coord.put("PROD_LINE", "CommAuto")
        ncube.setCell(new GroovyExpression("@CommAuto([:])", null, false), coord)
        coord.put("PROD_LINE", "CommGL")
        ncube.setCell(new GroovyExpression("@CommGL(input)", null, false), coord)
        coord.put("PROD_LINE", "CommIM")
        ncube.setCell(new GroovyExpression("\$CommIM(input)", null, false), coord)
        coord.put("PROD_LINE", "SBPProperty")
        ncube.setCell(new GroovyExpression("\$SBPProperty(input)", null, false), coord)

        coord.clear()
        coord.put("Attribute", "busType")
        commAuto.setCell("COB", coord)
        coord.put("Attribute", "riskType")
        commAuto.setCell("AUTOPS", coord)
        coord.put("Attribute", "longNm")
        commAuto.setCell("Commercial Auto", coord)
        coord.put("Attribute", "policySymbol")
        commAuto.setCell("CAP", coord)

        coord.clear()
        coord.put("Attribute", "busType")
        commGL.setCell("COB", coord)
        coord.put("Attribute", "riskType")
        commGL.setCell("CGLOPS", coord)
        coord.put("Attribute", "longNm")
        commGL.setCell("Commercial General Liability", coord)
        coord.put("Attribute", "policySymbol")
        commGL.setCell("GLP", coord)

        coord.clear()
        coord.put("Attribute", "busType")
        commIM.setCell("COB", coord)
        coord.put("Attribute", "riskType")
        commIM.setCell("EQPT", coord)
        coord.put("Attribute", "longNm")
        commIM.setCell("Contractors Equipment", coord)
        coord.put("Attribute", "policySymbol")
        commIM.setCell("MAC", coord)
        coord.put("Attribute", "parentRiskType")
        commIM.setCell("IMOPS", coord)

        coord.clear()
        coord.put("Attribute", "busType")
        commSBP.setCell("COB", coord)
        coord.put("Attribute", "riskType")
        commSBP.setCell("SBPOPS", coord)
        coord.put("Attribute", "longNm")
        commSBP.setCell("Select Business Policy", coord)
        coord.put("Attribute", "policySymbol")
        commSBP.setCell("MAC", coord)
        coord.put("Attribute", "busLobCd")
        commSBP.setCell("PPTY-SBP", coord)

        assertTrue(ncube.toHtml() != null)

        // ------------ Lookup into the Main table, and let it cascade to the children tables -------
        coord.clear()
        coord.put("BU", "Agri")
        coord.put("PROD_LINE", "CommAuto")
        coord.put("Attribute", "riskType")
        String riskType = (String) ncube.getCell(coord)
        assertTrue("AUTOPS".equals(riskType))

        Set<String> optionalScope = ncube.getOptionalScope([:], [:])
        optionalScope = ncube.getOptionalScope([:], [:])   // 2nd time to force fetch from cache
        assertEquals(1, optionalScope.size())
        assertTrue(optionalScope.contains("bu"))

        Set<String> requiredScope = ncube.getRequiredScope([:], [:])
        assertTrue(requiredScope.size() == 1)
        assertTrue(requiredScope.contains("PROD_LINE"))

        requiredScope = commAuto.getRequiredScope([:], [:])
        assertEquals(1, requiredScope.size())
        assertTrue(requiredScope.contains("attribute"))
        optionalScope = commAuto.getOptionalScope([:], [:])
        assertEquals(0, optionalScope.size())

        coord.clear()
        coord.put("BU", "Agri")
        coord.put("PROD_LINE", "CommGL")
        coord.put("Attribute", "riskType")

        requiredScope = ncube.getRequiredScope([:], [:])
        assertTrue(requiredScope.size() == 1)
        assertTrue(requiredScope.contains("PROD_LINE"))
    }

    @Test
    void testDuplicateAxisName()
    {
        NCube<Byte> ncube = new NCube<Byte>("Byte.Cube")
        ncube.defaultCellValue = (byte) -1
        Axis axis1 = NCubeBuilder.getGenderAxis(true)
        ncube.addAxis(axis1)
        Axis axis2 = NCubeBuilder.shortMonthsOfYear
        ncube.addAxis(axis2)
        Axis axis3 = NCubeBuilder.getGenderAxis(false)

        try
        {
            ncube.addAxis(axis3)
            assertTrue("should throw exception", false)
        }
        catch (IllegalArgumentException ignored)
        {
            assertTrue("should throw exception", true)
        }

        ncube.deleteAxis("miss")
        ncube.deleteAxis("Gender")
        ncube.addAxis(NCubeBuilder.getGenderAxis(true))
        ncube.toString()    // Force some APIs to be called during toString()
        assertTrue(ncube.toHtml() != null)
        assertTrue(ncube.numDimensions == 2)
    }

    @Test
    void testDefaultColumnOnly()
    {
        // 1D: 1 cell
        NCube<String> ncube = new NCube<String>("defaultOnly")
        ncube.addAxis(new Axis("BU", AxisType.DISCRETE, AxisValueType.STRING, true))

        def coord = [:]
        coord.put("BU", "foo")

        ncube.setCell("financial", coord)
        String s = ncube.getCell(coord)
        coord.put("BU", "bar")
        String t = ncube.getCell(coord)
        assertTrue("financial".equals(s))
        assertTrue(s.equals(t))

        // 2D: 1 cell (both axis only have default column)
        NCube<String> ncube2 = new NCube<String>("defaultOnly")
        ncube2.addAxis(new Axis("BU", AxisType.DISCRETE, AxisValueType.STRING, true))
        ncube2.addAxis(new Axis("age", AxisType.RANGE, AxisValueType.LONG, true))

        coord.clear()
        coord.put("BU", "foo")
        coord.put("age", 25)

        ncube2.setCell("bank", coord)
        s = ncube2.getCell(coord)
        coord.put("BU", "bar")
        t = ncube2.getCell(coord)
        coord.put("age", 19)
        String u = ncube2.getCell(coord)
        assertTrue("bank".equals(s))
        assertTrue(s.equals(t))
        assertTrue(t.equals(u))
        assertTrue(ncube2.toHtml() != null)
    }

    @Test
    void testAddAxisWithExistingCells()
    {
        NCube<String> ncube = new NCube<String>("existingCells")
        Axis buAxis = new Axis("BU", AxisType.DISCRETE, AxisValueType.STRING, false)
        ncube.addAxis(buAxis)
        buAxis.addColumn("foo")
        buAxis.addColumn("bar")

        def coord = [:]
        coord.put("BU", "foo")
        ncube.setCell("cell foo", coord)
        coord.put("BU", "bar")
        ncube.setCell("cell bar", coord)

        //Add a new axis with a default column
        Axis ageAxis = new Axis("age", AxisType.RANGE, AxisValueType.LONG, true)
        ncube.addAxis(ageAxis)

        //The existing cells should now be on the default column of the new axis
        coord.put("BU", "foo")
        coord.put("age", null)
        String s = ncube.getCell(coord)
        assertEquals("cell foo", s)
        coord.put("BU", "bar")
        coord.put("age", null)
        String t = ncube.getCell(coord)
        assertEquals("cell bar", t)

        //Add a new axis with no default column
        Axis prodLineAxis = new Axis("PROD_LINE", AxisType.DISCRETE, AxisValueType.STRING, false)
        prodLineAxis.addColumn("CommAuto")
        ncube.addAxis(prodLineAxis)

        //Cells should be cleared
        coord.put("BU", "foo")
        coord.put("age", null)
        coord.put("PROD_LINE", "CommAuto")
        s = ncube.getCell(coord)
        assertNull(s)
        coord.put("BU", "bar")
        coord.put("age", null)
        coord.put("PROD_LINE", "CommAuto")
        t = ncube.getCell(coord)
        assertNull(t)
    }

    @Test
    void testClearCells()
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

        ncube.clearCells()

        assertNull(ncube.getCell(male))
        assertNull(ncube.getCell(female))
        assertNull(ncube.getCell(nullGender))
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 4)
    }

    @Test
    void testIllegalArrayExceptions()
    {
        NCube ncube = (NCube) NCubeBuilder.getTestNCube2D(true)
        try
        {
            Object[] array = [] as Object[]
            ncube.setCell(array, [:] as Map)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("Cannot set"))
            assertTrue(e.message.contains("array type"))
        }

        try
        {
            ncube.setCellById([] as Object[], new HashSet<Long>())
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("Cannot set"))
            assertTrue(e.message.contains("array type"))
        }
    }

    @Test
    void testDefaultNCubeCellValue()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(true)
        ncube.setDefaultCellValue(3.0d)        // Non-set cells will return this value

        Map coord = [Gender:'Male', Age:18] as Map
        ncube.setCell(21.0d, coord)
        Double x = ncube.getCell(coord) as Double
        assertTrue(x == 21.0)
        coord.Age = 65
        x = ncube.getCell(coord) as Double
        assertTrue(x == 3.0)
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 5)
    }

    @Test
    void testDefaultValueForGetCell()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(true)
        Map coord = [Gender:'Male', Age:18] as Map
        ncube.setCell(21.0d, coord)
        Double x = ncube.getCell(coord, [:], -1) as Double
        assertTrue(x == 21.0)
        coord.Age = 65
        x = ncube.getCell(coord, [:], -1) as Double
        assertTrue(x == -1)
    }

    @Test
    void testLongAxis()
    {
        NCube<String> ncube = new NCube<String>("Long.test")
        ncube.addAxis(NCubeBuilder.getEvenAxis(false))

        Map coord = [Even:0 as byte] as Map
        ncube.setCell("zero", coord)
        coord.Even = (short) 2
        ncube.setCell("two", coord)
        coord.Even = (int) 4
        ncube.setCell("four", coord)
        coord.Even = (long) 6
        ncube.setCell("six", coord)
        coord['Even'] = "8"
        ncube.setCell("eight", coord)
        coord.Even = 10g
        ncube.setCell("ten", coord)

        coord.Even = 0
        assertTrue("zero".equals(ncube.getCell(coord)))
        coord.Even = 2L
        assertTrue("two".equals(ncube.getCell(coord)))
        coord.Even = (short) 4
        assertTrue("four".equals(ncube.getCell(coord)))
        coord.Even = (byte) 6
        assertTrue("six".equals(ncube.getCell(coord)))
        coord.Even = 8g
        assertTrue("eight".equals(ncube.getCell(coord)))
        coord['Even'] = "10"
        assertTrue("ten".equals(ncube.getCell(coord)))

        // Value not on axis
        try
        {
            coord.Even = 1
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains("alue"))
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("found"))
            assertTrue(e.message.contains("axis"))
        }

        // Illegal value to find on LONG axis:
        try
        {
            coord['Even'] = new File("foo")
            ncube.getCell(coord)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('unsupported value type')
        }

        ncube.toString() // force code in toString() to execute
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 7)
    }

    @Test
    void testLongAxis2()
    {
        Axis axis = new Axis("Long axis", AxisType.DISCRETE, AxisValueType.LONG, false, Axis.DISPLAY)
        axis.addColumn(0 as Byte)
        axis.addColumn(1 as Short)
        axis.addColumn(2 as Integer)
        axis.addColumn(3 as Long)
        axis.addColumn("4")
        axis.addColumn(new BigInteger("5"))
        axis.addColumn(new BigDecimal("6"))

        try
        {
            axis.addColumn(new Comparable() {
                int compareTo(Object o)
                {
                    return 0
                }
            })
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('unsupported value type')
        }

        assert axis.columns[0].value instanceof Long
        assert axis.columns[1].value instanceof Long
        assert axis.columns[2].value instanceof Long
        assert axis.columns[3].value instanceof Long
        assert axis.columns[4].value instanceof Long
        assert axis.columns[5].value instanceof Long
        assert axis.columns[6].value instanceof Long

        assert axis.columns[0].value == 0
        assert axis.columns[1].value == 1
        assert axis.columns[2].value == 2
        assert axis.columns[3].value == 3
        assert axis.columns[4].value == 4
        assert axis.columns[5].value == 5
        assert axis.columns[6].value == 6
    }

    @Test
    void testBigDecimalRangeAxis()
    {
        NCube<String> ncube = new NCube<String>("Big.Decimal.Range")
        Axis axis = NCubeBuilder.getDecimalRangeAxis(false)
        ncube.addAxis(axis)

        subTestErrorCases(axis)

        Map coord = [bigD:(byte) -10] as Map
        ncube.setCell("JSON", coord)
        coord.bigD = (short) 20
        ncube.setCell("XML", coord)
        coord.bigD = (int) 100
        ncube.setCell("YAML", coord)
        coord.bigD = 10000L
        ncube.setCell("PNG", coord)
        coord.bigD = "100000"
        ncube.setCell("JPEG", coord)

        coord.clear()
        coord.bigD = (byte) -10
        assertTrue("JSON".equals(ncube.getCell(coord)))
        coord.bigD = (short) 20
        assertTrue("XML".equals(ncube.getCell(coord)))
        coord.bigD = (int) 100
        assertTrue("YAML".equals(ncube.getCell(coord)))
        coord.bigD = 10000L
        assertTrue("PNG".equals(ncube.getCell(coord)))
        coord.bigD = 100000
        assertTrue("JPEG".equals(ncube.getCell(coord)))

        coord.bigD = (double) -10
        ncube.setCell("JSON", coord)
        coord.bigD = (float) 20
        ncube.setCell("XML", coord)
        coord.bigD = new BigInteger("100")
        ncube.setCell("YAML", coord)
        coord.bigD = new BigDecimal("10000")
        ncube.setCell("PNG", coord)
        coord.bigD = "100000"
        ncube.setCell("JPEG", coord)

        coord.bigD = (double) -10
        assertTrue("JSON".equals(ncube.getCell(coord)))
        coord.bigD = (float) 20
        assertTrue("XML".equals(ncube.getCell(coord)))
        coord.bigD = 100
        assertTrue("YAML".equals(ncube.getCell(coord)))
        coord.bigD = 10000L
        assertTrue("PNG".equals(ncube.getCell(coord)))
        coord.bigD = "100000"
        assertTrue("JPEG".equals(ncube.getCell(coord)))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 6)
        subTestEdgeCases(ncube, "bigD")
    }

    @Test
    void testDoubleRangeAxis()
    {
        NCube<String> ncube = new NCube<String>("Double.Range")
        Axis axis = NCubeBuilder.getDoubleRangeAxis(false)
        ncube.addAxis(axis)

        subTestErrorCases(axis)

        def coord = [:]
        coord.put("doubleRange", (byte) -10)
        ncube.setCell("JSON", coord)
        coord.put("doubleRange", (short) 20)
        ncube.setCell("XML", coord)
        coord.put("doubleRange", (int) 100)
        ncube.setCell("YAML", coord)
        coord.put("doubleRange", 10000L)
        ncube.setCell("PNG", coord)
        coord.put("doubleRange", "100000")
        ncube.setCell("JPEG", coord)

        coord.put("doubleRange", (byte) -10)
        assertTrue("JSON".equals(ncube.getCell(coord)))
        coord.put("doubleRange", (short) 20)
        assertTrue("XML".equals(ncube.getCell(coord)))
        coord.put("doubleRange", (int) 100)
        assertTrue("YAML".equals(ncube.getCell(coord)))
        coord.put("doubleRange", 10000L)
        assertTrue("PNG".equals(ncube.getCell(coord)))
        coord.put("doubleRange", "100000")
        assertTrue("JPEG".equals(ncube.getCell(coord)))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 6)
        subTestEdgeCases(ncube, "doubleRange")
    }

    @Test
    void testLongRangeAxis()
    {
        NCube<String> ncube = new NCube<String>("Long.Range")
        Axis axis = NCubeBuilder.getLongRangeAxis(false)
        ncube.addAxis(axis)

        subTestErrorCases(axis)

        def coord = [:]
        coord.put("longRange", (byte) -10)
        ncube.setCell("JSON", coord)
        coord.put("longRange", (short) 20)
        ncube.setCell("XML", coord)
        coord.put("longRange", (int) 100)
        ncube.setCell("YAML", coord)
        coord.put("longRange", 10000L)
        ncube.setCell("PNG", coord)
        coord.put("longRange", "100000")
        ncube.setCell("JPEG", coord)

        coord.put("longRange", (byte) -10)
        assertTrue("JSON".equals(ncube.getCell(coord)))
        coord.put("longRange", (short) 20)
        assertTrue("XML".equals(ncube.getCell(coord)))
        coord.put("longRange", (int) 100)
        assertTrue("YAML".equals(ncube.getCell(coord)))
        coord.put("longRange", 10000L)
        assertTrue("PNG".equals(ncube.getCell(coord)))
        coord.put("longRange", "100000")
        assertTrue("JPEG".equals(ncube.getCell(coord)))
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 6)

        subTestEdgeCases(ncube, "longRange")
    }

    @Test
    void testDateRangeAxis()
    {
        NCube<String> ncube = new NCube<>("Date.Range")
        Axis axis = NCubeBuilder.getDateRangeAxis(false)
        ncube.addAxis(axis)

        subTestErrorCases(axis)

        Calendar cal = Calendar.instance
        cal.set(1990, 5, 10, 13, 5, 25)
        Calendar cal1 = Calendar.instance
        cal1.set(2000, 0, 1, 0, 0, 0)
        Calendar cal2 = Calendar.instance
        cal2.set(2002, 11, 17, 0, 0, 0)
        Calendar cal3 = Calendar.instance
        cal3.set(2008, 11, 24, 0, 0, 0)
        Calendar cal4 = Calendar.instance
        cal4.set(2010, 0, 1, 12, 0, 0)
        Calendar cal5 = Calendar.instance
        cal5.set(2014, 7, 1, 12, 59, 58)

        Map coord = [:] as Map
        coord.dateRange = cal
        ncube.setCell("JSON", coord)
        coord.dateRange = cal1.time
        ncube.setCell("XML", coord)
        coord.dateRange = cal2.time.time
        ncube.setCell("YAML", coord)
        coord.dateRange = cal4
        ncube.setCell("PNG", coord)

        coord.dateRange = cal
        assertTrue("JSON".equals(ncube.getCell(coord)))
        coord.dateRange = cal1
        assertTrue("XML".equals(ncube.getCell(coord)))
        coord.dateRange = cal2
        assertTrue("YAML".equals(ncube.getCell(coord)))
        coord.dateRange = cal4
        assertTrue("PNG".equals(ncube.getCell(coord)))

        assertFalse(axis.contains(99))
        assertTrue(axis.contains(cal5))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 5)
        subTestEdgeCases(ncube, "dateRange")
    }

    private static void subTestEdgeCases(NCube<String> cube, String axis)
    {
        def coord = [:]
        try
        {
            coord.put(axis, -20)
            cube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.contains("not found on axis")
            assert cube.name == e.cubeName
            assert coord == e.coordinate
            assert axis == e.axisName
            assert -20 == e.value
        }
        catch (Exception ignored)
        {
            // varies
        }

        // 'null' value to find on String axis:
        try
        {
            coord.put(axis, null)
            cube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains('null')
            assert e.message.toLowerCase().contains('not found on axis')
            assert cube.name == e.cubeName
            assert coord == e.coordinate
            assert axis == e.axisName
            assert !e.value
        }

        // Illegal value to find on String axis:
        try
        {
            coord.put(axis, new File("foo"))
            cube.getCell(coord)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            String msg = e.message.toLowerCase()
            assert msg.contains('could not be converted') || msg.contains("unsupported value type")
        }
    }

    private static void subTestErrorCases(Axis axis)
    {
        // non-range being added
        try
        {
            axis.addColumn(new Long(7))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('only add range')
        }

        // Range with null low
        try
        {
            axis.addColumn(new Range(null, 999))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('range value cannot be null')
        }

        // Range with null high
        try
        {
            axis.addColumn(new Range(777, null))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('range value cannot be null')
        }

        // Range with bad low
        try
        {
            axis.addColumn(new Range("no", "999"))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('could not be converted')
        }

        // Range with bad high
        try
        {
            axis.addColumn(new Range("999", "yes"))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('could not be converted')
        }
    }

    @Test
    void testRange()
    {
        Range x = new Range() // test default constructor

        x = new Range(0, 1)
        x.toString()    // so it gets called at least once.

        NCube ncube = new NCube("RangeTest")
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true)
        axis.addColumn(new Range(22, 18))
        axis.addColumn(new Range(30, 22))
        ncube.addAxis(axis)
        Map coord = [:] as Map
        coord.Age = 17
        ncube.setCell(1.1, coord)    // set in default column
        assertEquals((double)ncube.getCell(coord), 1.1d, 0.00001d)
        coord.Age = 18
        ncube.setCell(2.0, coord)
        assertEquals((double) ncube.getCell(coord), 2.0d, 0.00001d)
        coord.Age = 21
        assertEquals((double)ncube.getCell(coord), 2.0d, 0.00001d)
        coord.Age = 22
        assertTrue(ncube.getCell(coord) == null)    // cell not set, therefore it should return null
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 4)

        try
        {
            x = new Range(null, 1)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("value"))
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("null"))
        }
        try
        {
            x = new Range(1, null)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("value"))
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("null"))
        }
    }

    @Test
    void testRangeWithDefault()
    {
        NCube ncube = new NCube("RangeTest")
        Axis axis = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true)
        ncube.addAxis(axis)

        Map coord = [age:1] as Map
        ncube.setCell(1.0, coord)
        assertEquals((Object) 1.0, ncube.getCell(coord))

        axis.addColumn(new Range(18, 22))
        coord.put("age", 18)
        ncube.setCell(2.0, coord)
        assertEquals((Object) 2.0, ncube.getCell(coord))

        axis.addColumn(new Range(5, 8))
        coord.put("age", 6)
        ncube.setCell(3.0, coord)
        assertEquals((Object) 3.0, ncube.getCell(coord))

        axis.addColumn(new Range(30, 40))
        coord.put("age", 35)
        ncube.setCell(4.0, coord)
        assertEquals((Object) 4.0, ncube.getCell(coord))

        axis.addColumn(new Range(1, 4))
        coord.put("age", 1)
        ncube.setCell(5.0, coord)
        assertEquals((Object) 5.0, ncube.getCell(coord))

        axis.addColumn(new Range(40, 50))
        coord.put("age", 40)
        ncube.setCell(6.0, coord)
        assertEquals((Object) 6.0, ncube.getCell(coord))
    }

    @Test
    void testGetCellWithMap()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(false)
        Map coord = [:] as Map
        coord.put("Gender", "Male")
        coord.put("Age", 39)
        ncube.setCell(9.9d, coord)
        assertTrue(ncube.getCell(coord) == 9.9)
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 4)

        coord.put("Gender", "Fmale")    // intentional
        try
        {
            ncube.setCell(9.9d, coord)
            fail("should throw an exception")
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("found"))
            assertTrue(e.message.contains("axis"))
            assertTrue(e.message.contains("Gender"))
            assertEquals(ncube.name, e.cubeName)
            assertEquals(coord, e.coordinate)
            assertEquals("Gender", e.axisName)
            assertEquals("Fmale", e.value)
        }
        catch (Exception ignore)
        {
            fail("should throw CoordinateNotFoundException")
        }
    }

    @Test
    void testWithImproperMapCoordinate()
    {
        // 'null' Map
        NCube ncube = NCubeBuilder.getTestNCube2D(false)
        try
        {
            ncube.setCell(9.9d, null)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assertContainsIgnoreCase(e.message, 'null', 'not found')
        }

        // Empty Map
        Map coord = [:] as Map
        try
        {
            ncube.setCell(9.9d, coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains('not found'))
            assertEquals(ncube.name, e.cubeName)
            assertEquals(coord, e.coordinate)
            assertTrue(['Age', 'Gender'].contains(e.axisName))
            assertNull(e.value)
        }

        // Map with not enough dimensions
        coord.put("Gender", "Male")
        try
        {
            ncube.setCell(9.9d, coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains("not found"))
        }
    }

    @Test
    void testNullCoordinate()
    {
        NCube<Boolean> ncube = NCubeBuilder.testNCube3D_Boolean
        ncube.defaultCellValue = false

        def coord = [:]
        coord.put("Trailers", "L1A")
        coord.put("Vehicles", "car")
        coord.put("BU", "Agri")
        ncube.setCell(true, coord)

        coord.put("Trailers", "M3A")
        coord.put("Vehicles", "med truck")
        coord.put("BU", "SHS")
        ncube.setCell(true, coord)
        try
        {
            ncube.getCell((Map)null)    // (Object[]) cast makes it the whole argument list
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("null"))
            assertTrue(e.message.contains("coordinate"))
        }

        try
        {
            coord.remove("BU")
            ncube.getCell(coord)        // (Object) cast makes it one argument
            fail()
        }
        catch (InvalidCoordinateException e)
        {
            assertTrue(e.message.contains("required scope"))
            assert ncube.name == e.cubeName
            assert coord.keySet() == e.coordinateKeys
            assert ['Trailers', 'Vehicles', 'BU'] as Set == e.requiredKeys
        }

        try
        {
            coord.put("Trailers", null)
            coord.put("Vehicles", null)
            coord.put("BU", null)
            ncube.getCell(coord)        // Valid 3D coordinate (if table had default col on all axis)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains("null"))
            assertTrue(e.message.contains("not found on axis"))
            assertEquals(ncube.name, e.cubeName)
            assertEquals(coord, e.coordinate)
            assertTrue(['Trailers', 'Vehicles', 'BU'].contains(e.axisName))
            assertNull(e.value)

        }
    }

    @Test
    void testCommandCellLookup()
    {
        NCube<Object> continentCounty = new NCube<Object>("ContinentCountries")
        ncubeRuntime.addCube(continentCounty)
        continentCounty.addAxis(NCubeBuilder.continentAxis)
        Axis countries = new Axis("Country", AxisType.DISCRETE, AxisValueType.STRING, true)
        countries.addColumn("Canada")
        countries.addColumn("USA")
        continentCounty.addAxis(countries)

        NCube<Object> canada = new NCube<Object>("Provinces")
        ncubeRuntime.addCube(canada)
        canada.addAxis(NCubeBuilder.provincesAxis)

        NCube<Object> usa = new NCube<Object>("States")
        ncubeRuntime.addCube(usa)
        usa.addAxis(NCubeBuilder.statesAxis)

        Map coord1 = [Continent:'North America', Country:'USA', State:'OH'] as Map
        Map coord2 = [Continent:'North America', Country:'Canada', Province:'Quebec'] as Map

        continentCounty.setCell(new GroovyExpression("\$States(input)", null, false), coord1)
        continentCounty.setCell(new GroovyExpression("\$Provinces(input)", null, false), coord2)

        usa.setCell(1.0d, coord1)
        canada.setCell(0.78d, coord2)

        assertEquals((Double) continentCounty.getCell(coord1), 1.0d, 0.00001d)
        assertEquals((Double) continentCounty.getCell(coord2), 0.78d, 0.00001d)
        assertTrue(countMatches(continentCounty.toHtml(), "<tr") == 5)
    }

    @Test
    void testBadCommandCellLookup()
    {
        NCube<Object> continentCounty = new NCube<Object>("ContinentCountries")
        ncubeRuntime.addCube(continentCounty)
        continentCounty.addAxis(NCubeBuilder.continentAxis)
        Axis countries = new Axis("Country", AxisType.DISCRETE, AxisValueType.STRING, true)
        countries.addColumn("Canada")
        countries.addColumn("USA")
        continentCounty.addAxis(countries)

        NCube canada = new NCube("Provinces")
        ncubeRuntime.addCube(canada)
        canada.addAxis(NCubeBuilder.provincesAxis)

        NCube usa = new NCube("States")
        ncubeRuntime.addCube(usa)
        usa.addAxis(NCubeBuilder.statesAxis)

        Map coord1 = [Continent:'North America', Country:'USA', State:'OH'] as Map
        Map coord2 = [Continent:'North America', Country:'Canada', Province:'Quebec'] as Map

        continentCounty.setCell(new GroovyExpression("\$StatesX(input)", null, false), coord1)
        continentCounty.setCell(new GroovyExpression("\$Provinces(input)", null, false), coord2)

        usa.setCell(1.0d, coord1)
        canada.setCell(0.78d, coord2)

        try
        {
            assertEquals((Double) continentCounty.getCell(coord1), 1.0d, 0.00001d)
            fail("should throw exception")
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred")
        }
        assertEquals((Double) continentCounty.getCell(coord2), 0.78d, 0.00001d)
    }

    @Test
    void testAddingDeletingColumn1D()
    {
        NCube<Long> ncube = new NCube<Long>("1D.Delete.Test")
        Axis states = new Axis("States", AxisType.DISCRETE, AxisValueType.STRING, true)
        states.addColumn("IN")
        states.addColumn("OH")
        states.addColumn("WY")
        ncube.addAxis(states)
        def coord = [:]

        coord.put("States", "IN")
        ncube.setCell(1111L, coord)
        assertTrue(ncube.getCell(coord) == 1111L)
        coord.put("States", "OH")
        ncube.setCell(2222L, coord)
        assertTrue(ncube.getCell(coord) == 2222L)
        coord.put("States", "WY")
        ncube.setCell(3333L, coord)
        assertTrue(ncube.getCell(coord) == 3333L)
        coord.put("States", null)
        ncube.setCell(9999L, coord)

        // Add new Column
        states.addColumn("AZ")

        coord.put("States", "IN")
        assertTrue(ncube.getCell(coord) == 1111L)
        coord.put("States", "OH")
        assertTrue(ncube.getCell(coord) == 2222L)
        coord.put("States", "WY")
        assertTrue(ncube.getCell(coord) == 3333L)

        coord.put("States", "AZ")
        ncube.setCell(4444L, coord)
        assertTrue(ncube.getCell(coord) == 4444L)

        coord.put("States", "IN")
        int numCells = ncube.numCells
        assertTrue(ncube.deleteColumn("States", "IN" as Comparable))
        assertTrue(numCells == ncube.numCells + 1i)

        assertTrue(ncube.getCell(coord) == 9999L)
        coord.put("States", "OH")
        assertTrue(ncube.getCell(coord) == 2222L)
        coord.put("States", "WY")
        assertTrue(ncube.getCell(coord) == 3333L)

        coord.put("States", "AZ")
        ncube.setCell(4444L, coord)
        assertTrue(ncube.getCell(coord) == 4444L)

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 5)
    }

    @Test
    void testAddingDeletingColumn2D()
    {
        NCube ncube = new NCube("2D.Delete.Test")
        Axis states = new Axis("States", AxisType.DISCRETE, AxisValueType.STRING, true)
        states.addColumn("IN")
        states.addColumn("OH")
        ncube.addAxis(states)
        Axis age = new Axis("Age", AxisType.RANGE, AxisValueType.LONG, true)
        age.addColumn(new Range(18, 30))
        age.addColumn(new Range(30, 50))
        age.addColumn(new Range(50, 80))
        ncube.addAxis(age)

        Map coord = [States:'IN'] as Map

        coord.Age = "18"
        ncube.setCell(1.0, coord)
        coord.Age = "30"
        ncube.setCell(2.0, coord)
        coord.Age = "50"
        ncube.setCell(3.0, coord)
        coord.Age = "90"
        ncube.setCell(4.0, coord)

        coord.States = "OH"
        coord.Age = 29
        ncube.setCell(10.0, coord)
        coord.Age = 30
        ncube.setCell(20.0, coord)
        coord.Age = 50
        ncube.setCell(30.0, coord)
        coord.Age = 80
        ncube.setCell(40.0, coord)

        coord.States = "WY"        // default col
        coord.Age = 20.0
        ncube.setCell(100.0, coord)
        coord.Age = 40.0
        ncube.setCell(200.0, coord)
        coord.Age = 60.0
        ncube.setCell(300.0, coord)
        coord.Age = 80.0
        ncube.setCell(400.0, coord)

        ncube.deleteColumn("Age", 90)
        assertTrue(age.size() == 3)
        assertFalse(age.hasDefaultColumn())    // default column was deleted.

        assertTrue(ncube.numCells == 9)
        assertTrue(ncube.deleteColumn("Age", 18))
        assertTrue(ncube.numCells == 6)
        assertTrue(age.size() == 2)
        assertTrue(ncube.deleteColumn("States", "IN"))
        assertTrue(ncube.numCells == 4)
        assertTrue(states.size() == 2)

        coord.put("States", "OH")
        coord.put("Age", 30)
        assertTrue(ncube.getCell(coord) == 20.0)
        coord.put("Age", 50)
        assertTrue(ncube.getCell(coord) == 30.0)

        coord.put("States", "WY")
        coord.put("Age", 40.0)
        assertTrue(ncube.getCell(coord) == 200.0)
        coord.put("Age", 60.0)
        assertTrue(ncube.getCell(coord) == 300.0)

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 4)
    }

    @Test
    void testDeleteColumnNotFound()
    {
        NCube<Boolean> ncube = new NCube("yo")
        Axis axis = NCubeBuilder.getGenderAxis(false)
        ncube.addAxis(axis)
        assertFalse(ncube.deleteColumn("Gender", "blah" as Comparable))
    }

    @Test
    void testColumnOrder()
    {
        NCube ncube = new NCube("columnOrder")
        Axis axis = NCubeBuilder.shortDaysOfWeekAxis
        axis.columnOrder = Axis.SORTED
        ncube.addAxis(axis)
        List<Column> cols = axis.columns
        assertTrue(cols.get(0).value.equals("Fri"))
        assertTrue(cols.get(1).value.equals("Mon"))
        assertTrue(cols.get(2).value.equals("Sat"))
        assertTrue(cols.get(3).value.equals("Sun"))
        assertTrue(cols.get(4).value.equals("Thu"))
        assertTrue(cols.get(5).value.equals("Tue"))
        assertTrue(cols.get(6).value.equals("Wed"))

        axis.columnOrder = Axis.DISPLAY
        List<Column> cols2 = axis.columns
        assertTrue(cols2.get(0).value.equals("Mon"))
        assertTrue(cols2.get(1).value.equals("Tue"))
        assertTrue(cols2.get(2).value.equals("Wed"))
        assertTrue(cols2.get(3).value.equals("Thu"))
        assertTrue(cols2.get(4).value.equals("Fri"))
        assertTrue(cols2.get(5).value.equals("Sat"))
        assertTrue(cols2.get(6).value.equals("Sun"))

        // Delete middle
        ncube.deleteColumn("Days", "Wed")

        axis.columnOrder = Axis.SORTED
        cols = axis.columns
        assertTrue(cols.get(0).value.equals("Fri"))
        assertTrue(cols.get(1).value.equals("Mon"))
        assertTrue(cols.get(2).value.equals("Sat"))
        assertTrue(cols.get(3).value.equals("Sun"))
        assertTrue(cols.get(4).value.equals("Thu"))
        assertTrue(cols.get(5).value.equals("Tue"))

        axis.columnOrder = Axis.DISPLAY
        cols2 = axis.columns
        assertTrue(cols2.get(0).value.equals("Mon"))
        assertTrue(cols2.get(1).value.equals("Tue"))
        assertTrue(cols2.get(2).value.equals("Thu"))
        assertTrue(cols2.get(3).value.equals("Fri"))
        assertTrue(cols2.get(4).value.equals("Sat"))
        assertTrue(cols2.get(5).value.equals("Sun"))

        // Ensure no gaps left in display order after column is removed
        assertTrue(cols2.get(0).displayOrder == 1)
        assertTrue(cols2.get(1).displayOrder == 2)
        assertTrue(cols2.get(2).displayOrder == 4)
        assertTrue(cols2.get(3).displayOrder == 5)
        assertTrue(cols2.get(4).displayOrder == 6)
        assertTrue(cols2.get(5).displayOrder == 7)

        // Delete First
        ncube.deleteColumn("Days", "Mon")
        cols2 = axis.columns
        assertTrue(cols2.get(0).value.equals("Tue"))
        assertTrue(cols2.get(1).value.equals("Thu"))
        assertTrue(cols2.get(2).value.equals("Fri"))
        assertTrue(cols2.get(3).value.equals("Sat"))
        assertTrue(cols2.get(4).value.equals("Sun"))

        // Ensure no gaps left in display order after column is removed
        assertTrue(cols2.get(0).displayOrder == 2)
        assertTrue(cols2.get(1).displayOrder == 4)
        assertTrue(cols2.get(2).displayOrder == 5)
        assertTrue(cols2.get(3).displayOrder == 6)
        assertTrue(cols2.get(4).displayOrder == 7)

        // Delete Last
        ncube.deleteColumn("Days", "Sun")
        cols2 = axis.columns
        assertTrue(cols2.get(0).value.equals("Tue"))
        assertTrue(cols2.get(1).value.equals("Thu"))
        assertTrue(cols2.get(2).value.equals("Fri"))
        assertTrue(cols2.get(3).value.equals("Sat"))

        // Ensure no gaps left in display order after column is removed
        assertTrue(cols2.get(0).displayOrder == 2)
        assertTrue(cols2.get(1).displayOrder == 4)
        assertTrue(cols2.get(2).displayOrder == 5)
        assertTrue(cols2.get(3).displayOrder == 6)
    }

    @Test
    void testColumnOrder2()
    {
        Axis axis = NCubeBuilder.shortDaysOfWeekAxis
        List<Column> cols = axis.columns

        // Alphabetical
        assertTrue(cols.get(0).value.equals("Mon"))
        assertTrue(cols.get(1).value.equals("Tue"))
        assertTrue(cols.get(2).value.equals("Wed"))
        assertTrue(cols.get(3).value.equals("Thu"))
        assertTrue(cols.get(4).value.equals("Fri"))
        assertTrue(cols.get(5).value.equals("Sat"))
        assertTrue(cols.get(6).value.equals("Sun"))

        assertTrue(cols.get(0).displayOrder == 1)
        assertTrue(cols.get(1).displayOrder == 2)
        assertTrue(cols.get(2).displayOrder == 3)
        assertTrue(cols.get(3).displayOrder == 4)
        assertTrue(cols.get(4).displayOrder == 5)
        assertTrue(cols.get(5).displayOrder == 6)
        assertTrue(cols.get(6).displayOrder == 7)

        axis.deleteColumn("Mon")
        axis.deleteColumn("Tue")
        axis.deleteColumn("Wed")
        axis.deleteColumn("Thu")
        cols = axis.columns
        assertTrue(cols.get(0).displayOrder == 5)
        assertTrue(cols.get(1).displayOrder == 6)
        assertTrue(cols.get(2).displayOrder == 7)

        axis.addColumn("Zee")
        cols = axis.columns
        assertTrue(cols.get(3).value.equals('Zee'))
        assertTrue(cols.get(3).displayOrder == 8)
    }

    @Test
    void testColumnApis()
    {
        NCube ncube = new NCube("columnApis")
        Axis axis = NCubeBuilder.shortMonthsOfYear
        ncube.addAxis(axis)
        try
        {
            ncube.addColumn("foo", "13th month")
            fail()
        }
        catch (Exception e)
        {
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("add"))
            assertTrue(e.message.contains("column"))
        }

        try
        {
            ncube.deleteColumn("foo", "13th month")
            fail()
        }
        catch (Exception e)
        {
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("delete"))
            assertTrue(e.message.contains("column"))
        }
    }

    @Test
    void testGenericComparables()
    {
        NCube<String> ncube = new NCube<String>("Test.BigInteger")
        Axis age = new Axis("Age", AxisType.DISCRETE, AxisValueType.COMPARABLE, true)
        age.addColumn(1g)
        age.addColumn(2g)
        age.addColumn(4g)
        age.addColumn(7g)
        age.addColumn(10g)
        ncube.addAxis(age)

        Map coord = [Age:1g] as Map
        ncube.setCell("alpha", coord)
        coord.Age = 2g
        ncube.setCell("bravo", coord)
        coord.Age = 3g    // should land it default column
        ncube.setCell("charlie", coord)
        coord.Age = 4g
        ncube.setCell("delta", coord)

        coord.Age = 1g
        assertTrue("alpha".equals(ncube.getCell(coord)))
        coord.Age = 2g
        assertTrue("bravo".equals(ncube.getCell(coord)))
        coord.Age = 5g        // Verify default column
        assertTrue("charlie".equals(ncube.getCell(coord)))
        coord.Age = 4g
        assertTrue("delta".equals(ncube.getCell(coord)))
    }

    @Test
    void testGenericRangeComparables()
    {
        NCube<String> ncube = new NCube<String>("Test.Character")
        Axis codes = new Axis("codes", AxisType.RANGE, AxisValueType.COMPARABLE, true)
        codes.addColumn(new Range('a', 'd'))
        codes.addColumn(new Range('d', 'm'))
        codes.addColumn(new Range('m', 'y'))
        ncube.addAxis(codes)

        def coord = [:]
        coord.put("codes", 'a')
        ncube.setCell("alpha", coord)
        coord.put("codes", 'd')
        ncube.setCell("bravo", coord)
        coord.put("codes", 't')    // should land it default column
        ncube.setCell("charlie", coord)
        coord.put("codes", 'z')
        ncube.setCell("delta", coord)

        coord.put("codes", 'a')
        assertTrue("alpha".equals(ncube.getCell(coord)))
        coord.put("codes", 'd')
        assertTrue("bravo".equals(ncube.getCell(coord)))
        coord.put("codes", 't')    // Verify default column
        assertTrue("charlie".equals(ncube.getCell(coord)))
        coord.put("codes", '@')
        assertTrue("delta".equals(ncube.getCell(coord)))

        Range range = new Range(10, 50)
        assertTrue(range.isWithin(null) == 1)
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 5)
    }

    @Test
    void testRangeSet()
    {
        NCube ncube = new NCube("RangeSetTest")
        Axis age = new Axis("Age", AxisType.SET, AxisValueType.LONG, true)
        RangeSet set = new RangeSet(1)
        set.add(3.0)
        set.add(new Range(10, 20))
        set.add(25)
        assertTrue(set.size() == 4)
        age.addColumn(set)

        set = new RangeSet(2)
        set.add(20L)
        set.add(35 as Byte)
        assertTrue(set.size() == 3)
        age.addColumn(set)
        ncube.addAxis(age)

        Map coord = [:] as Map
        coord.Age = 1
        ncube.setCell(1.0, coord)
        coord.Age = 2
        ncube.setCell(2.0, coord)
        coord.Age = 99
        ncube.setCell(99.9, coord)

        coord.clear()
        coord.age = 1        // intentional case mismatch
        assertTrue(ncube.getCell(coord) == 1.0)
        coord.age = 2        // intentional case mismatch
        assertTrue(ncube.getCell(coord) == 2.0)

        coord.clear()
        coord.Age = 3
        ncube.setCell(3.0, coord)
        coord.Age = 1
        assertTrue(ncube.getCell(coord) == 3.0)  // 1 & 3 share same cell

        coord.Age = 35
        ncube.setCell(35.0, coord)
        coord.Age = 20
        assertTrue(ncube.getCell(coord) == 35.0)

        coord.Age = "10"
        ncube.setCell(10.0, coord)
        coord.Age = 1
        assertTrue(ncube.getCell(coord) == 10.0)

        coord.Age = 80
        assertTrue(ncube.getCell(coord) == 99.9)

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 4)
    }

    @Test
    void testNearestAxisType()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'point2d.json')

        def coord = [:]

        coord.put("Point", new Point2D(0.0d, 0.0d))
        assertEquals("0.0, 0.0", ncube.getCell(coord))

        coord.put("Point", new Point2D(-0.1d, 0.1d))
        assertEquals("0.0, 0.0", ncube.getCell(coord))

        coord.put("Point", new Point2D(0.49d, 0.49d))
        assertEquals("0.0, 0.0", ncube.getCell(coord))

        coord.put("Point", new Point2D(0.55d, 0.0d))
        assertEquals("1.0, 0.0", ncube.getCell(coord))

        coord.put("Point", new Point2D(-1.0d, 50))
        assertEquals("0.0, 1.0", ncube.getCell(coord))

        coord.put("Point", new Point2D(-1.5d, -0.4d))
        assertEquals("-1.0, 0.0", ncube.getCell(coord))

        coord.put("Point", new Point2D(0.5d, -0.6d))
        assertEquals("0.0, -1.0", ncube.getCell(coord))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 6)

        Axis points = new Axis("Point", AxisType.NEAREST, AxisValueType.COMPARABLE, true)
        assert !points.hasDefaultColumn()
        assert points.columnOrder == Axis.SORTED

        points = new Axis("Point", AxisType.NEAREST, AxisValueType.COMPARABLE, false)
        points.addColumn(new Point2D(0.0, 0.0))
        points.addColumn(new Point2D(1.0, 0.0))
        points.addColumn(new Point2D(0.0, 1.0))
        points.addColumn(new Point2D(-1.0, 0.0))
        points.addColumn(new Point2D(0.0, -1.0))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 6)

        try
        {
            points.addColumn(new Point3D(1.0, 2.0, 3.0))
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("cannot"))
            assertTrue(e.message.contains("add"))
            assertTrue(e.message.contains("axis"))
        }

        try
        {
            points.addColumn(new Point2D(0.0, 0.0))
            fail()
        }
        catch (AxisOverlapException e)
        {
            assertTrue(e.message.contains("matches"))
            assertTrue(e.message.contains("value"))
            assertTrue(e.message.contains("already"))
        }

        try
        {
            points.addColumn("12")
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("cannot"))
            assertTrue(e.message.contains("add"))
            assertTrue(e.message.contains("oximity"))
        }

        Point2D p1 = new Point2D(24.0, 36.0)
        Point2D p2 = new Point2D(24.0, 36.0)
        Point2D p3 = new Point2D(36.0, 24.0)
        assertTrue(p1.equals(p2))
        assertTrue(p1.compareTo(p2) == 0)
        assertEquals(p1.x, p2.x, 0.0001d)
        assertEquals(p1.y, p2.y, 0.0001d)
        assertFalse(p2.equals(p3))
        assertFalse(p1.equals("string"))
    }

    @Test
    void testLatLonAxisType()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'latlon.json')

        String axisName = "Lat / Lon"

        Map coord = new HashMap<String, Object>()
        coord.put("Lat / Lon", new LatLon(25, -112))
        assertEquals("Austin", cube.getCell(coord))

        coord = new HashMap<String, Object>()
        coord.put("Lat / Lon", new LatLon(35, -90))
        assertEquals("Springboro", cube.getCell(coord))

        LatLon newYork = new LatLon(40.714353, -74.005973)
        LatLon losAngeles = new LatLon(34.052234, -118.243685)
        LatLon phoenix = new LatLon(33.448377, -112.074037)
        LatLon elpaso = new LatLon(31.75872, -106.486931)

        coord.put(axisName, newYork)
        assertEquals("Springboro", cube.getCell(coord))

        coord.put(axisName, losAngeles)
        assertEquals("Breckenridge", cube.getCell(coord))

        coord.put(axisName, phoenix)
        assertEquals("Breckenridge", cube.getCell(coord))

        coord.put(axisName, elpaso)
        assertEquals("Austin", cube.getCell(coord))
    }

    @Test
    void testSimpleJson1()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube6.json')
        assertTrue("TestCube".equals(ncube.name))
        Calendar cal = Calendar.instance
        cal.clear()
        cal.set(2012, Calendar.DECEMBER, 17, 0, 11, 22)
        assertTrue(cal.time.time == ((Date) ncube.defaultCellValue).time)
        List<Axis> axes = ncube.axes
        assertTrue(axes.size() == 1)
        Axis gender = axes.get(0)
        assertTrue("Gender".equals(gender.name))
        assertTrue(gender.type == AxisType.DISCRETE)
        assertTrue(gender.valueType == AxisValueType.STRING)
        assertTrue(gender.columnOrder == Axis.SORTED)
        List<Column> columns = gender.columns
        assertTrue(columns.size() == 3)
        assertTrue(gender.size() == 3)   // default column = true
        assertTrue(columns.get(0).value.equals("Female"))
        assertTrue(columns.get(1).value.equals("Male"))

        def coord = [:]
        coord.put("Gender", "Male")
        assertEquals((Double) ncube.getCell(coord), 1.0d, 0.00001d)
        coord.put("Gender", "Female")
        assertEquals((Double) ncube.getCell(coord), 1.1d, 0.00001d)
    }

    @Test
    void testSimpleJson2()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube5.json')
        def coord = [:]
        coord.put("Age", 10)
        assertEquals((Double) ncube.getCell(coord), 9.0d, 0.00001d)
        coord.put("Age", 22)
        assertEquals((Double) ncube.getCell(coord), 5.0d, 0.00001d)
        coord.put("Age", 28)
        assertEquals((Double) ncube.getCell(coord), 2.7d, 0.00001d)
        coord.put("Age", 50)
        assertEquals((Double) ncube.getCell(coord), 1.5d, 0.00001d)
        coord.put("Age", 69)
        assertEquals((Double) ncube.getCell(coord), 1.8d, 0.00001d)
        coord.put("Age", 75)
        assertEquals((Double) ncube.getCell(coord), 9.0d, 0.00001d)
    }

    @Test
    void testSimpleJson3()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube4.json')
        def coord = [:]

        coord.put("Code", "a")
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", "o")
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", "t")
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", "y")
        assertTrue("ABC".equals(ncube.getCell(coord)))

        coord.put("Code", "b")
        assertTrue("DEF".equals(ncube.getCell(coord)))
        coord.put("Code", "d")
        assertTrue("DEF".equals(ncube.getCell(coord)))

        coord.put("Code", "h")
        assertTrue("ZZZ".equals(ncube.getCell(coord)))
        coord.put("Code", "i")
        assertTrue("ZZZ".equals(ncube.getCell(coord)))
        coord.put("Code", "w")
        assertTrue("ZZZ".equals(ncube.getCell(coord)))

        coord.put("Code", "mic")
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", "november")
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", "oscar")
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", "xray")
        assertTrue("ABC".equals(ncube.getCell(coord)))

        try
        {
            coord.put("Code", "p")
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        {
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("found"))
            assertTrue(e.message.contains("axis"))
        }
    }

    @Test
    void testNearestAxisStringType()
    {
        NCube<String> ncube = new NCube<String>("NearestString")

        // The last parameter below is true on purpose, even though NEAREST axes cannot have a default column.
        // The test ensures that it does not blow up with a default column set (NCube sets it to false).
        Axis points = new Axis("Point", AxisType.NEAREST, AxisValueType.COMPARABLE, false)
        points.addColumn("Alpha")
        points.addColumn("Bravo")
        points.addColumn("Charlie")
        points.addColumn("Delta")
        points.addColumn("Echo")
        points.addColumn("ABC")
        ncube.addAxis(points)

        def coord = [:]
        coord.put("Point", "Alpha")
        ncube.setCell("alpha", coord)
        coord.put("Point", "Bravo")
        ncube.setCell("bravo", coord)
        coord.put("Point", "Charlie")
        ncube.setCell("charlie", coord)
        coord.put("Point", "Delta")
        ncube.setCell("delta", coord)
        coord.put("Point", "Echo")
        ncube.setCell("echo", coord)
        coord.put("Point", "ABC")
        ncube.setCell("abc", coord)

        coord.put("Point", "alfa")
        assertTrue("alpha".equals(ncube.getCell(coord)))
        coord.put("Point", "Alpha")
        assertTrue("alpha".equals(ncube.getCell(coord)))
        coord.put("Point", "calpa")
        assertTrue("alpha".equals(ncube.getCell(coord)))

        coord.put("Point", "brave")
        assertTrue("bravo".equals(ncube.getCell(coord)))
        coord.put("Point", "ehavo")
        assertTrue("bravo".equals(ncube.getCell(coord)))
        coord.put("Point", "rbavo")
        assertTrue("bravo".equals(ncube.getCell(coord)))

        coord.put("Point", "charpie")
        assertTrue("charlie".equals(ncube.getCell(coord)))
        coord.put("Point", "carpie")
        assertTrue("charlie".equals(ncube.getCell(coord)))
        coord.put("Point", "carlie")
        assertTrue("charlie".equals(ncube.getCell(coord)))

        coord.put("Point", "detla")
        assertTrue("delta".equals(ncube.getCell(coord)))
        coord.put("Point", "desert")
        assertTrue("delta".equals(ncube.getCell(coord)))
        coord.put("Point", "belta")
        assertTrue("delta".equals(ncube.getCell(coord)))

        coord.put("Point", "ecko")
        assertTrue("echo".equals(ncube.getCell(coord)))
        coord.put("Point", "heco")
        assertTrue("echo".equals(ncube.getCell(coord)))
        coord.put("Point", "ehco")
        assertTrue("echo".equals(ncube.getCell(coord)))

        coord.put("Point", "AC")
        assertTrue("abc".equals(ncube.getCell(coord)))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 7)
    }

    @Test
    void testNearestLong()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube3.json')
        def coord = [:]
        coord.put("Code", 1)
        assertTrue("DEF".equals(ncube.getCell(coord)))
        coord.put("Code", (byte) -8)
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", (short) 8)
        assertTrue("GHI".equals(ncube.getCell(coord)))
    }

    @Test
    void testNearestDouble()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube2.json')
        def coord = [:]
        coord.put("Code", 1.0f)
        assertTrue("DEF".equals(ncube.getCell(coord)))
        coord.put("Code", -8.0f)
        assertTrue("ABC".equals(ncube.getCell(coord)))
        coord.put("Code", 8.0)
        assertTrue("GHI".equals(ncube.getCell(coord)))
    }

    @Test
    void testNearestDate()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube1.json')
        def coord = [:]
        Calendar cal = Calendar.instance
        cal.clear()
        cal.set(1984, 6, 9, 2, 2, 2)
        coord.put("Code", cal.time)
        assertTrue("ABC".equals(ncube.getCell(coord)))
        cal.set(2001, 4, 22, 3, 3, 3)
        coord.put("Code", cal.time)
        assertTrue("DEF".equals(ncube.getCell(coord)))
        cal.set(2009, 2, 8, 4, 4, 4)
        coord.put("Code", cal.time)
        assertTrue("GHI".equals(ncube.getCell(coord)))
    }

    @Test
    void testClearCell()
    {
        NCube ncube = new NCube("TestClearCell")
        ncube.defaultCellValue = "DEFAULT VALUE"
        Axis gender = NCubeBuilder.getGenderAxis(true)
        ncube.addAxis(gender)
        def coord = [:]
        coord.put("Gender", "Male")
        ncube.setCell("m", coord)
        coord.put("Gender", "Female")
        ncube.setCell("f", coord)

        assertTrue("f".equals(ncube.getCell(coord)))
        ncube.removeCell(coord)
        assertTrue("DEFAULT VALUE".equals(ncube.getCell(coord)))
    }

    @Test
    void testGetMap()
    {
        NCube ncube = new NCube("TestGetMap")
        ncube.defaultCellValue = "DEFAULT VALUE"
        Axis gender = NCubeBuilder.getGenderAxis(true)
        ncube.addAxis(gender)
        def coord = [:]
        coord.put("Gender", "Male")
        ncube.setCell("m", coord)
        coord.put("Gender", "Female")
        ncube.setCell("f", coord)

        Set set = new HashSet()
        coord.Gender = set
        Map result = ncube.getMap(coord)
        assertTrue("f".equals(result.Female))
        assertTrue("m".equals(result.Male))

        set.clear()
        set.add("Male")
        assertSame(coord.Gender, set)
        result = ncube.getMap(coord)
        assertFalse("f".equals(result.Female))
        assertTrue("m".equals(result.Male))

        set.clear()
        set.add("Snail")
        assertSame(coord.Gender, set)
        result = ncube.getMap(coord)
        assertTrue(result.size() == 1)
        assertTrue("DEFAULT VALUE".equals(result.get(null)))
    }

    @Test
    void testGetMapErrorHandling()
    {
        NCube ncube = new NCube("TestGetMap")
        ncube.defaultCellValue = "DEFAULT VALUE"
        Axis gender = NCubeBuilder.getGenderAxis(true)
        Axis days = NCubeBuilder.shortDaysOfWeekAxis
        ncube.addAxis(gender)
        ncube.addAxis(days)
        def coord = [:]

        try
        {
            ncube.getMap(coord)
            fail()
        }
        catch (InvalidCoordinateException e)
        {
            assertTrue(e.message.contains("required scope"))
            assert ncube.name == e.cubeName
            assert coord.keySet() == e.coordinateKeys
            assert ['Days'] as Set == e.requiredKeys
        }

        try
        {
            coord.put("Gender", new HashSet())
            coord.put("Days", new TreeSet())
            ncube.getMap(coord)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("than"))
            assertTrue(e.message.contains("one"))
            assertTrue(e.message.contains("coord"))
        }
    }

    @Test
    void testGetMapWithRangeColumn()
    {
        NCube ncube = new NCube("TestGetMapWithRange")
        Axis range = NCubeBuilder.getDateRangeAxis(false)
        ncube.addAxis(range)

        Set set = new HashSet()
        Map coord = [dateRange:set] as Map
        Map result = ncube.getMap(coord)
        for (Object o : result.entrySet())
        {
            Map.Entry entry = (Map.Entry) o
            assertTrue(entry.key instanceof Range)
            Range r = (Range) entry.key
            assertTrue(r.low instanceof Date)
        }
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 5)
    }

    @Test
    void testGetMapWithRangeSetColumn()
    {
        NCube ncube = new NCube("TestGetMapWithRangeSet")
        Axis age = new Axis("Age", AxisType.SET, AxisValueType.LONG, false)
        ncube.addAxis(age)
        RangeSet rs = new RangeSet(new Range(60, 80))
        rs.add(10)
        age.addColumn(rs)

        def coord = [:]
        coord.put("age", 10)
        ncube.setCell("young", coord)
        coord.put("age", 60)
        ncube.setCell("old", coord)        // overwrite 'young'

        Set set = new HashSet()
        coord.put("age", set)
        Map result = ncube.getMap(coord)
        Iterator i = result.entrySet().iterator()
        if (i.hasNext())
        {
            Map.Entry entry = (Map.Entry) i.next()
            assertTrue(entry.key instanceof RangeSet)
            rs = (RangeSet) entry.key
            assertTrue(rs.get(0) instanceof Range)
            Range range = (Range) rs.get(0)
            assertEquals(60L, range.low)
            assertEquals(80L, range.high)
            assertEquals("old", entry.value)
            assertEquals(10l, rs.get(1))
        }
        else
        {
            assertTrue("Should have 2 items", false)
        }
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 2)
    }

    @Test
    void test2DSimpleJson()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, '2DSimpleJson.json')
        def coord = [:]
        coord["businessDivisionCode"] = "ALT"
        coord["attribute"] = "workflowAppCode"
        String cellValue = ncube.getCell(coord)
        assertTrue("AMWRKFLW".equals(cellValue))

        coord["businessDivisionCode"] = "FIDCR"
        coord["attribute"] = "longName"
        assertTrue("Fidelity/Crime Division".equals(ncube.getCell(coord)))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 7)
    }

    @Test
    void testContainsCell()
    {
        NCube<Date> ncube = new NCube<Date>("Dates")
        ncube.addAxis(NCubeBuilder.shortMonthsOfYear)

        def coord = [:]
        coord.put("Months", "Jun")
        Date now = new Date()
        ncube.setCell(now, coord)

        assertTrue(ncube.getCell(coord).equals(now))
        assertTrue(ncube.containsCell(coord))

        coord.put("Months", "Jan")
        assertFalse(ncube.containsCell(coord))
        coord.put("Months", "Jul")
        assertFalse(ncube.containsCell(coord))
        coord.put("Months", "Dec")
        assertFalse(ncube.containsCell(coord))
    }

    @Test
    void testEmptyToHtml()
    {
        NCube ncube = new NCube("Empty")
        assertTrue(countMatches(ncube.toHtml(), "<tr") == 0)
    }

    @Test
    void testInternalColumnPointers()
    {
        NCube<String> ncube = new NCube<>("TestColumnPointers")
        ncube.addAxis(NCubeBuilder.getGenderAxis(true))
        Axis triAxis = new Axis("Tristate", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY, 2)
        triAxis.addColumn("true")
        triAxis.addColumn("false")
        ncube.addAxis(triAxis)

        def coord = [:]
        coord.put("Gender", "Male")
        coord.put("TriState", "true")
        ncube.setCell("male-true", coord)

        coord.put("TriState", "false")
        ncube.setCell("male-false", coord)

        coord.put("TriState", null)
        ncube.setCell("male-default", coord)

        coord.put("Gender", "Female")
        coord.put("TriState", "true")
        ncube.setCell("female-true", coord)

        coord.put("TriState", "false")
        ncube.setCell("female-false", coord)

        coord.put("TriState", null)
        ncube.setCell("female-default", coord)

        coord.put("Gender", null)
        coord.put("TriState", "true")
        ncube.setCell("default-true", coord)

        coord.put("TriState", "false")
        ncube.setCell("default-false", coord)

        coord.put("TriState", null)
        ncube.setCell("default-default", coord)

        coord.put("Gender", "Male")
        coord.put("TriState", "true")
        assertTrue("male-true".equals(ncube.getCell(coord)))

        coord.put("TriState", "false")
        assertTrue("male-false".equals(ncube.getCell(coord)))

        coord.put("TriState", null)
        assertTrue("male-default".equals(ncube.getCell(coord)))

        coord.put("Gender", "Female")
        coord.put("TriState", "true")
        assertTrue("female-true".equals(ncube.getCell(coord)))

        coord.put("TriState", "false")
        assertTrue("female-false".equals(ncube.getCell(coord)))

        coord.put("TriState", null)
        assertTrue("female-default".equals(ncube.getCell(coord)))

        coord.put("Gender", null)
        coord.put("TriState", "true")
        assertTrue("default-true".equals(ncube.getCell(coord)))

        coord.put("TriState", "false")
        assertTrue("default-false".equals(ncube.getCell(coord)))

        coord.put("TriState", null)
        assertTrue("default-default".equals(ncube.getCell(coord)))

        assertTrue(countMatches(ncube.toHtml(), "<tr") == 5)
    }

    @Test
    void testStackTrace()
    {
        NCube<CommandCell> continents = new NCube<CommandCell>("Continents")
        Axis continent = NCubeBuilder.continentAxis
        continents.addAxis(continent)

        def coord = [:]
        coord.put("Continent", "Africa")
        continents.setCell(new GroovyExpression("\$AfricaCountries(input)", null, false), coord)
        coord.put("Continent", "Antarctica")
        continents.setCell(new GroovyExpression("\$AntarticaCountries(input)", null, false), coord)
        coord.put("Continent", "Asia")
        continents.setCell(new GroovyExpression("\$AsiaCountries(input)", null, false), coord)
        coord.put("Continent", "Australia")
        continents.setCell(new GroovyExpression("\$AustraliaCountries(input)", null, false), coord)
        coord.put("Continent", "Europe")
        continents.setCell(new GroovyExpression("\$EuropeanCountries(input)", null, false), coord)
        coord.put("Continent", "North America")
        continents.setCell(new GroovyExpression("\$NorthAmericaCountries(input)", null, false), coord)
        coord.put("Continent", "South America")
        continents.setCell(new GroovyExpression("\$SouthAmericaCountries(input)", null, false), coord)

        coord.put("Continent", "North America")
        coord.put("Country", "USA")
        coord.put("State", "OH")

        NCube<CommandCell> naCountries = new NCube<CommandCell>("NorthAmericaCountries")
        Axis country = new Axis("Country", AxisType.DISCRETE, AxisValueType.STRING, false)
        country.addColumn("Canada")
        country.addColumn("USA")
        country.addColumn("Mexico")
        naCountries.addAxis(country)

        naCountries.setCell(new GroovyExpression("\$UsaStates(input)", null, false), coord)
        ncubeRuntime.addCube(continents)
        ncubeRuntime.addCube(naCountries)

        try
        {
            continents.getCell(coord)
            fail("should throw exception")
        }
        catch (RuntimeException e)
        {
            assertContainsIgnoreCase(e.message, 'error occurred', 'NorthAmericaCountries', 'cell:NorthAmericaCountries', 'North America', 'USA', 'OH')
        }
    }

    @Test
    void testStackEntryCoordinateValueAbbreviated()
    {
        String failMessage = "should throw exception"
        String cmdString = "throw new Exception('cell error')"

        String size0 = ''
        String size1 = 'b'
        String size999 = 'b'.multiply(999)
        String size1000 = 'b'.multiply(1000)
        String size1001 = 'b'.multiply(1001)

        NCube<CommandCell> cube = new NCube<CommandCell>("StackEntryTest")
        Axis axis1 = new Axis("axis1", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY)
        cube.addAxis(axis1)
        axis1.addColumn("a")

        Axis axis2 = new Axis("axis2", AxisType.DISCRETE, AxisValueType.STRING, true, Axis.DISPLAY)
        cube.addAxis(axis2)
        axis2.addColumn(size0)
        axis2.addColumn(size1)
        axis2.addColumn(size999)
        axis2.addColumn(size1000)
        axis2.addColumn(size1001)

        def coord = [axis1: 'a']
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)

        coord.axis2 = size1001
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)
        coord.axis2 = size1000
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)
        coord.axis2 = size999
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)
        coord.axis2 = size1
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)
        coord.axis2 = size0
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)
        coord.remove("axis2")
        cube.setCell(new GroovyExpression(cmdString, null, false), coord)

        ncubeRuntime.addCube(cube)

        //Test coordinate value that gets abbreviated - size1001
        coord.axis2 = size1001
        try
        {
            cube.getCell(coord)
            fail(failMessage)
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred in cube: stackentrytest\n-> cell:stackentrytest:[axis1:a,axis2:${size1000}...")
        }

        //Test coordinate value that does not get abbreviated - size1000 (same length as max length)
        coord.axis2 = size1000
        try
        {
            cube.getCell(coord)
            fail(failMessage)
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred in cube: stackentrytest\n-> cell:stackentrytest:[axis1:a,axis2:${size1000}")
        }

        //Test coordinate value that does not get abbreviated - size999
        coord.axis2 = size999
        try
        {
            cube.getCell(coord)
            fail(failMessage)
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred in cube: stackentrytest\n-> cell:stackentrytest:[axis1:a,axis2:${size999}")
        }

        //Test coordinate value that does not get abbreviated - size1
        coord.axis2 = size1
        try
        {
            cube.getCell(coord)
            fail(failMessage)
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred in cube: stackentrytest\n-> cell:stackentrytest:[axis1:a,axis2:${size1}")
        }

        //Test coordinate value that does not get abbreviated - size0
        coord.axis2 = size0
        try
        {
            cube.getCell(coord)
            fail(failMessage)
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred in cube: stackentrytest\n-> cell:stackentrytest:[axis1:a,axis2:")
        }

        //Test coordinate value that does not get abbreviated - column default
        coord.remove("axis2")
        try
        {
            cube.getCell(coord)
            fail(failMessage)
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("error occurred in cube: stackentrytest\n-> cell:stackentrytest:[axis1:a]")
        }
    }

    @Test
    void testRenameAxis()
    {
        NCube<String> ncube = new NCube('RenameAxisTest')
        Axis days = NCubeBuilder.shortDaysOfWeekAxis
        ncube.addAxis(days)

        def coord = [:]
        coord.days = 'Mon'
        ncube.setCell('Monday', coord)
        coord.clear()
        coord.DAYS = 'Wed'
        ncube.setCell('Wednesday', coord)
        coord.clear()
        coord.Days = 'Fri'
        ncube.setCell('Friday', coord)

        ncube.renameAxis('DAYS', 'DAYS-OF-WEEK')

        coord.clear()
        coord['DAYS-OF-WEEK'] = 'Mon'
        assertTrue('Monday'.equals(ncube.getCell(coord)))
        coord.clear()
        coord['DAYS-of-WEEK'] = 'Wed'
        assertTrue('Wednesday'.equals(ncube.getCell(coord)))
        coord.clear()
        coord['DAYS-OF-week'] = 'Fri'
        assertTrue('Friday'.equals(ncube.getCell(coord)))

        try
        {
            ncube.renameAxis(null, 'DAYS-OF-WEEK')
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains('name'))
            assertTrue(e.message.contains('cannot'))
            assertTrue(e.message.contains('empty'))
        }

        try
        {
            ncube.renameAxis('days', null)
            assertTrue('should throw exception', false)
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains('name'))
            assertTrue(e.message.contains('cannot'))
            assertTrue(e.message.contains('empty'))
        }

        try
        {
            ncube.renameAxis('days-OF-week', 'Days-of-week')
            assertTrue('should throw exception', false)
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains('already'))
            assertTrue(e.message.contains('axis'))
            assertTrue(e.message.contains('named'))
        }

        try
        {
            ncube.renameAxis('jojo', 'mojo')
            assertTrue('should throw exception', false)
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains('xis'))
            assertTrue(e.message.contains('not'))
            assertTrue(e.message.contains('on'))
            assertTrue(e.message.contains('cube'))
        }
    }

    @Test(expected=RuntimeException.class)
    void testInvalidTemplate()
    {
        NCube n1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template-with-error.json')
        n1.getCell([State:'TX'] as Map)
    }

    @Test
    void testUpdateColumnValue()
    {
        NCube n1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'updateColumns.json')
        Axis state = n1.getAxis("state")
        Column col = state.findColumn("WY")

        def coord = [:]
        coord.put("code", 1)
        coord.put("state", "WY")
        String val = (String) n1.getCell(coord)
        assertEquals("1 WY", val)

        n1.updateColumn(col.id, "ZZ")

        coord.put("state", "ZZ")
        val = (String) n1.getCell(coord)
        assertEquals("1 WY", val)

        assertNull(n1.getAxisFromColumnId(100))

        try
        {
            // bogus column
            n1.updateColumn(1234567, "zz")
            fail("should not make it here")
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('no column exists')
        }
    }

    @Test
    void testBinaryUrl()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template1.json')
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template2.json')
        NCube n1 = createRuntimeCubeFromResource(ApplicationID.testAppId, 'urlContent.json')
        def coord = [:]
        coord.put("sites", "BinaryFromLocalUrl")
        byte[] localBinaryBytes = (byte[]) n1.getCell(coord)
        assertEquals(77383, localBinaryBytes.length)

        coord.put("sites", "BinaryFromRemoteUrl")
        byte[] remoteBinaryBytes = (byte[]) n1.getCell(coord)
        assertEquals(77383, remoteBinaryBytes.length)

        coord.put("sites", "StringFromLocalUrl")
        assertEquals("CAFEBABE", n1.getCell(coord))

        coord.put("sites", "StringFromValue")
        assertEquals("return \"Local Hello, world.\"", n1.getCell(coord))

        coord.put("sites", "StringFromRemoteUrl")
        assertEquals("CAFEBABE", n1.getCell(coord))

        coord.put("sites", "TemplateFromLocalUrl")
        assertEquals("You saved 0.12 on your plane insurance. Does this 0.12 work?", n1.getCell(coord))

        coord.put("sites", "TemplateFromRemoteUrl")
        assertEquals("You saved 0.12 on your plane insurance. Does this 0.12 work?", n1.getCell(coord))
    }

    @Test
    void testWildcardSet()
    {
        NCube<String> ncube = new NCube("test.WildcardSet")
        Axis attributes = new Axis("attribute", AxisType.DISCRETE, AxisValueType.STRING, false)
        Axis busDivCode = new Axis("businessDivisionCode", AxisType.DISCRETE, AxisValueType.STRING, false)

        busDivCode.addColumn("AGR")
        busDivCode.addColumn("ALT")
        busDivCode.addColumn("EQM")
        busDivCode.addColumn("FIDCR")
        busDivCode.addColumn("PIM")
        busDivCode.addColumn("SHS")

        attributes.addColumn("businessDivisionId")
        attributes.addColumn("longName")
        attributes.addColumn("underwriterLdapGroup")
        attributes.addColumn("assignToLdapGroup")
        attributes.addColumn("workflowAppCode")
        attributes.addColumn("divisionId")

        ncube.addAxis(attributes)
        ncube.addAxis(busDivCode)

        def coord = [:]
        coord.put("attribute", "longName")
        coord.put("businessDivisionCode", new LinkedHashSet())
        Map slice = ncube.getMap(coord)
        assertTrue(slice.size() == 6)

        coord.clear()
        Set wild = new TreeSet()
        wild.add("AGR")
        wild.add("PIM")
        coord.put("attribute", "longName")
        coord.put("businessDivisionCode", wild)
        slice = ncube.getMap(coord)
        assertTrue(slice.size() == 2)

        coord.put("businessDivisionCode", ['BOGUS'] as Set)
        try
        {
            ncube.getMap(coord)
            fail("should not make it here")
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains('not found using set on axis')
            assert ncube.name == e.cubeName
            assert coord == e.coordinate
            assert "businessDivisionCode" == e.axisName
            assert "BOGUS" == e.value
        }

        coord.put("businessDivisionCode", null)
        try
        {
            ncube.getMap(coord)
            fail("should not make it here")
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains("no 'set' value found")
        }

        coord.clear()
        try
        {
            ncube.getMap(coord)
            fail("should not make it here")
        }
        catch (InvalidCoordinateException e)
        {
            assertContainsIgnoreCase(e.message,'missing required scope', 'attribute', 'businessDivisionCode', 'test.WildcardSet')
            assert ncube.name == e.cubeName
            assert coord.keySet() == e.coordinateKeys
            assert ["attribute", "businessDivisionCode"] as Set == e.requiredKeys
        }
    }

    @Test
    void testJsonInJson()
    {
        String jsonInner = "{\n" +
                "    \\\"array\\\": [\n" +
                "        1,\n" +
                "        2,\n" +
                "        3\n" +
                "    ]\n" +
                "}"
        String jsonOuter = "{\n" +
                "   \"ncube\":\"TestCube\",\n" +
                "   \"defaultCellValue\":\"ZZZ\",\n" +
                "   \"axes\":[\n" +
                "      {\n" +
                "         \"name\":\"Code\",\n" +
                "         \"type\":\"NEAREST\",\n" +
                "         \"valueType\":\"DATE\",\n" +
                "         \"hasDefault\":false,\n" +
                "         \"preferredOrder\":0,\n" +
                "         \"columns\":[\n" +
                "            {\n" +
                "               \"value\":\"1990-01-01T02:00:00\"\n" +
                "            },\n" +
                "            {\n" +
                "               \"value\":\"2000-01-01T02:00:00\"\n" +
                "            },\n" +
                "            {\n" +
                "               \"value\":\"2012-01-01T02:00:00\"\n" +
                "            }\n" +
                "         ]\n" +
                "      }\n" +
                "   ],\n" +
                "   \"cells\":[\n" +
                "      {\n" +
                "         \"key\":{\"Code\":\"1993-06-04T02:22:44\"},\n" +
                "         \"value\":\"ABC\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"key\":{\"Code\":\"1998-11-17T11:05:20\"},\n" +
                "         \"value\":\"DEF\"\n" +
                "      },\n" +
                "      {\n" +
                "         \"key\":{\"Code\":\"2014-09-14T16:07:01\"},\n" +
                "         \"value\":\"" + jsonInner + "\"" +
                "      }\n" +
                "   ]\n" +
                "}"
        NCube ncube = NCube.fromSimpleJson(jsonOuter)
        def coord = [:]
        coord.put("code", new Date())
        Object value = ncube.getCell(coord)
        assertTrue(value instanceof String)
    }

    @Test
    void testGroovyCallingNCube()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        axis.addColumn("scalar")
        ncube.addAxis(axis)

        def coord = [:]
        coord.put("type", "good")
        ncube.setCell(new GroovyExpression("output.out='dog'; output['ncube']=ncube; return 'great'", null, false), coord)
        coord.put("type", "bad")
        ncube.setCell(new GroovyExpression("input.type='scalar'; return \$(input)", null, false), coord)
        coord.put("type", "scalar")
        ncube.setCell(16, coord)

        def output = [:]
        coord.put("type", "good")
        Object o = ncube.getCell(coord, output)
        assertEquals("great", o)
        assertEquals(output.get("out"), "dog")
        assertEquals(ncube, output.get("ncube"))  // ncube was passed in

        output.clear()
        coord.put("type", "bad")
        o = ncube.getCell(coord, output)
        assertEquals(16, o)
    }

    @Test
    void testGroovyModifyingInput()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        axis.addColumn("scalar")
        ncube.addAxis(axis)

        Map input = new HashMap()
        input.put("type", "bad")
        ncube.setCell(new GroovyExpression("input['type']='scalar'; output.funny = 'bone'; return 5", null, false), input)

        def output = [:]
        input.put("type", "bad")
        Object ret = ncube.getCell(input, output)
        assertEquals(5, ret)
        assertEquals(input.get("type"), "bad") // input coord does not change
    }

    @Test
    void testGroovyNCubeMgr()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        axis.addColumn("property")
        ncube.addAxis(axis)

        def coord = [:]
        coord.put("type", "good")
        ncube.setCell(new GroovyExpression("\$GroovyCube([type:'property'])", null, false), coord)
        coord.put("type", "bad")
        ncube.setCell(new GroovyExpression("def total = 0; (1..10).each { i -> total += i}; return total", null, false), coord)
        coord.put("type", "property")
        ncube.setCell(new GroovyExpression("9", null, false), coord)

        Map output = [:] as Map
        coord.put("type", "good")
        assertEquals(9, ncube.getCell(coord, output))

        output = new HashMap()
        coord.put("type", "bad")
        assertEquals(55, ncube.getCell(coord, output))

        output = new HashMap()
        coord.put("type", "property")
        assertEquals(9, ncube.getCell(coord, output))
    }

    @Test
    void testGroovyMath()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("age", AxisType.DISCRETE, AxisValueType.LONG, false)
        axis.addColumn(25)
        axis.addColumn(35)
        axis.addColumn(45)
        ncube.addAxis(axis)

        def coord = [:]
        coord.put("age", 25)
        ncube.setCell(new GroovyExpression("def age=input['age']; return Math.abs(age - 100)", null, false), coord)

        def output = [:]
        coord.put("age", 25)
        Object o = ncube.getCell(coord, output)
        assertEquals(o, 75)
    }

    @Test
    void testGroovy()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("age", AxisType.DISCRETE, AxisValueType.LONG, false)
        axis.addColumn(25)
        axis.addColumn(35)
        axis.addColumn(45)
        ncube.addAxis(axis)

        // Bad command (CommandCell not GroovyProg used)
        Map coord = [:] as Map
        coord.age = 25

        // Bad Groovy (Compile error)
        try
        {
            ncube.setCell(new GroovyMethod(
                    "Object run(Map args whoops) " +
                            "{ 1 }", null, false), coord)

            ncube.getCell(coord, new HashMap())
            fail("Should not make it here")
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains('error occurred')
        }

        // Bad Groovy (NCube cmd syntax error)
        try
        {
            ncube.setCell(new GroovyMethod(
                    "def run(Map args whoops) " +
                            "{ 1 }", null, false), coord)

            ncube.getCell(coord, new HashMap())
            fail("Should not make it here")
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains('error occurred')
        }

        // Repeat error...should just throw it again (not attempt to recompile)
        try
        {
            ncube.getCell(coord, new HashMap())
            fail("Should not make it here")
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains('error occurred')
        }

        coord = [:] as Map
        coord.age = 25
        coord.method = "oldify"
        ncube.setCell(new GroovyMethod(
                "import ncube.grv.method.NCubeGroovyController;" +
                        "class Chicken extends NCubeGroovyController" +
                        "{" +
                        "def oldify() " +
                        "{" +
                        " input['age'] * 10;" +
                        "}}", null, false), coord)

        Map output = [:] as Map
        coord.age = 25
        coord.method = "oldify"
        long start = System.currentTimeMillis()
        Object o = null
        for (int i = 0; i < 100000; i++)
        {
            o = ncube.getCell(coord, output)
            assertEquals(o, 250)
        }
        long stop = System.currentTimeMillis()
        println("execute GroovyMethod 100,000 times = " + (stop - start))
        assertEquals(o, 250)
    }

    static class CallJavaTest
    {
        static Object testInput(Map input, Map output, String type)
        {
            if ("good".equalsIgnoreCase(type))
            {
                output.put("out", "dog")
                return "great"
            }
            else
            {
                output.put("out", "cat")
                return "terrible"
            }
        }
    }

    @Test
    void testGroovyExpThatCallsJava()
    {
        NCube ncube = new NCube("CallCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        ncube.addAxis(axis)

        // Illustrates that return is optional in expressions
        def coord = [:]
        coord.put("type", "good")
        String className = TestNCube.class.name
        ncube.setCell(new GroovyExpression(className + "\$CallJavaTest.testInput(input, output, input.type)", null, false), coord)
        coord.put("type", "bad")
        ncube.setCell(new GroovyExpression("return " + className + "\$CallJavaTest.testInput(input, output, input.type)", null, false), coord)

        def output = [:]
        coord.put("type", "good")
        Object o = ncube.getCell(coord, output)
        assertEquals("great", o)
        assertEquals(output.get("out"), "dog")

        coord.put("type", "bad")
        o = ncube.getCell(coord, output)
        assertEquals("terrible", o)
        assertEquals(output.get("out"), "cat")
    }

    @Test
    void testShorthandNotation()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        axis.addColumn("alpha")
        axis.addColumn("beta")
        ncube.addAxis(axis)

        def coord = [:]
        coord.put("type", "good")
        ncube.setCell(new GroovyExpression("\$GroovyCube([type:'alpha'])", null, false), coord)
        coord.put("type", "bad")
        ncube.setCell(new GroovyExpression("\$([type:'beta'])", null, false), coord)
        coord.put("type", "alpha")
        ncube.setCell(16, coord)
        coord.put("type", "beta")
        ncube.setCell(26, coord)

        coord.put("type", "good")
        Object o = ncube.getCell(coord)
        assertEquals(16, o)

        coord.put("type", "bad")
        o = ncube.getCell(coord)
        assertEquals(26, o)
    }

    @Test
    void testShorthandNotationWithOutput()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        axis.addColumn("alpha")
        axis.addColumn("beta")
        ncube.addAxis(axis)

        def coord = [:]
        coord.put("type", "good")
        ncube.setCell(new GroovyExpression("\$GroovyCube([type:'alpha'])", null, false), coord)
        coord.put("type", "bad")
        ncube.setCell(new GroovyExpression("\$([type:'beta'])", null, false), coord)
        coord.put("type", "alpha")
        ncube.setCell(new GroovyExpression("output['stack'] = 'foo'; output.good=16", null, false), coord)
        coord.put("type", "beta")
        ncube.setCell(new GroovyExpression("output.stack = 'foo'; output.bad=26", null, false), coord)

        coord.put("type", "good")
        def output = [:]
        Object o = ncube.getCell(coord, output)
        assertEquals(16, o)
        assertEquals(16, output.get("good"))
        assertEquals(4, output.size())
        String foo = (String) output.get("stack")
        assertEquals("foo", foo)

        coord.put("type", "bad")
        output.clear()
        o = ncube.getCell(coord, output)
        assertEquals(26, o)
        assertEquals(26, output.get("bad"))
    }

    @Test
    void testSupportDeprecatedJoinCommand()
    {
        NCube ncube = new NCube("GroovyCube")
        Axis axis = new Axis("type", AxisType.DISCRETE, AxisValueType.STRING, false)
        axis.addColumn("good")
        axis.addColumn("bad")
        ncube.addAxis(axis)
        ncubeRuntime.addCube(ncube)

        def coord = [:]
        coord.put("type", "good")
        ncube.setCell(new GroovyExpression("@JoinedCube([:])", null, false), coord)
        coord.put("type", "bad")
        ncube.setCell(new GroovyExpression("@JoinedCube([])", null, false), coord)      // Can't pass an array

        NCube cube2 = new NCube("JoinedCube")
        axis = new Axis("state", AxisType.DISCRETE, AxisValueType.LONG.STRING, false)
        axis.addColumn("OH")
        axis.addColumn("TX")
        cube2.addAxis(axis)
        ncubeRuntime.addCube(cube2)

        coord.clear()
        coord.put("type", "good")
        coord.put("state", "OH")
        cube2.setCell("Cincinnati", coord)
        coord.put("state", "TX")
        cube2.setCell("Austin", coord)

        coord.clear()
        coord.put("type", "good")
        coord.put("state", "OH")
        Object o = ncube.getCell(coord)
        assertEquals("Cincinnati", o)

        coord.put("type", "bad")
        coord.put("state", "TX")
        coord.put("state", "TX")
        try
        {
            ncube.getCell(coord)
            fail("Should not get here")
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains('error occurred')
        }

        Set<String> names = ncube.getRequiredScope([:], [:])
        assertTrue(names.size() == 1)
        assertTrue(names.contains("type"))
        names = ncube.getOptionalScope([:], [:])
        assertTrue(names.size() == 0)
    }

    @Test
    void testNullCommand()
    {
        try
        {
            new GroovyMethod(null, null, true)
            fail("Should not make it here.")
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('both')
            assert e.message.toLowerCase().contains('cmd')
            assert e.message.toLowerCase().contains('url')
            assert e.message.toLowerCase().contains('cannot be null')
        }
    }

    @Test
    void testSimpleJsonArray()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonArrayTest.json')
        def coord = [:]
        coord.put("Code", "ints")
        Object[] ints = (Object[]) ncube.getCell(coord)
        assertEquals(ints[0], 0L)
        assertEquals(ints[1], 1)
        assertEquals(ints[2], 4L)

        coord.put("Code", "strings")
        Object[] strings = (Object[]) ncube.getCell(coord)
        assertEquals(strings[0], "alpha")
        assertEquals(strings[1], "bravo")
        assertEquals(strings[2], "charlie")

        coord.put("Code", "arrays")
        Object[] arrays = (Object[]) ncube.getCell(coord)

        Object[] sub1 = (Object[]) arrays[0]
        assertEquals(sub1[0], 0L)
        assertEquals(sub1[1], 1L)
        assertEquals(sub1[2], 6L)

        Object[] sub2 = (Object[]) arrays[1]
        assertEquals(sub2[0], "a")
        assertEquals(sub2[1], "b")
        assertEquals(sub2[2], "c")

        coord.clear()
        coord.put("Code", "crazy")
        arrays = (Object[]) ncube.getCell(coord)

        assertEquals("1.0", arrays[0])
        List sub = (List) arrays[1]
        assertEquals("1.a", sub.get(0))
        sub = (List) arrays[2]
        assertEquals("1.b", sub.get(0))
        assertEquals("2.0", arrays[3])
    }

    @Test
    void testSimpleJsonExpression()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonExpression.json')
        Map coord = [:] as Map
        coord.put("code", "exp")
        Object ans = ncube.getCell(coord)
        assertEquals(6.28d, (double)ans, 0.00001d)
        assertEquals(coord.code, "exp")

        // Type promotion from double to BigDecimal
        coord.CODE = "bigdec"
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof BigDecimal)
        assertTrue(((BigDecimal) ans).doubleValue() > 3.13d)
        assertTrue(((BigDecimal) ans).doubleValue() < 3.15d)

        // Type promotion from double to float
        coord.CODE = "floatVal"
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof Float)
        assertTrue(((Float) ans).doubleValue() > 3.13d)
        assertTrue(((Float) ans).doubleValue() < 3.15d)

        // Type promotion from long to int
        coord.put("CODE", "integerVal")
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof Integer)
        assertEquals(16, ans)

        // Type promotion from long to BigInteger
        coord.put("CODE", "bigintVal")
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof BigInteger)
        assertTrue(((BigInteger) ans).intValue() == -16)

        // Type promotion from long to byte
        coord.put("CODE", "byteVal")
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof Byte)
        assertEquals((byte) 101, ans)

        // Type promotion from long to short
        coord.put("CODE", "shortVal")
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof Short)
        assertEquals((short) -101, ans)

        // Date format (date + time)
        coord.put("CODE", "date1Val")
        ans = ncube.getCell(coord)
        assertTrue(ans instanceof Date)
        Calendar cal = Calendar.instance
        cal.clear()
        cal.time = (Date) ans

        assertEquals(cal.get(Calendar.YEAR), 2013)
        assertEquals(cal.get(Calendar.MONTH), 7)
        assertEquals(cal.get(Calendar.DAY_OF_MONTH), 30)
        assertEquals(cal.get(Calendar.HOUR_OF_DAY), 22)
        assertEquals(cal.get(Calendar.MINUTE), 0)
        assertEquals(cal.get(Calendar.SECOND), 1)

        // Date format (date)
        coord.put("CODE", "date2Val")
        ans = ncube.getCell(coord)
        cal.clear()
        cal.time = (Date) ans
        assertTrue(ans instanceof Date)
        assertEquals(cal.get(Calendar.YEAR), 2013)
        assertEquals(cal.get(Calendar.MONTH), 7)
        assertEquals(cal.get(Calendar.DAY_OF_MONTH), 31)
    }

    @Test(expected=CoordinateNotFoundException.class)
    void testNoColumnsNoCellsNoDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'nocolumns-nocells-nodefault-error.json')
        ncube.getCell([test:'foo'] as Map)
    }

    @Test
    void testNoColumnsNoCellsHasDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'nocolumns-nocells-hasdefault.json')
        assertEquals("bar", ncube.getCell([test:'foo'] as Map))
    }

    @Test
    void testIdInCellDoesNotMatch()
    {
        // Throws no exception because cell is effectively orphaned.  This test exists so that
        // if we start throwing an exception, this test will need to be updated.
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'id-in-cell-does-not-match-columns-error.json')
    }

    @Test
    void testUrlCommandWithoutValueAndUrl()
    {
        // Throws no exception because cell is effectively orphaned.  This test exists so that
        // if we start throwing an exception, this test will need to be updated.
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'url-command-without-value-and-url-error.json')
    }

    @Test
    void testCaseInsensitiveCoordinate()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonArrayTest.json')
        def coord = [:]
        coord.put("c0dE", "ints")
        try
        {
            ncube.getCell(coord)
            fail("Should not make it here")
        }
        catch (InvalidCoordinateException e)
        {
            assert e.message.toLowerCase().contains('required scope')
            assert ncube.name == e.cubeName
            assert coord.keySet() == e.coordinateKeys
            assert ['Code'] as Set == e.requiredKeys
        }
        coord.clear()
        coord.put("codE", "ints")
        assertNotNull(ncube.getCell(coord))
    }

    @Test
    void testAtCommand()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testAtCommand.json')
        Map coord = new CaseInsensitiveMap()
        coord.put("Bu", "PIM")
        coord.put("State", "GA")
        String x = (String) ncube.getCell(coord)
        assertEquals("PIM", coord.Bu)
        assertEquals("GA", coord.State)
        assertEquals("1", x)

        coord.put("state", "OH")
        x = (String) ncube.getCell(coord)
        assertEquals("PIM", coord.Bu)
        assertEquals("OH", coord.State)
        assertEquals("2", x)

        coord.put("STATE", "TX")
        x = (String) ncube.getCell(coord)
        assertEquals("PIM", coord.Bu)
        assertEquals("TX", coord.State)
        assertEquals("3", x)

        coord.put("state", "WY")
        x = (String) ncube.getCell(coord)
        assertEquals("PIM", coord.Bu)
        assertEquals("WY", coord.State)
        assertEquals("4", x)

        coord.put("bu", "EQM")
        x = (String) ncube.getCell(coord)
        assertEquals("EQM", coord.Bu)
        assertEquals("WY", coord.State)
        assertEquals("1", x)

        try {
            NCubeGroovyExpression.legacyNCubeGroovyExpression = true
            x = (String) ncube.getCell(coord)
            assertEquals("EQM", coord.Bu)
            assertEquals("WY", coord.State)
            assertEquals("10", x)
        }
        finally
        {
            NCubeGroovyExpression.legacyNCubeGroovyExpression = false
        }

        Set<String> scope = ncube.getRequiredScope([:], [:])
        assertTrue(scope.size() == 2)
    }

    @Test
    void testOverlappingRangeCubeError()
    {
        try
        {
            ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'idBasedCubeError.json')
            fail("should not get here")
        }
        catch (AxisOverlapException e)
        {
            assert e.message.toLowerCase().contains("range overlap")
        }
    }

    @Test
    void testEqualsMetaPropsMismatch()  throws Exception
    {
        NCube cube1 = NCubeBuilder.getTestNCube2D(false)
        NCube cube2 = NCubeBuilder.getTestNCube2D(false)

        assertEquals(cube1, cube2)

        cube1.setMetaProperty("foo", "bar")
        assertNotEquals(cube1, cube2)
        cube2.setMetaProperty("foo", "bar")
        assertEquals(cube1, cube2)
        cube1.removeMetaProperty("foo")
        assertNotEquals(cube1, cube2)
        cube1.setMetaProperty("foo", "baz")
        assertNotEquals(cube1, cube2)
        cube1.clearMetaProperties()
        cube2.removeMetaProperty("foo")
        assertEquals(cube1, cube2)
    }

    @Test
    void testRemoveCellById()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(true)
        Axis age = ncube.getAxis("age")
        Axis gender = ncube.getAxis("gender")
        Column ageCol = age.columns.get(0)
        long ageCol0 = ageCol.id
        Column genderCol = gender.columns.get(0)
        long genderCol0 = genderCol.id
        assertTrue(ageCol0 != 0)
        assertTrue(genderCol0 != 0)

        Set colIds = new HashSet()
        colIds.add(ageCol0)
        colIds.add(genderCol0)
        ncube.setCellById(1.1d, colIds)

        def coord = [:]
        coord.AGE = ageCol.valueThatMatches
        coord.GENDER = genderCol.valueThatMatches
        Double x = (Double) ncube.getCell(coord)
        assertEquals(x, 1.1d, 0.00001d)

        assertTrue(ncube.containsCellById(colIds))
        ncube.removeCellById(colIds)
        assertFalse(ncube.containsCellById(colIds))
    }

    @Test
    void testReadCubeList()
    {
        List<NCube> ncubes = ncubeRuntime.getNCubesFromResource(ApplicationID.testAppId, 'testCubeList.json')
        assertTrue(ncubes.size() == 2)
        NCube ncube1 = ncubes.get(0)
        assertEquals(ncube1.name, "TestCube")
        NCube ncube2 = ncubes.get(1)
        assertEquals(ncube2.name, "idTest")
    }

    @Test
    void testTemplate()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonExpression.json')
        def coord = [:]
        coord.put("code", "stdTemplate")
        coord.put("overdue", "not overdue")
        String str = (String) ncube.getCell(coord)
        assertEquals("Dear 2, Your balance of 3.14 is not overdue.", str)

        coord.put("code", "stdTemplate2")
        coord.put("overdue", "overdue")
        str = (String) ncube.getCell(coord)
        assertEquals("2, Your balance is overdue 3.14", str)
        str = (String) ncube.getCell(coord)
        assertEquals("2, Your balance is overdue 3.14", str)

        coord.put("code", "stdTemplate3")
        str = (String) ncube.getCell(coord)
        assertEquals("Nothing to replace", str)
        str = (String) ncube.getCell(coord)
        assertEquals("Nothing to replace", str)
    }

    @Test
    void testTemplateRefOtherCube()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template2.json')   // Get it loaded
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template1.json')
        def coord = [:]
        coord.put("state", "GA")
        coord.put("code", 1)
        long start = System.nanoTime()
        String str = (String) ncube.getCell(coord)
        assertEquals("You saved 0.15 on your car insurance. Does this 0.12 work?", str)
        long stop = System.nanoTime()
        //        System.out.println("str = " + str)
        //        System.out.println((stop - start)/1000000)
        coord.put("state", "OH")
        coord.put("code", 1)
        start = System.nanoTime()
        str = (String) ncube.getCell(coord)
        assertEquals("You saved 0.14 on your boat insurance. Does this 0.15 work?", str)
        stop = System.nanoTime()
        //        System.out.println("str = " + str)
        //        System.out.println((stop - start)/1000000)

        coord.put("state", "AL")
        coord.put("code", 1)
        str = (String) ncube.getCell(coord)
        assertEquals("You saved 0.15 on your car insurance. Does this 0.12 work?", str)

        coord.put("state", "AR")
        coord.put("code", 1)
        str = (String) ncube.getCell(coord)
        assertEquals("Dear Bitcoin, please continue your upward growth trajectory.", str)
    }

    @Test
    void testTemplateWithEquivalentCube()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template2-equivalent.json')   // Get it loaded
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template1.json')
        def coord = [:]
        coord.put("state", "GA")
        coord.put("code", 1)
        long start = System.nanoTime()
        String str = (String) ncube.getCell(coord)
        assertEquals("You saved 0.15 on your car insurance. Does this 0.12 work?", str)
        long stop = System.nanoTime()
        //        System.out.println("str = " + str)
        //        System.out.println((stop - start)/1000000)
        coord.put("state", "OH")
        coord.put("code", 1)
        start = System.nanoTime()
        str = (String) ncube.getCell(coord)
        assertEquals("You saved 0.14 on your boat insurance. Does this 0.15 work?", str)
        stop = System.nanoTime()
        //        System.out.println("str = " + str)
        //        System.out.println((stop - start)/1000000)

        coord.put("state", "AL")
        coord.put("code", 1)
        str = (String) ncube.getCell(coord)
        assertEquals("You saved 0.15 on your car insurance. Does this 0.12 work?", str)

        coord.put("state", "AR")
        coord.put("code", 1)
        str = (String) ncube.getCell(coord)
        assertEquals("Dear Bitcoin, please continue your upward growth trajectory.", str)
    }

    @Test
    void testExpressionWithImports()
    {
        NCube<String> ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonExpression.json')
        def coord = [:]
        coord.put("code", "expWithImport")
        String str = ncube.getCell(coord)
        assertEquals(str, "I love Bitcoin")
    }

    @Test
    void testTemplateRequiredScope()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'stringIds.json')
        NCube<String> ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonExpression.json')
        Set<String> scope = ncube.getRequiredScope([:], [:])
        assertEquals(1, scope.size())
        assertTrue(scope.contains("CODe"))

        scope = ncube.getOptionalScope([:], [:])
        assertEquals(0, scope.size())

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template2.json')   // Get it loaded
        ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'template1.json')
        scope = ncube.getRequiredScope([:], [:])
        assertEquals(2, scope.size())
        assertTrue(scope.contains("coDe"))
        assertTrue(scope.contains("staTe"))
    }

    @Test
    void testStringIds()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'stringIds.json')

        def coord = [:]
        coord.put("age",15)
        coord.put("state", "CA")
        assertEquals("young CA", ncube.getCell(coord))
        coord.put("age",18)
        coord.put("state", "OH")
        assertEquals("adult OH", ncube.getCell(coord))
        coord.put("age",60)
        coord.put("state", "TX")
        assertEquals("old TX", ncube.getCell(coord))
        coord.put("age",99)
        coord.put("state", "TX")
        assertEquals("def TX", ncube.getCell(coord))
    }

    @Test
    void testEmptyCube()
    {
        NCube ncube = new NCube("Empty")
        assertNotNull(ncube.toHtml())  // Ensure it does not blow up with exception on completely empty n-cube.
    }

    @Test
    void testDuplicateEqualsAndHashCode()
    {
        simpleJsonCompare("2DSimpleJson.json")
        simpleJsonCompare("big5D.json")
        simpleJsonCompare("expressionAxis.json")
        simpleJsonCompare("expressionAxis2.json")
        simpleJsonCompare("idBasedCube.json")
        simpleJsonCompare("simpleJsonArrayTest.json")
        simpleJsonCompare("simpleJsonExpression.json")
        simpleJsonCompare("stringIds.json")
        simpleJsonCompare("template1.json")
        simpleJsonCompare("template2.json")
        simpleJsonCompare("testAtCommand.json")
        simpleJsonCompare("testCube1.json")
        simpleJsonCompare("testCube2.json")
        simpleJsonCompare("testCube3.json")
        simpleJsonCompare("testCube4.json")
        simpleJsonCompare("testCube5.json")
        simpleJsonCompare("testCube6.json")
        simpleJsonCompare("urlContent.json")
    }

    @Test
    void testIdNoValue()
    {
        NCube<String> ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'idNoValue.json')
        def coord = [:]
        coord.put("age", 18)
        coord.put("state", "OH")
        String s = ncube.getCell(coord)
        assertEquals("18 OH", s)

        coord.put("age", 19)
        coord.put("state", "TX")
        s = ncube.getCell(coord)
        assertEquals("TX", s)
    }

    @Test
    void testUpdateColumns()
    {
        NCube<String> ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'updateColumns.json')
        assertEquals(30, ncube.cellMap.size())

        // Delete 1st, middle, and last column
        Map<Object, Long> valueToId = new HashMap<>()
        Axis code = ncube.getAxis("code")
        for (Column column : code.columns)
        {
            valueToId.put(column.value, column.id)
        }
        Axis axisDto = new Axis("code", AxisType.DISCRETE, AxisValueType.LONG, true)
        axisDto.addColumn(2)
        axisDto.addColumn(4)
        List<Column> cols = axisDto.columns
        for (Column column : cols)
        {
            long id = valueToId.get(column.value)
            column.id = id
        }
        // 1,3,5 deleted
        ncube.updateColumns(axisDto.name, axisDto.columns)
        assertEquals(15, ncube.cellMap.size())

        // Delete 1st, middle, last on state axis
        code = ncube.getAxis("state")
        for (Column column : code.columns)
        {
            valueToId.put(column.value, column.id)
        }
        axisDto = new Axis("state", AxisType.DISCRETE, AxisValueType.STRING, true)
        axisDto.addColumn("CA")
        axisDto.addColumn("TX")
        cols = axisDto.columns
        for (Column column : cols)
        {
            long id = valueToId.get(column.value)
            column.id = id
        }

        ncube.updateColumns(axisDto.name, axisDto.columns)
        assertEquals(6, ncube.cellMap.size())

        ncube.deleteColumn("code", null)
        assertEquals(4, ncube.cellMap.size())

        try
        {
            ncube.updateColumns('code', null)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('cannot pass in null')
        }

        try
        {
            Axis fake = new Axis("fake", AxisType.DISCRETE, AxisValueType.DOUBLE, false)
            ncube.updateColumns(fake.name, fake.columns)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('no axis exists with the name')
        }
    }

    @Test
    void testShortHandReferences()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'stringIds.json')
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'simpleJsonExpression.json')
        Map coord = [:] as Map
        coord.put("code", "FixedExp")
        assertEquals(6.28d, (double) ncube.getCell(coord), 0.00001d)

        coord.put("code", "FixedExtExp")
        assertEquals("young OH", ncube.getCell(coord))

        coord.put("code", "RelativeExp")
        assertEquals(32, ncube.getCell(coord))

        coord.put("code", "RelativeExtExp")
        assertEquals("adult TX", ncube.getCell(coord))
    }

    @Test(expected=RuntimeException.class)
    void testGroovyExpressionThatDoesntExist()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testExpressionAxisUrl.json')
        def coord = [:]
        coord.put("code", "exp")
        assertEquals(6.28d, ncube.getCell(coord))
    }

    @Test(expected=RuntimeException.class)
    void testNullCube()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'null-error.json')
    }

    @Test
    void testGroovyMethods()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testGroovyMethods1.json')
        def coord = [:]
        coord.put("method", "foo")
        coord.put("state", "OH")
        assertEquals(2, ncube.getCell(coord))

        coord.put("method", "bar")
        assertEquals(4, ncube.getCell(coord))

        coord.put("method", "baz")
        assertEquals(8, ncube.getCell(coord))

        coord.put("method", "qux")
        assertEquals(16, ncube.getCell(coord))

        coord.put("method", "foo")
        coord.put("state", "TX")
        assertEquals(3, ncube.getCell(coord))

        coord.put("method", "bar")
        assertEquals(9, ncube.getCell(coord))

        coord.put("method", "baz")
        assertEquals(27, ncube.getCell(coord))

        coord.put("method", "qux")
        assertEquals(81, ncube.getCell(coord))

        coord.put("method", "foo")
        coord.put("state", "OH")
        assertEquals(2, ncube.getCell(coord))

        coord.put("method", "bar")
        assertEquals(4, ncube.getCell(coord))

        coord.put("method", "baz")
        assertEquals(8, ncube.getCell(coord))

        coord.put("method", "qux")
        assertEquals(16, ncube.getCell(coord))
    }

    @Test(expected = RuntimeException.class)
    void testCommandCellReferencedCubeNotFoundOnExpandUrl()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'expand-url-cube-not-found-error.json')
        Map<String, Object> map = new HashMap<String,Object>()
        map.put("Sites", "StringFromLocalUrl")
        ncube.getCell(map)
    }

    @Test(expected = IllegalArgumentException.class)
    void testInvalidArgumentsToConstructor()
    {
        new GroovyTemplate(null, null, false)
    }

    @Test(expected = IllegalArgumentException.class)
    void testInvalidArgumentsToConstructor2()
    {
        new GroovyMethod(null, null, false)
    }

    @Test(expected = IllegalArgumentException.class)
    void testInvalidColumn()
    {
        new GroovyTemplate(null, null, false)
    }

    @Test
    void testContainsCellValue()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'containsCell.json')

        def coord = [:]
        coord.put("Gender", "Male")
        assertTrue(ncube.containsCell(coord, true))
        coord.put("gender", "Female")
        assertTrue(ncube.containsCell(coord))

        coord.put("gender", "GI Joe")
        try
        {
            ncube.containsCell(coord)
            fail("should not make it here")
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains('not found on axis')
            assert ncube.name == e.cubeName
            assert e.coordinate.containsKey("gender")
            assert e.coordinate.get("gender") == "GI Joe"
            assert "Gender" == e.axisName
            assert "GI Joe" == e.value
        }

        ncube.defaultCellValue = null

        coord.put("gender", "Male")
        assertFalse(ncube.containsCell(coord))
        coord.put("gender", "Female")
        assertTrue(ncube.containsCell(coord))

        coord.put("gender", "GI Joe")
        try
        {
            ncube.containsCell(coord)
            fail("should not make it here")
        }
        catch (CoordinateNotFoundException e)
        {
            assert e.message.toLowerCase().contains('not found on axis')
        }
    }

    @Test
    void testMetaPropsRead()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'containsCell.json')
        assertTrue(ncube.metaProperties.size() > 0)
        assertEquals("y", ncube.metaProperties.get("x"))

        Axis axis = ncube.getAxis("gender")
        assertTrue(axis.metaProperties.size() > 0)
        assertEquals(2L, axis.metaProperties.get("feet"))

        Column col = axis.findColumn("Female")
        assertNotNull(col)
        assertTrue(col.metaProperties.size() > 0)
        assertEquals("Jane", col.metaProperties.get("Name"))  // intentional mismatch on case
        assertEquals(36L, col.metaProperties.get("age"))

        String json = ncube.toFormattedJson()
        ncube = NCube.fromSimpleJson(json)
        assertTrue(ncube.metaProperties.size() > 0)
        assertEquals("y", ncube.metaProperties.get("x"))

        axis = ncube.getAxis("gender")
        assertTrue(axis.metaProperties.size() > 0)
        assertEquals(2L, axis.metaProperties.get("feet"))

        col = axis.findColumn("Female")
        assertNotNull(col)
        assertTrue(col.metaProperties.size() > 0)
        assertEquals("Jane", col.metaProperties.get("Name"))  // intentional mismatch on case
        assertEquals(36L, col.metaProperties.get("age"))
    }

    @Test
    void testMetaPropAPIs()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'containsCell.json')

        Axis axis = ncube.getAxis("gender")
        assertTrue(axis.metaProperties.size() > 0)
        assertEquals(2L, axis.metaProperties.feet)

        Column col = axis.findColumn("Female")
        assertNotNull(col)

        ncube.setMetaProperty("language", "groovy")
        axis.setMetaProperty("car", "cruze")
        col.setMetaProperty("one", 1)

        assertEquals(2, ncube.metaProperties.size())
        assertEquals(2, axis.metaProperties.size())
        assertEquals(3, col.metaProperties.size())

        Map metaProps = new HashMap()
        metaProps.sport = "football"
        metaProps.currency = "Bitcoin"

        ncube.addMetaProperties(metaProps)
        axis.addMetaProperties(metaProps)
        col.addMetaProperties(metaProps)
        ncube.clearSha1()

        String json = ncube.toFormattedJson()
        ncube = NCube.fromSimpleJson(json)
        axis = ncube.getAxis("gender")
        col = axis.findColumn("Female")

        //  removed sha1 above, so no sha1 in cube
        assertEquals(4, ncube.metaProperties.size())
        ncube.sha1()
        assertEquals(4, ncube.metaProperties.size())
        assertEquals(4, axis.metaProperties.size())
        assertEquals(5, col.metaProperties.size())

        ncube.clearMetaProperties()
        axis.clearMetaProperties()
        col.clearMetaProperties()

        assertEquals(0, ncube.metaProperties.size())
        assertEquals(0, axis.metaProperties.size())
        assertEquals(0, col.metaProperties.size())

        json = ncube.toFormattedJson()
        ncube = NCube.fromSimpleJson(json)
        axis = ncube.getAxis("gender")
        col = axis.findColumn("Female")

        assertEquals(0, ncube.metaProperties.size())
        assertEquals(0, axis.metaProperties.size())
        assertEquals(0, col.metaProperties.size())
    }

    @Test
    void testHtmlCubeTitle()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'debugExp.json')
        String html = ncube.toHtml()
        assertNotNull(html)
//        System.out.println("html = " + html)
    }

    @Test
    void testHtml2DCubeTitle()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'debugExp2D.json')
        String html = ncube.toHtml()
        assertNotNull(html)
//        System.out.println("html = " + html)
    }

    /**
     * Must set the URL to the path containing the test groovy code.  Do not include
     * the com/... in the path.  This is a RESOURCE URL location, which points to the
     * root of a resource hierarchy, in this case, the resources are groovy source code.
     *
     * This test is a 'spot' test, and should not be run with all the other tests. It should
     * be ignored by default, unless you are testing the single-step debugging
     * capabilities of n-cube Groovy.
     */
    @Test
    void testDebugExpression()
    {
        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'sys.classpath.local.resources.json')

        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'debugExp.json')
        Map coord = [:] as Map
        int age = 9
        coord.age = age
        assertEquals(Math.pow(age, 2), (double) ncube.getCell(coord), 0.00001d)
    }

    @Test
    void testCreateNCubeFromBytesWithException()
    {
        byte[] bytes = new byte[2]
        bytes[0] = (byte)0x1F
        bytes[1] = (byte)0x8b
        try
        {
            NCube.createCubeFromBytes(bytes)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(EOFException.class, e.cause.class)
            assert e.message.toLowerCase().contains("error reading cube from stream")
        }
    }

    @Test
    void testObjectToMapWithNull()
    {
        try
        {
            NCube.objectToMap(null)
            fail("should not make it here")
        }
        catch(IllegalArgumentException e)
        {
            assertTrue(e.message.contains("convert null into a Map"))
            assertTrue(e.message.contains("null passed"))
        }
    }

    static class DTO
    {
        private Date when = new Date()
        String fname = "Albert"
        String lname = "Einstein"
    }

    @Test
    void testObjectToMap()
    {
        DTO instance = new DTO()
        Map coord = NCube.objectToMap(instance)

        // test case-insensitivity
        assertEquals("Albert", coord.get("FName"))
        assertEquals("Einstein", coord.get("LName"))
        assertEquals("Albert", coord.get("fname"))
        assertEquals("Einstein", coord.get("lname"))
        assertEquals(instance.when, coord.get("when"))
    }

    static class ParentDto
    {
        String fname = "Foo"
    }

    static class ChildDto extends ParentDto
    {
        private Date when = new Date()
        String fname = "Albert"
        String lname = "Einstein"
    }

    @Test
    void testObjectToMapWithConflictingFieldNameInParent()
    {
        ChildDto instance = new ChildDto()
        Map coord = NCube.objectToMap(instance)
        assertTrue(coord.containsKey('com.cedarsoftware.ncube.TestNCube$ParentDto.fname'))
        assertEquals("Albert", coord.get("FName"))
        assertEquals("Foo",coord.get('com.cedarsoftware.ncube.TestNCube$ParentDto.fname'))
        assertEquals("Einstein", coord.get("LName"))
        assertEquals(instance.when, coord.get("when"))
    }

    @Test
    void testContainsCellWithDefault()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'containsCellNoDefault.json')

        Map input = new HashMap()
        input.put("gender", "Female")
        assertTrue(ncube.containsCell(input, true))
        input.put("gender", "Male")
        assertFalse(ncube.containsCell(input, true))
    }

    @Test
    void testMetaProps()
    {
        NCube ncube = new NCube("dude")
        assertNull(ncube.removeMetaProperty("test"))
        ncube.setMetaProperty("test", true)
        assertTrue((Boolean) ncube.metaProperties.get("test"))
        assertEquals(1, ncube.metaProperties.size())

        Map<String, Object> metaProps = [:]
        metaProps.put("foo", "bar")
        ncube.addMetaProperties(metaProps)
        assertTrue((Boolean) ncube.metaProperties.get("test"))
        assertEquals("bar", ncube.metaProperties.get("foo"))
        assertEquals(2, ncube.metaProperties.size())

        ncube = new NCube("dude")
        ncube.addMetaProperties(metaProps)
        assertEquals("bar", ncube.metaProperties.get("foo"))
        assertEquals(1, ncube.metaProperties.size())
    }

    @Test
    void testGetLong()
    {
        def map = [:]
        map.put("food", 'w')
        try
        {
            NCube.getLong(map, "food")
            fail("should not make it here")
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('expected')
            assert e.message.toLowerCase().contains('but instead found')
        }
    }

    @Test
    void testGetBoolean()
    {
        Map<String, Integer> map = [food:null] as Map
        assertFalse(NCube.getBoolean(map, "food"))

        try
        {
            map['food'] = 9
            NCube.getBoolean(map, "food")
            fail("should not make it here")
        }
        catch (Exception e)
        {
            assert e.message.toLowerCase().contains('expected')
            assert e.message.toLowerCase().contains('but instead found')
        }
    }

    @Test
    void testRequiredScope()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'requiredScopeKeys.json')
        Set<String> scope = ncube.getRequiredScope([:], [:])
        assertEquals(3, scope.size())
        assertTrue(scope.contains("codE"))
        assertTrue(scope.contains("bU"))
        assertTrue(scope.contains("sTaTe"))

        Axis axis = ncube.getAxis("codE")
        Object b = ncube.extractMetaPropertyValue(axis.getMetaProperty("extraByte"))
        assertTrue(b instanceof Byte)
        assertEquals((byte)8, b)

        List<Column> columns = axis.columns
        assertEquals(3, columns.size())
        Column col1 = columns.get(0)
        Map map = (Map) ncube.extractMetaPropertyValue(col1.getMetaProperty("colProp"))
        assertEquals(1, map.size())
        assertTrue(map.containsKey("dude"))
        assertEquals("male", map.get("dude"))

        Column col2 = columns.get(1)
        map = (Map) ncube.extractMetaPropertyValue(col2.getMetaProperty("colProp"))
        assertEquals(1, map.size())
        assertTrue(map.containsKey("chick"))
        assertEquals("female", map.get("chick"))

        Column col3 = columns.get(2)
        map = (Map) ncube.extractMetaPropertyValue(col3.getMetaProperty("colProp"))
        assertEquals(1, map.size())
        assertTrue(map.containsKey("42"))
        assertEquals("meaning of life", map.get("42"))
    }

    @Test
    void testRequiredScopeSetCell()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'requiredScopeKeys.json')

        Map input = new HashMap()
        input.put("code", 0)
        ncube.setCell("123", input)
        assertTrue(ncube.containsCell(input))

        try
        {
            ncube.getCell(input)
            fail("Should not make it here")
        }
        catch (Exception e)
        {
            assertContainsIgnoreCase(e.message, 'code', 'missing required scope', 'state', 'bu')
        }

        input.put("bU", "blah")
        input.put("StAtE", "blah")
        Object value = ncube.getCell(input)
        assertEquals("123", value)

        input.put("code", -10)
        value = ncube.getCell(input)
        assertEquals("ABC", value)

        input.put("code", 10)
        value = ncube.getCell(input)
        assertEquals("GHI", value)

        input.clear()
        input.put("code", 0)
        ncube.removeCell(input)
        assertFalse(ncube.containsCell(input))
        assertTrue(ncube.containsCell(input, true))

        input.put("bU", "blah")
        input.put("StAtE", "blah")
        value = ncube.getCell(input)
        assertEquals("f", value)   // The default n-cube value (cell no longer exists)
    }

    @Test
    void testNoRequiredScope()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'noRequiredScope.json')
        Set<String> scope = ncube.getRequiredScope([:], [:])
        assertEquals(0, scope.size())

        Object value = ncube.getCell(new HashMap())
        assertEquals("XYZ", value)
    }

    @Test
    void testDuplicate()
    {
        NCube c1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId.asBranch('branch'), 'testCube6.json')
        NCube c2 = c1.duplicate('TestCube')
        assert c1.applicationID == c2.applicationID
    }

    @Test
    void testCubeEquals()
    {
        NCube c1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testCube6.json')
        NCube c2 = c1.duplicate("TestCube")
        String sha1_a = c1.sha1()
        String sha1_b = c2.sha1()
        assertEquals(c1, c2)
        assertEquals(sha1_a, sha1_b)

        c2 = c1.duplicate("Joe")
        assertNotEquals(c1, c2)
        assertNotEquals(c1, "not a cube")
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)

        Axis a = NCubeBuilder.statesAxis
        c2 = c1.duplicate("TestCube")
        c2.addAxis(a)
        assertNotEquals(c1, c2)
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)

        c2 = c1.duplicate("TestCube")
        a = c2.getAxis("gender")
        c2.deleteAxis("gender")
        a.name = 'foo'
        c2.addAxis(a)
        assertNotEquals(c1, c2)
        assertNotEquals(c2, c1)
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)

        c2 = c1.duplicate("TestCube")
        a = c2.getAxis("gender")
        a.columnOrder = Axis.DISPLAY
        assertNotEquals(c1, c2)
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)

        c2 = c1.duplicate("TestCube")
        c2.clearCells()
        assertNotEquals(c1, c2)
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)

        c2 = c1.duplicate("TestCube")
        Map input = new HashMap()
        input.put("gender", "Female")
        c2.setCell(9, input)
        assertNotEquals(c1, c2)
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)

        c2 = c1.duplicate("TestCube")
        c2.defaultCellValue = null
        assertNotEquals(c1, c2)
        assertNotEquals(c2, c1)
        sha1_a = c1.sha1()
        sha1_b = c2.sha1()
        assertNotEquals(sha1_a, sha1_b)
    }

    @Test
    void testDuplicateMetaProperties()
    {
        NCube c1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'testDuplicate.json')
        NCube c2 = c1.duplicate("DupeTest")
        assertTrue(c2.metaProperties.containsKey("money"))
        assertEquals(100.0d, (double) c2.metaProperties.get("money"), 0.00001d)
        assertTrue(c1.metaProperties.equals(c2.metaProperties))

        Axis gender = (Axis) c1.axes.get(0)
        Axis sex = (Axis) c2.axes.get(0)
        assertTrue(gender.metaProperties.size() == 1)
        assertTrue(gender.metaProperties.equals(sex.metaProperties))
        assertEquals("gender", sex.getMetaProperty("sex"))

        Column female = sex.findColumn("Female")
        assertTrue((Boolean)female.getMetaProperty("chick"))
        assertFalse((Boolean)female.getMetaProperty("dude"))

        Column male = sex.findColumn("Male")
        assertFalse((Boolean) male.getMetaProperty("chick"))
        assertTrue((Boolean) male.getMetaProperty("dude"))
    }

    @Test
    void testAbsoluteHttpUrlToGroovy()
    {
        NCube cube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'urlContent.json')
        def coord = [:]
        coord.put("sites", "AbsoluteHttpUrl")
        String s = (String) cube.getCell(coord)
        assertEquals("Hello, world.", s)
    }

    @Test
    void testValidateCubeNames()
    {
        NCube.validateCubeName("This-is.legal_but-hard_to.read")
        try
        {
            NCube.validateCubeName("This:is.not/legal#and-hard_to|read")
            fail("should not make it here")
        }
        catch (IllegalArgumentException ignored)
        { }
        try
        {
            NCube.validateCubeName(" NotValid")
            fail("should not make it here")
        }
        catch (IllegalArgumentException ignored)
        { }
    }

    @Test
    void testValidateCubeName()
    {
        NCube.validateCubeName("Joe")
        NCube.validateCubeName("Joe.Dirt")
        NCube.validateCubeName(NCube.validCubeNameChars)
        try
        {
            NCube.validateCubeName("")
            fail("should not make it here")
        }
        catch (Exception ignored)
        { }

        try
        {
            NCube.validateCubeName(null)
            fail("should not make it here")
        }
        catch (Exception ignored)
        { }
    }

    @Test
    void testToJson()
    {
        assertEquals("null", NCube.toJson(null))
    }

    @Test
    void testNCubeApplicationIdParts()
    {
        ApplicationID appId = new ApplicationID("foo", "bar", "0.0.1", ApplicationID.DEFAULT_STATUS, ApplicationID.TEST_BRANCH)
        NCube ncube = NCubeBuilder.testNCube3D_Boolean
        ncube.applicationID = appId
        assertEquals(appId.status, ncube.status)
        assertEquals(appId.version, ncube.version)
    }

    @Test
    void testDeltaDescriptionCellValue()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube, cube2)
        assertEquals(0, delta.size())

        def coord = [:]
        coord.put("gender", "male")
        coord.put("age", 48)
        cube2.setCell(2, coord)
        delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        String delta0 = delta.get(0).toString().toLowerCase()
        assertTrue(delta0.contains("change cell"))
        assertTrue(delta0.contains("gender"))
        assertTrue(delta0.contains("male"))
        assertTrue(delta0.contains("age"))
        assertTrue(delta0.contains("30"))
        assertTrue(delta0.contains("from"))
        assertTrue(delta0.contains("1"))
        assertTrue(delta0.contains("to"))
        assertTrue(delta0.contains("2"))

        coord.put("gender", "male")
        coord.put("age", 84)
        cube2.setCell(3.1, coord)
        delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(2, delta.size())
        String delta1 = delta.get(1).toString().toLowerCase()
        assertTrue(delta1.contains("change cell"))
        assertTrue(delta1.contains("gender"))
        assertTrue(delta1.contains("male"))
        assertTrue(delta1.contains("age"))
        assertTrue(delta1.contains("default col"))
        assertTrue(delta1.contains("from"))
        assertTrue(delta1.contains("1"))
        assertTrue(delta1.contains("to"))
        assertTrue(delta1.contains("3.1"))
    }

    @Test
    void testDeltaDescriptionCubeName()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.name = "funkey"
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        String delta0 = delta.get(0).toString().toLowerCase()
        assertTrue(delta0.contains("name change"))
        assertTrue(delta0.contains("from"))
        assertTrue(delta0.contains("delta"))
        assertTrue(delta0.contains("to"))
        assertTrue(delta0.contains("funkey"))
    }

    @Test
    void testDeltaDescriptionCubeMetaProp()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.setMetaProperty("foo", "bar")
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        assertTrue(delta.get(0).toString().toLowerCase().contains("meta"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("property"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("foo: bar"))
    }

    @Test
    void testDeltaDescriptionDimMismatchAdded()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.addAxis(NCubeBuilder.statesAxis)
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("state"))
    }

    @Test
    void testDeltaDescriptionAxisPropDiff()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        Axis age = cube2.getAxis("age")
        age.columnOrder = Axis.SORTED
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        assertTrue(delta.get(0).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("prop"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("change"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("sorted"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("unsorted"))
    }

    @Test
    void testDeltaDescriptionAxisMetaPropDiff()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        Axis age = cube2.getAxis("age")
        age.setMetaProperty("foo", 18)
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        assertTrue(delta.get(0).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("age"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("meta"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("property"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("foo: 18"))
    }

    @Test
    void testDeltaDescriptionDimMismatchRemoved()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.deleteAxis("gender")
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())
        assertTrue(delta.get(0).toString().toLowerCase().contains("remove"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("gender"))
    }

    @Test
    void testDeltaDescriptionDimMismatchAddRemove()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.deleteAxis("gender")
        cube2.addAxis(NCubeBuilder.statesAxis)
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(2, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("state"))

        assertTrue(delta.get(1).toString().toLowerCase().contains("remove"))
        assertTrue(delta.get(1).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(1).toString().toLowerCase().contains("gender"))
    }

    @Test
    void testDeltaDescriptionColumnAdded()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.addColumn("age", new Range(55, 70))
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("column"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("55"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("70"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
    }

    @Test
    void testDeltaDescriptionColumnDeleted()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.deleteColumn("gender", "male")
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("column"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("male"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("remove"))
    }

    @Test
    void testDeltaDescriptionColumnMetaProp()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        Axis age = cube2.getAxis("age")
        Column column = age.findColumn(48)
        column.setMetaProperty("baz", "qux")
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("column"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("[30 - 55)"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("meta"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("property"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("baz: qux"))
    }

    @Test
    void testDeltaDescriptionCellAdded()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        def coord = [:]
        coord.put("age", 48)
        coord.put("gender", "male")
        cube.removeCell(coord)
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("cell"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("add"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("male"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("30"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("value: 1"))
    }

    @Test
    void testDeltaDescriptionCellCleared()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        def coord = [:]
        coord.put("age", 48)
        coord.put("gender", "male")
        cube.removeCell(coord)
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube, cube2)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("cell"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("remove"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("gender"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("male"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("age"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("30"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("value"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("1"))
    }

    @Test
    void testDeltaDescriptionColumChanged()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        Axis axis = cube.getAxis("gender")
        Column col = axis.findColumn("male")
        cube.updateColumn(col.id, "mule")

        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube, cube2)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("column"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("change"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("male"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("mule"))
    }

    @Test
    void testDeltaDescriptionAxisDeletedAndColumChanged()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        Axis axis = cube.getAxis("gender")
        Column col = axis.findColumn("male")
        cube.updateColumn(col.id, "mule")

        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        cube2.deleteAxis("agE")
        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(2, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("remove"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("axis"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("age"))

        assertTrue(delta.get(1).toString().toLowerCase().contains("column"))
        assertTrue(delta.get(1).toString().toLowerCase().contains("change"))
        assertTrue(delta.get(1).toString().toLowerCase().contains("male"))
        assertTrue(delta.get(1).toString().toLowerCase().contains("mule"))
    }

    @Test
    void testSha1SwitchingDataBetweenSameValueColumnsOnMultipleAxes()
    {
        String cellVal = 'test'
        Axis axis1 = new Axis('axis1', AxisType.DISCRETE, AxisValueType.STRING, false)
        Axis axis2 = new Axis('axis2', AxisType.DISCRETE, AxisValueType.STRING, false)
        axis1.addColumn('a')
        axis1.addColumn('b')
        axis2.addColumn('a')
        axis2.addColumn('b')

        NCube cube = new NCube('testsha1')
        cube.applicationID = ApplicationID.testAppId
        cube.addAxis(axis1)
        cube.addAxis(axis2)
        cube.setCell(cellVal, [axis1:'a', axis2:'b'])
        String oldSha1 = cube.sha1()

        cube.clearCells()
        cube.setCell(cellVal, [axis1:'b', axis2:'a'])
        String newSha1 = cube.sha1()

        assert oldSha1 != newSha1
    }

    @Test
    void testSha1SwitchingDataBetweenSameValueColumnsOnMultipleRuleAxes()
    {
        String cellVal = 'test'
        Axis axis1 = new Axis('axis1', AxisType.RULE, AxisValueType.STRING, false)
        Axis axis2 = new Axis('axis2', AxisType.RULE, AxisValueType.STRING, false)
        axis1.addColumn('a', 'a')
        axis1.addColumn('a', 'b')
        axis2.addColumn('a', 'a')
        axis2.addColumn('a', 'b')

        NCube cube = new NCube('testsha1')
        cube.applicationID = ApplicationID.testAppId
        cube.addAxis(axis1)
        cube.addAxis(axis2)
        cube.setCell(cellVal, [axis1:'a', axis2:'b'])
        String oldSha1 = cube.sha1()

        cube.clearCells()
        cube.setCell(cellVal, [axis1:'b', axis2:'a'])
        String newSha1 = cube.sha1()

        assert oldSha1 != newSha1
    }

    @Test
    void testSha1CollectionCell()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        String sha1 = cube.sha1()
        def coord = [:]
        coord.put("age", 48)
        coord.put("gender", "male")
        cube.setCell(coord.values(), coord)
        assertNotEquals(sha1, cube.sha1())
    }

    @Test
    void testSha1CycleBreaker()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'delta.json')
        String sha1 = cube.sha1()

        // Create cycle
        Object[] stuff = new Object[1]
        List things = new ArrayList()
        things.add(stuff)
        stuff[0] = things

        // Stuff cyclic cell contents
        def coord = [:]
        coord.put("age", 48)
        coord.put("gender", "male")
        cube.setCell(things, coord)

        // Ensure we do not lock up here.
        assertNotEquals(sha1, cube.sha1())
    }

    @Test
    void testRuleDelta()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'deltaRule.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'deltaRule.json')

        def coord = [:]
        coord.put("rule", "Init Random")
        cube2.setCell("bogus", coord)

        List<Delta> delta = DeltaProcessor.getDeltaDescription(cube2, cube)
        assertEquals(1, delta.size())

        assertTrue(delta.get(0).toString().toLowerCase().contains("cell"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("change"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("rule"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("init random"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("input.random"))
        assertTrue(delta.get(0).toString().toLowerCase().contains("bogus"))
    }

    @Test
    void testMetaCompare()
    {
        def oldMeta = new CaseInsensitiveMap()
        def newMeta = new CaseInsensitiveMap()

        oldMeta.put("foo", "foot")
        oldMeta.put("bar", "bart")
        oldMeta.put("baz", "bazinga")
        oldMeta.put("alpha", "bravo")
        newMeta.put("foo", "fool")
        newMeta.put("bar", "barf")
        newMeta.put("qux", "quxqux")
        newMeta.put("alpha", "bravo")
        List<Delta> changes = DeltaProcessor.compareMetaProperties(oldMeta, newMeta, Delta.Location.AXIS, "Axis 'state'", null)
        String s = changes.toString()
        assertTrue(s.contains("Add Axis 'state' meta-property {qux: quxqux}"))
        assertTrue(s.contains("Delete Axis 'state' meta-property {baz: bazinga}"))
        assertTrue(s.contains("Change Axis 'state' meta-property {foo: foot} ==> {foo: fool}"))
        assertTrue(s.contains("foo: fool"))
        assertTrue(s.contains("bar: bart"))
        assertTrue(s.contains("bar: barf"))
    }

    @Test
    void testMetaCompareAdd()
    {
        def oldMeta = new CaseInsensitiveMap()
        def newMeta = new CaseInsensitiveMap()

        newMeta.put("foo", "foot")
        newMeta.put("bar", "bart")
        List<Delta> changes = DeltaProcessor.compareMetaProperties(oldMeta, newMeta, Delta.Location.AXIS, "Axis 'state'", null)
        String s = changes.toString()
        assertTrue(s.contains("Add Axis 'state' meta-property {foo: foot}"))
        assertTrue(s.contains("Add Axis 'state' meta-property {bar: bart}"))
    }

    @Test
    void testMetaCompareChanged()
    {
        def oldMeta = new CaseInsensitiveMap()
        def newMeta = new CaseInsensitiveMap()

        oldMeta.put("foo", "foot")
        oldMeta.put("bar", "bart")
        newMeta.put("foo", "fool")
        newMeta.put("bar", "barf")
        List<Delta> changes = DeltaProcessor.compareMetaProperties(oldMeta, newMeta, Delta.Location.AXIS, "Axis 'state'", null)
        String s = changes.toString()
        assertTrue(s.contains("Change Axis 'state' meta-property {foo: foot} ==> {foo: fool}"))
        assertTrue(s.contains("Change Axis 'state' meta-property {bar: bart} ==> {bar: barf}"))
    }

    @Test
    void testMetaCompareDelete()
    {
        def oldMeta = new CaseInsensitiveMap()
        def newMeta = new CaseInsensitiveMap()

        oldMeta.put("foo", "foot")
        oldMeta.put("bar", "bart")
        List<Delta> changes = DeltaProcessor.compareMetaProperties(oldMeta, newMeta, Delta.Location.AXIS, "Axis 'state'", null)
        String s = changes.toString()
        assertTrue(s.contains("Delete Axis 'state' meta-property {foo: foot}"))
        assertTrue(s.contains("Delete Axis 'state' meta-property {bar: bart}"))
    }

    @Test
    void testGetPopulatedCells()
    {
        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'urlPieces.json')
        assert cube.populatedCellCoordinates.size() == 2

        cube = new NCube('Hey')
        Axis one = new Axis('one', AxisType.DISCRETE, AxisValueType.STRING, false)
        one.addColumn('1')
        one.addColumn('2')
        one.addColumn('3')
        one.addColumn('4')

        Axis two = new Axis('two', AxisType.DISCRETE, AxisValueType.STRING, false)
        two.addColumn('a')
        two.addColumn('b')
        two.addColumn('c')
        two.addColumn('d')
        two.addColumn('e')

        cube.addAxis(one)
        cube.addAxis(two)

        assert cube.populatedCellCoordinates.size() == 0

        cube.setCell('hi', [one:1, two:'a'] as Map)
        List cells = cube.populatedCellCoordinates
        assert cells.size() == 1
        Map coord = cells[0]
        assert coord.one == '1'
        assert coord.two == 'a'
        cube.setCell('hey', [one:2, two:'a'] as Map)
        cells = cube.populatedCellCoordinates
        assert cells.size() == 2
    }

    @Test
    void testMergeDiffDimensions()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'debugExp.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'debugExp2D.json')
        assert null == DeltaProcessor.getDelta(cube1, cube2)
    }

    @Test
    void testMergeDiffAxisNames()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, '2DSimpleJson.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'debugExp2D.json')
        assert null == DeltaProcessor.getDelta(cube1, cube2)
    }

    @Test
    void testMergeSameCube()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, '2DSimpleJson.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, '2DSimpleJson.json')
        assert cube1.sha1() == cube2.sha1()
        Map delta = DeltaProcessor.getDelta(cube1, cube2)
        DeltaProcessor.mergeDeltaSet(cube1, delta)
        assert cube1.sha1() == cube2.sha1()
    }

    @Test
    void testMergeOtherWithContentIntoEmpty()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'empty2D.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge1.json')
        Map cubeDelta = DeltaProcessor.getDelta(cube1, cube2)
        Map delta = (Map) cubeDelta[DeltaProcessor.DELTA_CELLS]
        DeltaProcessor.mergeDeltaSet(cube1, cubeDelta)
        assert delta.size() == 5
        Map coord = [row:1, column:'A'] as Map
        assert "1" == cube1.getCell(coord)

        coord = [row:2, column:'B'] as Map
        assert 2 == cube1.getCell(coord)

        coord = [row:3, column:'C'] as Map
        assert 3.14 == cube1.getCell(coord)

        coord = [row:4, column:'D'] as Map
        assert 6.28 == cube1.getCell(coord)

        coord = [row:5, column:'E'] as Map
        assert cube1.containsCell(coord)

        assert cube1.numCells == 5
    }

    @Test
    void testMergeEmptyIntoContent()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge1.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'empty2D.json')
        Map cubeDelta = DeltaProcessor.getDelta(cube1, cube2)
        Map delta = (Map) cubeDelta[DeltaProcessor.DELTA_CELLS]
        assert delta.size() == 5
        DeltaProcessor.mergeDeltaSet(cube1, cubeDelta)
        assert cube1.numCells == 0
    }

    @Test
    void testMergeConflictCellOverlap()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge1.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge2.json')
        String cube1Sha = cube1.sha1()
        String cube2Sha = cube2.sha1()
        Map cubeDelta1 = DeltaProcessor.getDelta(cube1, cube2)
        Map delta1 = (Map) cubeDelta1[DeltaProcessor.DELTA_CELLS]
        assert delta1.size() == 1
        assert delta1.values().iterator().next() == 3.14159
        Map cubeDelta2 = DeltaProcessor.getDelta(cube2, cube1)
        Map delta2 = (Map) cubeDelta2[DeltaProcessor.DELTA_CELLS]
        assert delta2.size() == 1
        assert delta2.values().iterator().next() == 3.14
        assert cube1.sha1() == cube1Sha
        assert cube2.sha1() == cube2Sha
    }

    @Test
    void testMergeNormal()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge1.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge2.json')
        Map coord = [row:3, column:'C'] as Map
        cube1.removeCell(coord)
        Map delta = DeltaProcessor.getDelta(cube1, cube2)
        DeltaProcessor.mergeDeltaSet(cube1, delta)
        Object v = cube1.getCell(coord)
        assert v == 3.14159
    }

    @Test
    void testMergeNormalCaseEquivalency()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge2.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge2.json')
        Map coord = [row:3, column:'C'] as Map
        cube1.removeCell(coord)
        Map delta = DeltaProcessor.getDelta(cube1, cube2)
        DeltaProcessor.mergeDeltaSet(cube1, delta)
        Object v = cube1.getCell(coord)
        assert cube1.sha1() == cube2.sha1()
        assert v == 3.14159
    }

    @Test
    void testCellChangeSetCompatibility()
    {
        NCube cube1 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge1.json')
        NCube cube2 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge2.json')
        NCube cube3 = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'merge3.json')

        Map changeSet1 = DeltaProcessor.getDelta(cube2, cube1)
        Map changeSet2 = DeltaProcessor.getDelta(cube2, cube3)
        assertFalse DeltaProcessor.areDeltaSetsCompatible(changeSet1, changeSet2)
        assertFalse DeltaProcessor.areDeltaSetsCompatible(changeSet2, changeSet1)

        changeSet1 = DeltaProcessor.getDelta(cube1, cube2)
        changeSet2 = DeltaProcessor.getDelta(cube3, cube2)
        assert DeltaProcessor.areDeltaSetsCompatible(changeSet1, changeSet2)
        assert DeltaProcessor.areDeltaSetsCompatible(changeSet2, changeSet1)
    }

    @Test
    void testFromSimpleJsonNullHandling()
    {
        try
        {
            NCube.fromSimpleJson((InputStream)null)
            fail()
        }
        catch (Exception ignored)
        {
        }

        try
        {
            NCube.fromSimpleJson((String)null)
            fail()
        }
        catch (Exception ignored)
        {
        }
    }

    @Test
    void testRemoveCoordWithBadCoord()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(false)
        assert null == ncube.removeCellById(new HashSet())
    }

    @Test
    void testContainsCoordWithBadCoord()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(false)
        assert !ncube.containsCellById(null)
    }

    @Test
    void testSetCellByIdWithBadCoord()
    {
        NCube ncube = NCubeBuilder.getTestNCube2D(false)
        try
        {
            ncube.setCellById(1.0d, null)
            fail()
        }
        catch (InvalidCoordinateException e)
        {
            assertTrue(e.message.contains("Unable to setCellById"))
            assertEquals(ncube.name, e.cubeName)
            assertNull(e.coordinateKeys)
            assertNull(e.requiredKeys)
        }
    }

    @Test
    void testGetReferencedCubeNamesOnRuleCubeWithDefault()
    {
        NCube ncube = NCubeBuilder.ruleWithOutboundRefs
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 2

        Map coord = [rule: "${'(availability): @test.available[:]'}"] as CaseInsensitiveMap
        assert ['test.available'] as Set == refs[coord]

        coord = [:] as CaseInsensitiveMap
        assert ['test.price'] as Set == refs[coord]
    }

    @Test
    void testGetReferencedCubeNamesOnCubeWithMultipleRefCubesPerCoord()
    {
        NCube ncube = NCubeBuilder.cubeWithMultipleRefCubesPerCoord
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 2

        Map coord = [Axis1: 'Axis1Col1', Axis2: 'Axis2Col1'] as CaseInsensitiveMap
        assert ['CubeA', 'CubeB'] as Set == refs[coord]

        coord = [Axis1: 'Axis1Col2', Axis2: 'Axis2Col2'] as CaseInsensitiveMap
        assert ['CubeA', 'CubeB', 'CubeC'] as Set == refs[coord]
    }

    @Test
    void testGetReferencedCubeNamesOnCubeWithColumnDefault()
    {
        NCube ncube = NCubeBuilder.cubeWithColumnDefault
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 2

        Map coord = [Axis1: 'Axis1Col1', Axis2: 'Axis2Col1'] as CaseInsensitiveMap
        assert ['CubeA'] as Set == refs[coord]

        coord = [Axis1: 'Axis1Col2'] as CaseInsensitiveMap
        assert ['Axis1Col2Default'] as Set == refs[coord]
      }

    @Test
    void testGetReferencedCubeNamesOnCubeWithCubeDefault()
    {
        NCube ncube = NCubeBuilder.cubeWithCubeDefault
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 2

        Map coord = [Axis1: 'Axis1Col2', Axis2: 'Axis2Col2'] as CaseInsensitiveMap
        assert ['CubeB'] as Set == refs[coord]

        coord = [:] as CaseInsensitiveMap
        assert ['CubeA'] as Set == refs[coord]
    }

    @Test
    void testGetReferencedCubeNamesOnCubeWithDefaultColumns()
    {
        NCube ncube = NCubeBuilder.cubeWithDefaultColumns
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 4

        Map coord = [Axis1: 'Axis1Col1', Axis2: 'Axis2Col1'] as CaseInsensitiveMap
        assert ['CubeA'] as Set == refs[coord]

        coord = [Axis1: 'default column', Axis2: 'Axis2Col2'] as CaseInsensitiveMap
        assert ['CubeB'] as Set == refs[coord]

        coord = [Axis1: 'Axis1Col2', Axis2: 'default column'] as CaseInsensitiveMap
        assert ['CubeC'] as Set == refs[coord]

        coord = [Axis1: 'default column', Axis2: 'default column'] as CaseInsensitiveMap
        assert ['CubeD'] as Set == refs[coord]
    }

    @Test
    void testGetReferencedCubeNamesOnCubeWithAllDefaults()
    {
        NCube ncube = NCubeBuilder.cubeWithAllDefaults
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 7

        Map coord = [Axis1: 'Axis1Col1', Axis2: 'Axis2Col1'] as CaseInsensitiveMap
        assert ['CubeA'] as Set == refs[coord]

        coord = [Axis1: 'default column', Axis2: 'Axis2Col2'] as CaseInsensitiveMap
        assert ['CubeB'] as Set == refs[coord]

        coord = [Axis1: 'Axis1Col2', Axis2: 'default column'] as CaseInsensitiveMap
        assert ['CubeC'] as Set == refs[coord]

        coord = [Axis1: 'default column', Axis2: 'default column'] as CaseInsensitiveMap
        assert ['CubeD'] as Set == refs[coord]

        coord = [Axis1: 'Axis1Col3'] as CaseInsensitiveMap
        assert ['Axis1Col3Default'] as Set == refs[coord]

        coord = [Axis2: 'Axis2Col3'] as CaseInsensitiveMap
        assert ['Axis2Col3Default'] as Set == refs[coord]

        coord = [:] as CaseInsensitiveMap
        assert ['CubeLevelDefault'] as Set == refs[coord]
    }

    @Test
    void testGetReferencedCubeNamesOnRuleCubeWithAllDefaults()
    {
        NCube ncube = NCubeBuilder.ruleCubeWithAllDefaults
        Map<Map, Set<String>> refs = ncube.referencedCubeNames
        assert refs.size() == 6

        Map coord = [Axis2: 'Axis2Col1', RuleAxis1: "${'(Condition1): @Condition1[:]'}"] as CaseInsensitiveMap
        assert ['CubeB'] as Set == refs[coord]

        coord = [Axis2: 'Axis2Col2',  RuleAxis1: "${'(Condition2): true'}"] as CaseInsensitiveMap
        assert ['CubeC'] as Set == refs[coord]

        coord = [Axis2: 'Axis2Col2'] as CaseInsensitiveMap
        assert ['Axis2Col2Default'] as Set == refs[coord]

        coord = [RuleAxis1: "${'(Condition1): @Condition1[:]'}"] as CaseInsensitiveMap
        assert ['Condition1'] as Set == refs[coord]

        coord = [RuleAxis1: "${'(Condition3): true'}"] as CaseInsensitiveMap
        assert ['Condition3ColumnDefault'] as Set == refs[coord]

        coord = [:] as CaseInsensitiveMap
        assert ['CubeLevelDefault'] as Set == refs[coord]
    }

    @Test
    void testHtmlFormatter()
    {
        NCube ncube5 = NCubeBuilder.get5DTestCube()
        assert ncube5.toHtml() != null
    }

    @Test
    void testPotentialCells()
    {
        NCube ncube = NCubeBuilder.get5DTestCube()
        assert 48L == ncube.numPotentialCells

        ncube = NCubeBuilder.discrete1D
        assert 2L == ncube.numPotentialCells

        ncube = NCubeBuilder.testNCube3D_Boolean
        assert 144L == ncube.numPotentialCells

        ncube = NCubeBuilder.trackingTestCube
        assert 4L == ncube.numPotentialCells
    }

    @Test
    void testAt()
    {
        NCube ncube = NCubeBuilder.discrete1DEmptyWithDefault
        ncube.setCell(1, [state:'OH'])
        ncube.setCell(2, [state:'TX'])

        assert 1 == ncube.at([state:'OH'])
        assert 2 == ncube.at([state: 'TX'], [:])
        assert '999' == ncube.at([state: 'AZ'], [:], '999')
        ncube.setCell(888, [state:'HI'])
        assert 888 == ncube.at([state: 'AZ'], [:], '999')
    }

    @Test
    @TypeChecked(TypeCheckingMode.SKIP)
    void testNullCell()
    {
        NCube ncube = NCubeBuilder.discrete1D
        ncube.clearCells()
        Map input = [state:'OH']
        ncube.setCell(null, input)
        assert null == ncube.at(input)
        assert ncube.numCells == 1
        input.state = 'TX'
        ncube.setCell('null', input)
        assert ncube.numCells == 2
        assert ncube.toHtml() != null
    }

    @Test
    void testVersionComparator()
    {
        Set sorted = new TreeSet(new VersionComparator())
        sorted.add('1.0.1')
        sorted.add('1.0.2')
        sorted.add('1.0.10')
        sorted.add('1.0.11')
        sorted.add('1.0.12')
        sorted.add('1.0.20')
        sorted.add('1.1.0')
        sorted.add('1.11.0')
        sorted.add('1.111.0')
        sorted.add('1.2.0')
        sorted.add('1.20.0')
        sorted.add('2.9.88')
        sorted.add('11.999.8888')
        sorted.add('12.999.8888')

        sorted.toString() == '[12.999.8888, 11.999.8888, 2.9.88, 1.111.0, 1.20.0, 1.11.0, 1.2.0, 1.1.0, 1.0.20, 1.0.12, 1.0.11, 1.0.10, 1.0.2, 1.0.1]'
    }

    @Test
    void testFindColumn()
    {
        NCube ncube = NCubeBuilder.discrete1D
        Column col = ncube.findColumn('city', 'Dallas')
        assert null == col
        col = ncube.findColumn('state', 'OH')
        assert 1000000000001L == col.id
        assert 'OH' == col.value
        // See TestAxis.groovy for complete findColumn() test cases
    }

    @Test
    void testOpenInvalidRuleNCube()
    {
        try
        {
            NCube rule = NCubeBuilder.simpleAutoBadRule
            Axis axis = rule.getAxis('Smoker')
            assert axis.findColumnByName('br1')
            assert axis.findColumnByName('Credit-Score')

            axis = rule.getAxis('Obesity')
            assert axis.findColumnByName('br1')
            assert axis.findColumnByName('BR2')

            NCubeBuilder.duplicateRule
            axis = rule.getAxis('Smoker')
            assert axis.findColumnByName('credit-score')
            assert axis.findColumnByName('BR1')
        }
        catch (IllegalArgumentException ignored)
        {
            fail('NCube RULE axis without name causing error.')
        }
    }

    @Test
    void testMapReduceWithBasicQuery()
    {
        Map output = [:]
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'NY'}, [output:output])

        assert queryResult.size() == 2

        RuleInfo ruleInfo = (RuleInfo) output.get(NCube.RULE_EXEC_INFO)
        Set inputKeysUsed = ruleInfo.getInputKeysUsed()
        assert inputKeysUsed.size() == 3
        assert inputKeysUsed.contains('key')
        assert inputKeysUsed.contains('query')
        assert inputKeysUsed.contains('cubeName')

        Map row = queryResult['B'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == 'a string long enough to test contains'

        row = queryResult['F'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == null
    }

    @Test
    void testMapReduceWithBasicQueryAndInput()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == input.fubar}, [input:[fubar: 'NY']])

        assert queryResult.size() == 2

        Map row = queryResult['B'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == 'a string long enough to test contains'

        row = queryResult['F'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == null
    }

    @Test
    void testMapReduceWithComplexQuery()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'IN' || (input.bar instanceof Number && (input.bar as int )< 50)})

        assert queryResult.size() == 2

        Map row = queryResult['C'] as Map
        assert row['foo'] == 'IN'
        assert row['bar'] == 'something random'

        row = queryResult['D'] as Map
        assert row['foo'] == 'KY'
        assert row['bar'] == 33
    }

    @Test
    void testMapReduceWithLargeCube()
    {
        long start, stop
        Map queryResult
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'mapReduceLargeCube.json')

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] })
        stop = System.nanoTime()
        println("MapReduce on large cube with no axes on input = " + ((stop - start) / 1000000))
        assert 14 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'AllOther']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage on input = " + ((stop - start) / 1000000))
        assert 5 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[sourceRisk: 'Building']])
        stop = System.nanoTime()
        println("MapReduce on large cube with sourceRisk on input = " + ((stop - start) / 1000000))
        assert 13 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[risk: 'PremisesOperations']])
        stop = System.nanoTime()
        println("MapReduce on large cube with risk on input = " + ((stop - start) / 1000000))
        assert 4 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[field: 'RateFactors']])
        stop = System.nanoTime()
        println("MapReduce on large cube with field on input = " + ((stop - start) / 1000000))
        assert 2 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'CommercialGeneralLiabilityCoverage', sourceRisk: 'Building']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage, sourceRisk on input = " + ((stop - start) / 1000000))
        assert 3 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'CommercialGeneralLiabilityCoverage', risk: 'PremisesOperations']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage, risk on input = " + ((stop - start) / 1000000))
        assert 4 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'CommercialGeneralLiabilityCoverage', field: 'coverages']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage, field on input = " + ((stop - start) / 1000000))
        assert 1 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[sourceRisk: 'Building', risk: 'ProductsCompletedOperations']])
        stop = System.nanoTime()
        println("MapReduce on large cube with sourceRisk, risk on input = " + ((stop - start) / 1000000))
        assert 10 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[sourceRisk: 'Building', field: 'Rates']])
        stop = System.nanoTime()
        println("MapReduce on large cube with sourceRisk, field on input = " + ((stop - start) / 1000000))
        assert 2 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[risk: 'ProductsCompletedOperations', field: 'Rates']])
        stop = System.nanoTime()
        println("MapReduce on large cube with risk, field on input = " + ((stop - start) / 1000000))
        assert 2 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'CommercialGeneralLiabilityCoverage', sourceRisk: 'CGLOperations', risk: 'PremisesOperations']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage, sourceRisk, risk on input = " + ((stop - start) / 1000000))
        assert 1 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'CommercialGeneralLiabilityCoverage', sourceRisk: 'CGLOperations', field: 'Limits']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage, sourceRisk, field on input = " + ((stop - start) / 1000000))
        assert 0 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[coverage: 'CommercialGeneralLiabilityCoverage', risk: 'PremisesOperations', field: 'coverages']])
        stop = System.nanoTime()
        println("MapReduce on large cube with coverage, risk, field on input = " + ((stop - start) / 1000000))
        assert 1 == queryResult.size()

        start = System.nanoTime()
        queryResult = ncube.mapReduce('trait', { Map input -> input['r:exists'] }, [input:[sourceRisk: 'Building', risk: 'PremisesOperations', field: 'deductibles']])
        stop = System.nanoTime()
        println("MapReduce on large cube with sourceRisk, risk, field on input = " + ((stop - start) / 1000000))
        assert 1 == queryResult.size()
    }

    @Test
    void testMapReduceWithContains()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.bar?.toString()?.contains('test contains')})

        assert queryResult.size() == 2

        Map row = queryResult['B'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == 'a string long enough to test contains'

        row = queryResult['E'] as Map
        assert row['foo'] == 'TX'
        assert row['bar'] == 'also test contains'
    }

    @Test
    void testMapReduceWithMultipleConditions()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'NY' && (input.bar as String)?.contains('a string long enough')})

        assert queryResult.size() == 1

        Map row = queryResult['B'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == 'a string long enough to test contains'
    }

    @Test
    void testMapReduceLookingForEmptyValue()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> !input.bar })

        assert queryResult.size() == 1

        Map row = queryResult['F'] as Map
        assert row['foo'] == 'NY'
        assert row['bar'] == null
    }

    @Test
    void testMapReduceFindAllRows()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query')

        assert queryResult.size() == 8
        assert queryResult.keySet().containsAll(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'])
    }

    @Test
    void testMapReduceFindSingleRowSpecified()
    {
        Map output = [:]
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:'B'],output:output])

        RuleInfo ruleInfo = (RuleInfo) output.get(NCube.RULE_EXEC_INFO)
        Set inputKeysUsed = ruleInfo.getInputKeysUsed()
        assert inputKeysUsed.size() == 2
        assert inputKeysUsed.contains('key')
        assert inputKeysUsed.contains('query')

        assert queryResult.size() == 1
        assert queryResult.keySet().containsAll(['B'])
    }

    @Test
    void testMapReduceFindMultipleRowsSpecified()
    {
        Map output = [:]
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:['B','C','D']],output:output])

        RuleInfo ruleInfo = (RuleInfo) output.get(NCube.RULE_EXEC_INFO)
        Set inputKeysUsed = ruleInfo.getInputKeysUsed()
        assert inputKeysUsed.size() == 2
        assert inputKeysUsed.contains('key')
        assert inputKeysUsed.contains('query')

        assert queryResult.size() == 3
        assert queryResult.keySet().containsAll(['B','C','D'])
    }

    @Test
    void testMapReduceFindNullRowSpecified()
    {
        Map output = [:]
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:null],output:output])

        RuleInfo ruleInfo = (RuleInfo) output.get(NCube.RULE_EXEC_INFO)
        Set inputKeysUsed = ruleInfo.getInputKeysUsed()
        assert inputKeysUsed.size() == 3
        assert inputKeysUsed.contains('key')
        assert inputKeysUsed.contains('query')
        assert inputKeysUsed.contains('cubeName')

        assert queryResult.size() == 8
        assert queryResult.keySet().containsAll(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'])
    }

    @Test
    void testMapReduceFindDuplicateRowsSpecified()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:['C','C','C']]])

        assert queryResult.size() == 1
        assert queryResult.keySet().containsAll(['C'])
    }

    @Test
    void testMapReduceFindNoMatchingRowsSpecified()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:['NonExistentKey']]])
        assert queryResult.isEmpty()
    }

    @Test
    void testMapReduceFindNoMatchingRowsInSetSpecified()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:['NonExistentKey','AlsoNotFound']]])
        assert queryResult.isEmpty()
    }

    @Test
    void testMapReduceFindSomeMatchingRowsInSetSpecified()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { true }, [input:[key:['NonExistentKey','C']]])
        assert queryResult.size() == 1
        assert queryResult.keySet().containsAll(['C'])
    }

    @Test
    void testMapReduceWithLinkedCubeRequiredScope()
    {
        NCube cubeFrom = createRuntimeCubeFromResource(ApplicationID.testAppId, 'MapReduceLinkedFrom.json')
        createRuntimeCubeFromResource(ApplicationID.testAppId, 'MapReduceLinkedTo.json')
        Axis axis = cubeFrom.getAxis('one')
        String axisName = axis.name
        String colName = axis.columns.first().value
        Closure where = { Map input -> input.get(colName) != null }

        Map map = cubeFrom.mapReduce(axisName, where, [input:[three:colName]] as Map)
        assert 1 == map.size()
        assert map.containsKey(colName)
        assert ((Map)map.get(colName)).containsValue(cubeFrom.getCell([one:colName, two:colName, three:colName]))
    }

    @Test
    void testMapReduceWithoutLinkedCubeRequiredScope()
    {
        NCube cubeFrom = createRuntimeCubeFromResource(ApplicationID.testAppId, 'MapReduceLinkedFrom.json')
        createRuntimeCubeFromResource(ApplicationID.testAppId, 'MapReduceLinkedTo.json')
        Axis axis = cubeFrom.getAxis('one')
        String axisName = axis.name
        String colName = axis.columns.first().value
        Closure where = { Map input -> input.get(colName) != null }

        Map map = cubeFrom.mapReduce(axisName, where)
        assert 1 == map.size()
        assert map.containsKey(colName)
        assertContainsIgnoreCase(((Map)map.get(colName)).get(colName) as String, 'two', 'one','missing required scope', 'three')
    }

    @Test
    void testMapReduceWithoutLinkedCubeRequiredScopeNoExecute()
    {
        NCube cubeFrom = createRuntimeCubeFromResource(ApplicationID.testAppId, 'MapReduceLinkedFrom.json')
        createRuntimeCubeFromResource(ApplicationID.testAppId, 'MapReduceLinkedTo.json')
        Axis axis = cubeFrom.getAxis('one')
        String axisName = axis.name
        String colName = axis.columns.first().value
        Closure where = { Map input -> input.get(colName) != null }

        Map map = cubeFrom.mapReduce(axisName, where, [(NCube.MAP_REDUCE_SHOULD_EXECUTE):false])
        assert 1 == map.size()
        assert map.containsKey(colName)
        assert ((Map)map.get(colName)).containsValue(cubeFrom.getCellNoExecute([one:colName, two:colName]))
    }

    @Test
    void testMapReduceCaseSensitive3D() {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQuery3DTest.json')
        Map queryResult = ncube.mapReduce('Val', {Map input -> input['Val1'] != null || input['val1'] != null || input[null] != null})
        assert queryResult.size() == 3
        assert queryResult.containsKey([Row:1L, Column:'A'])
        assert queryResult.containsKey([Row:1L, Column:'B'])
        assert queryResult.containsKey([Row:2L, Column:'B'])
    }

    @Test
    void testMapReduceFindCaseSensitiveRows()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryCaseSensitive.json')

        Map queryResult = ncube.mapReduce('key', { true })

        assert queryResult.size() == 2

        Map row = queryResult['STATE'] as Map
        assert row['A'] == 'OH'
        assert row['B'] == 'KY'

        row = queryResult['State'] as Map
        assert row['A'] == 'Ohio'
        assert row['B'] == 'Kentucky'
    }

    @Test
    void testMapReduceFindCaseSensitiveRowsSpecified()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryCaseSensitive.json')

        Map queryResult = ncube.mapReduce('key', { true }, [input:[query:'STATE']])

        assert queryResult.size() == 1

        Map row = queryResult['STATE'] as Map
        assert row['A'] == 'OH'
        assert row['B'] == 'KY'
    }

    @Test
    void testMapReduceFindCaseSensitiveCols()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryCaseSensitive.json')

        Map queryResult = ncube.mapReduce('query', { true })

        assert queryResult.size() == 2

        Map row = queryResult['A'] as Map
        assert row['STATE'] == 'OH'
        assert row['State'] == 'Ohio'

        row = queryResult['B'] as Map
        assert row['STATE'] == 'KY'
        assert row['State'] == 'Kentucky'
    }

    @Test
    void testMapReduceWithCommandCell()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'OH' })

        assert queryResult.size() == 1

        Map row = queryResult['G'] as Map
        assert row['foo'] == 'OH'
        assert row['bar'] == '5 G - bar'
    }

    @Test
    void testMapReduceWithOtherDimensions()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryMultiDimTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'OH'}, [input:[bind: 'bindToAValue']])

        assert queryResult.size() == 1

        Map row = queryResult['A'] as Map
        assert row['foo'] == 'OH'
        assert row['bar'] == 'Ohio'
    }

    @Test
    void testMapReduceWithOtherDimensionsNotSet()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryMultiDimTestWithDefCol.json')
        Set set = new HashSet()
        Map queryResult = ncube.mapReduce('query', { Map input, Map map ->
            set.add(map.key)
            input.foo == 'def-A-foo'
        }, [:])

        assert queryResult.size() == 1
        assert set.contains('A')
        assert set.contains('B')
        assert set.size() == 2
    }

    @Test
    void testMapReduceWithOtherDimensionsInvalidCoordinate()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryMultiDimTest.json')

        try
        {
            ncube.mapReduce('query', { Map input -> input.foo == 'OH'}, [input:[bind: 'invalid']])
            fail 'Should have thrown a CoordinateNotFoundException'
        }
        catch (CoordinateNotFoundException e)
        {
            assertContainsIgnoreCase(e.message,'invalid', 'not found', 'bind', 'Test.Select.MultiDimension')
        }
    }

    @Test
    void testMapReduceWithNonDiscreteRowAxis()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryRowAxisRangeTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo != 'N/A' && input.foo != 'Slow'})

        assert queryResult.size() == 3

        Map row = queryResult['legal'] as Map
        assert row['foo'] == 'Legal'
        assert row['bar'] == 'DEF'

        row = queryResult['fair'] as Map
        assert row['foo'] == 'Fair'
        assert row['bar'] == 'GHI'

        row = queryResult['average'] as Map
        assert row['foo'] == 'Average'
        assert row['bar'] == 'JKL'
    }

    @Test
    void testMapReduceWithNonDiscreteRowAxisMissingNames()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryRowAxisMissingNamesTest.json')

        try
        {
            ncube.mapReduce('query')
            fail 'Should have thrown an IllegalStateException'
        }
        catch(IllegalStateException ex)
        {
            assertContainsIgnoreCase(ex.message, 'must have', 'name', 'Test.Select.RowAxisRange.MissingNames', 'key')
        }
    }

    @Test
    void testMapReduceWithNonDiscreteColumnAxis()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryColumnAxisSetTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.group2 != 'Y'})

        assert queryResult.size() == 1

        Map row = queryResult['location'] as Map
        assert row['group1'] == 'midwest'
        assert row['group2'] == 'scattered'
        assert row['group3'] == 'western'
    }

    @Test
    void testMapReduceWithNonDiscreteColumnAxisMissingNames()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryColumnAxisMissingNamesTest.json')

        try
        {
            ncube.mapReduce('key')
            fail('Should have thrown an IllegalStateException')
        }
        catch(IllegalStateException ex)
        {
            assertContainsIgnoreCase(ex.message, 'must have', 'name', 'Test.Select.ColumnAxisSet.MissingNames', 'query')
        }
    }

    @Test
    void testMapReduceWithNoColumnAxisName()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')

        try
        {
            ncube.mapReduce(null, { Map input -> input.foo == 'NY'})
        }
        catch(IllegalArgumentException ex)
        {
            assertContainsIgnoreCase(ex.message, 'column axis', 'cannot be null')
        }
    }

    @Test
    void testMapReduceWithNoWhereClause()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')

        try
        {
            ncube.mapReduce('query', null)
        }
        catch(IllegalArgumentException ex)
        {
            assert ex.message == 'The where clause cannot be null'
        }
    }

    @Test
    void testMapReduceWithFilterAndReturnSets()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'TX' }, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH):['foo'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN):['bar'] as Set])

        assert queryResult.size() == 1

        Map row = queryResult['E'] as Map
        assert row.size() == 1
        assert row['bar'] == 'also test contains'
    }

    @Test
    void testMapReduceWithFilterAndReturnSetsColumnInBoth()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'TX' }, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH):['foo'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN):['foo', 'bar'] as Set])

        assert queryResult.size() == 1

        Map row = queryResult['E'] as Map
        assert row.size() == 2
        assert row['foo'] == 'TX'
        assert row['bar'] == 'also test contains'
    }

    @Test
    void testMapReduceWithFilterAndReturnSetsNonDiscreteAxis()
    {
        NCube ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryColumnAxisSetTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.group2 != 'Y'}, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH):['group2'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN):['group3'] as Set])

        assert queryResult.size() == 1

        Map row = queryResult['location'] as Map
        assert row.size() == 1
        assert row['group3'] == 'western'
    }

    @Test
    void testMapReduceWithFilterAndReturnSetsRunningCommandCell()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query', { Map input -> input.foo == 'OH' }, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH):['foo'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN):['bar'] as Set])

        assert queryResult.size() == 1

        Map row = queryResult['G'] as Map
        assert row.size() == 1
        assert row['bar'] == '5 G - bar'
    }

    @Test
    void testMapReduceWithFilterAndReturnSetsRunningCommandCellWithClosure()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.mapReduce('query',  { Map input -> input.foo == 'OH' }, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH):['foo'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN):['bar'] as Set])

        assert queryResult.size() == 1

        Map row = queryResult['G'] as Map
        assert row.size() == 1
        assert row['bar'] == '5 G - bar'
    }

    @Test
    void testMapReduceFromGroovyExpression()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        Map queryResult = ncube.getCell([key: 'H', query: 'bar']) as Map

        assert queryResult.size() == 1

        Map row = queryResult['D'] as Map
        assert row['foo'] == 'KY'
        assert row['bar'] == 33
    }

    @Test
    void testMapReduceFromGroovyExpressionAgainstAnotherCube()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'selectQueryTest.json')
        NCube otherCube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'selectQueryMultiDimTest.json')
        Map queryResult = ncube.getCell([key: 'H', query: 'bar', bind: 'bindToAValue', cubeName: 'Test.Select.MultiDimension']) as Map

        assert queryResult.size() == 1

        Collection<String> row = queryResult.values()
        assert row['foo'] == ['OH']
        assert row['bar'] == ['Ohio']
    }

    @Test
    void testMapReduceWithInvalidColumnsToSearch()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'mapReduceInvalidUsage.json')
        try {
            Map queryResult = ncube.mapReduce('code', {true}, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): ['date', 'priority'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): ['bar'] as Set])
            assert queryResult.size() == 2

            Map row = queryResult['name'] as Map
            assert row['bar'] == 'bar-name'

            row = queryResult['value'] as Map
            assert row['bar'] == 'bar-value'
        }
        catch (Exception e) {
            fail('Should allow and ignore invalid search columns specified in mapReduce options' + e)
        }
    }

    @Test
    void testMapReduceWithInvalidColumnsToReturn()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'mapReduceInvalidUsage.json')
        try {
            Map queryResult = ncube.mapReduce('code', {true}, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): ['bar'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): ['baz'] as Set])
            assert queryResult.size() == 2

            Map row = queryResult['name'] as Map
            assert row.size() == 0

            row = queryResult['value'] as Map
            assert row.size() == 0

        } catch (Exception e) {
            fail('Should allow and ignore invalid return columns specified in mapReduce options' + e)
        }
    }

    @Test
    void testMapReduceWithInvalidAxis()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'mapReduceInvalidUsage.json')
        try {
            ncube.mapReduce('fake', {true}, [(NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): ['bar'] as Set, (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): ['bar'] as Set])
            fail('Should not attempt to search when Axis does not exist')
        }
        catch (IllegalArgumentException e) {
            assertContainsIgnoreCase(e.message, 'axis', 'does not exist', 'fake')
        }
        catch (Exception e) {
            fail('Unexpected exception thrown: ' + e.toString())
        }
    }

    @Test
    void testUse()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'useRef.json')
        def x = ncube.getCell([column:'a', row:1])
        assert 'A3' == x

        x = ncube.getCell([column:'a', row:2])
        assert 'C2' == x

        x = ncube.getCell([column:'b', row:1])
        assert 'B3' == x

        x = ncube.getCell([column:'b', row:2])
        assert 'C2' == x

        x = ncube.getCell([column:'c', row:3])
        assert 'foo' == x
    }

    @Test
    void testUseAcrossCubes()
    {
        createRuntimeCubeFromResource(ApplicationID.testAppId, 'useRef.json')
        NCube ncube2 = createRuntimeCubeFromResource(ApplicationID.testAppId, 'useRef1.json')
        def x = ncube2.getCell([column:'a', row:1])
        assert 'C1' == x
    }

    @Test
    void testGetCells()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'getCellsTest.json')
        Object[] ids = [
                [1000000000001, 2000000000001],
                ['1000000000001', '2000000000002'],
                [1000000000002, 2000000000001],
                [1000000000002, 2000000000002],
                [1000000000002, 2000000000003],
                [1000000000001, 2000000000003]
        ] as Object[]
        Map input = [:]     // proves that no input still gets axis names bound (gender and age show up in inputUsed)
        Map output = [:]
        Object[] cellValues = ncube.getCells(ids, input, output, 13)
        assert cellValues.length == 6

        List list1 = cellValues[0] as List
        List id1 = list1[0] as List
        assert id1[0] == 1000000000001
        assert id1[1] == 2000000000001
        Map info1 = list1[1] as Map
        assert info1.type == 'boolean'
        assert info1.value == 'true'

        List list2 = cellValues[1] as List
        List id2 = list2[0] as List
        assert id2[0] == '1000000000001'       // proves that cell IDs can be Strings, longs, etc. (converter.convertToLong used internally)
        assert id2[1] == '2000000000002'
        Map info2 = list2[1] as Map
        assert info2.type == 'long'
        assert info2.value == '10'

        List list3 = cellValues[2] as List
        List id3 = list3[0] as List
        assert id3[0] == 1000000000002
        assert id3[1] == 2000000000001
        Map info3 = list3[1] as Map
        assert info3.type == 'boolean'
        assert info3.value == 'true'

        List list4 = cellValues[3] as List
        List id4 = list4[0] as List
        assert id4[0] == 1000000000002
        assert id4[1] == 2000000000002
        Map info4 = list4[1] as Map
        assert info4.type == 'int'
        assert info4.value == '13'     // proves passed in 'defaultValue' for cell works

        List list5 = cellValues[4] as List
        List id5 = list5[0] as List
        assert id5[0] == 1000000000002
        assert id5[1] == 2000000000003
        Map info5 = list5[1] as Map
        assert info5.type == 'string'
        assert info5.value == '[a, b, c]'     // proves passed in 'defaultValue' for cell works

        List list6 = cellValues[5] as List
        List id6 = list6[0] as List
        assert id6[0] == 1000000000001
        assert id6[1] == 2000000000003
        Map info6 = list6[1] as Map
        assert info6.type == 'string'
        assertContainsIgnoreCase((String)info6.value, 'err: Cannot invoke method next() on null object')

        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        assert output.cell11 == true
        assert output.cell21 == true    // prove same output used for all cells
        assert ruleInfo.getInputKeysUsed().containsAll(['gender', 'age'])
        assert ruleInfo.getInputKeysUsed().size() == 2
    }

    @Test
    void testCircularRefWithUse()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'testCircularRef.json')
        try {
            def x = ncube.getCell([accessType:'use'])
        }
        catch(Throwable t)
        {
            assert t instanceof CommandCellException
            assert t.cause instanceof StackOverflowError

            // Enforces order
            assertContainsIgnoreCase(t.message,
                    'error occurred',
                    'ncube.testcircularreference',
                    'cell:ncube.testcircularreference:[accesstype:use',
                    'cell:ncube.testcircularreference:[accesstype:null'
            )
        }
    }

    @Test
    void testCircularRefWithAt()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'testCircularRef.json')
        try {
            def x = ncube.getCell([accessType:'at'])
        }
        catch(Throwable t)
        {
            assert t instanceof CommandCellException
            assert t.cause instanceof StackOverflowError

            assertContainsIgnoreCase(t.message,
                    'error occurred',
                    'ncube.testcircularreference',
                    'cell:ncube.testcircularreference:[accesstype:at',
                    'cell:ncube.testcircularreference:[accesstype:null')
        }
    }

    @Test
    void testCircularRefWithRecursion()
    {
        try {
            NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'testCircularRef.json')
            def x = ncube.getCell([accessType:'recursion'])
            fail('exception should have been thrown')
        }
        catch (CommandCellException cce) {
            assertTrue(cce.cause instanceof StackOverflowError)
        }
        catch (Throwable t) {
            fail('CommandCellException should have been thrown')
        }
    }

    @Test
    void testGetDecisionTable()
    {
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'decision-tables/2dv.json')
        ncube.decisionTable
        assert ncube.getMetaProperty(DECISION_TABLE)
    }
    
    // ---------------------------------------------------------------------------------
    // ---------------------------------------------------------------------------------

    private static void simpleJsonCompare(String name)
    {
        NCube<?> ncube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, name)
        int h1 = ncube.hashCode()
        NCube dupe = ncube.duplicate(ncube.name)
        int h2 = dupe.hashCode()
        assertEquals(ncube, dupe)
        assertEquals(h1, h2)

        // Verify that all Axis and Column IDs are the same
        for (Axis axis : ncube.axes)
        {
            Axis dupeAxis = dupe.getAxis(axis.name)
            assertEquals(axis.id, dupeAxis.id)

            Iterator<Column> iThisCol = axis.columns.iterator()
            Iterator<Column> iThatCol = dupeAxis.columns.iterator()
            while (iThisCol.hasNext())
            {
                Column thisCol = iThisCol.next()
                Column thatCol = iThatCol.next()
                assertEquals(thisCol.id, thatCol.id)
            }
        }
    }

    static int countMatches(String s, String pattern)
    {
        int lastIndex = 0
        int count = 0

        while (lastIndex != -1)
        {
            lastIndex = s.indexOf(pattern, lastIndex)

            if (lastIndex != -1)
            {
                count++
                lastIndex += pattern.length()
            }
        }
        return count
    }

    private static void println(Object... args)
    {
        if (_debug)
        {
            for (Object arg : args)
            {
                System.out.println(arg)
            }
        }
    }
}
