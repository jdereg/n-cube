import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.exception.RuleJump
import com.cedarsoftware.ncube.exception.RuleStop
import com.cedarsoftware.util.CaseInsensitiveMap

import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeClient
import static com.cedarsoftware.util.IOUtilities.close
import static com.cedarsoftware.util.IOUtilities.inputStreamToBytes
import static com.cedarsoftware.util.StringUtilities.createUTF8String
import static com.cedarsoftware.util.StringUtilities.equalsIgnoreCase

NCube getCube(String name = ncube.name, boolean quiet = false)
{
    if (equalsIgnoreCase(ncube.name, name))
    {
        return ncube
    }
    NCube cube = ncubeClient.getCube(ncube.applicationID, name)
    if (cube == null && !quiet)
    {
        throw new IllegalArgumentException("getCube() in templated cell, n-cube: ${name} not found.")
    }
    return cube
}

Axis getAxis(String axisName, String cubeName = ncube.name)
{
    Axis axis = getCube(cubeName).getAxis(axisName)
    if (axis == null)
    {
        throw new IllegalArgumentException("Axis: ${axisName}, does not exist on n-cube: ${cubeName}, app: ${ncube.applicationID}")
    }
    return axis
}

Column getColumn(Comparable value, String axisName, String cubeName = ncube.name)
{
    return getAxis(axisName, cubeName).findColumn(value)
}

def at(Map coord, String cubeName = ncube.name, def defaultValue = null)
{
    input.putAll(coord)
    return getCube(cubeName).getCell(input, output, defaultValue)
}

def at(Map coord, NCube cube, def defaultValue = null)
{
    input.putAll(coord)
    return cube.getCell(input, output, defaultValue)
}

def at(Map coord, String cubeName, def defaultValue, ApplicationID appId)
{
    NCube target = ncubeClient.getCube(appId, cubeName)
    if (target == null)
    {
        throw new IllegalArgumentException("at() within template cell attempted, n-cube: ${name} not found, app: ${appId}.")
    }
    input.putAll(coord)
    return target.getCell(input, output, defaultValue)
}

def go(Map coord, String cubeName = ncube.name, def defaultValue = null)
{
    return getCube(cubeName).getCell(coord, output, defaultValue)
}

def go(Map coord, NCube cube, def defaultValue = null)
{
    return cube.getCell(coord, output, defaultValue)
}

def go(Map coord, String cubeName, def defaultValue, ApplicationID appId)
{
    NCube target = ncubeClient.getCube(appId, cubeName)
    if (target == null)
    {
        throw new IllegalArgumentException("go() within template cell attempted, n-cube: ${name} not found, app: ${appId}.")
    }
    return target.getCell(coord, output, defaultValue)
}

def use(Map altInput, String cubeName = ncube.name, def defaultValue = null)
{
    Map origInput = new CaseInsensitiveMap(input)
    input.putAll(altInput)
    return getCube(cubeName).use(input, origInput, output, defaultValue)
}

def use(Map altInput, String cubeName, def defaultValue, ApplicationID appId)
{
    NCube target = ncubeClient.getCube(appId, cubeName)
    if (target == null)
    {
        throw new IllegalArgumentException("use() within template cell attempted, n-cube: ${cubeName} not found, app: ${appId}")
    }
    Map origInput = new CaseInsensitiveMap(input)
    input.putAll(altInput)
    return getCube(cubeName).use(input, origInput, output, defaultValue)
}

Map mapReduce(String colAxisName, Closure where = { true }, Map options = [:], String cubeName = null, ApplicationID appId = null)
{
    NCube target
    if (cubeName != null)
    {
        appId = appId ?: applicationID
        target = ncubeClient.getCube(appId, cubeName)
        if (target == null)
        {
            throw new IllegalArgumentException("mapReduce() attempted within template cell, but n-cube: ${cubeName} not found in app: ${appId}")
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

Map<Comparable, ?> getDecision(Map<String, ?> decisionInput, String cubeName, ApplicationID appId = ncube.applicationID)
{
    NCube ncube = ncubeClient.getCube(appId, cubeName)
    if (ncube == null)
    {
        throw new IllegalArgumentException("getDecision() attempted within cell, but n-cube: ${cubeName} not found in app: ${appId}")
    }
    return ncube.decisionTable.getDecision(decisionInput)
}

Map<Comparable, ?> getDecision(Iterable<Map<String, ?>> iterable, String cubeName, ApplicationID appId = ncube.applicationID)
{
    NCube ncube = ncubeClient.getCube(appId, cubeName)
    if (ncube == null)
    {
        throw new IllegalArgumentException("getDecision() attempted within cell, but n-cube: ${cubeName} not found in app: ${appId}")
    }
    return ncube.decisionTable.getDecision(iterable)
}

String url(String url)
{
    byte[] bytes = urlToBytes(url)
    if (bytes == null)
    {
        return null
    }
    return createUTF8String(bytes)
}

byte[] urlToBytes(String url)
{
    InputStream inStream = getClass().getResourceAsStream(url)
    byte[] bytes = inputStreamToBytes(inStream)
    close(inStream as Closeable)
    return bytes
}

def ruleStop()
{
    throw new RuleStop()
}

def jump(Map coord = [:])
{
    input.putAll(coord)
    throw new RuleJump(input)
}

static long now()
{
    return System.nanoTime()
}

static double elapsedMillis(long begin, long end)
{
    return (double) (end - begin) / 1000000.0d
}
