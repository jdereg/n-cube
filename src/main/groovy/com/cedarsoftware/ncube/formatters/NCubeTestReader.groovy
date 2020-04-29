package com.cedarsoftware.ncube.formatters

import com.cedarsoftware.ncube.CellInfo
import com.cedarsoftware.ncube.NCubeTest
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.io.JsonObject
import com.cedarsoftware.util.io.JsonReader
import groovy.transform.CompileStatic

import static com.cedarsoftware.util.StringUtilities.isEmpty

/**
 * @author Ken Partlow (kpartlow@gmail.com)
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
class NCubeTestReader
{
    static List<NCubeTest> convert(String s) throws IOException
    {
        List<NCubeTest> list = []
        if (isEmpty(s))
        {
            return list
        }

        Map args = [(JsonReader.USE_MAPS):true] as Map
        Object[] items = (Object[]) JsonReader.jsonToJava(s, args)

        for (Object o : items)
        {
            JsonObject item = (JsonObject) o
            String name = (String) item['name']
            Map<String, CellInfo> coord = createCoord((Object[]) item['coord'])
            List<CellInfo> assertions = createAssertions((Object[]) item['assertions'])
            CellInfo[] cellInfos = assertions.toArray(new CellInfo[0]) as CellInfo[]
            NCubeTest test = new NCubeTest(name, coord, cellInfos)
            list.add(test)
        }
        return list
    }

    static Map<String, CellInfo> createCoord(Object[] inputs)
    {
        Map<String, CellInfo> coord = new CompactCILinkedMap<>()
        if (ArrayUtilities.isEmpty(inputs))
        {
            return coord
        }

        for (Object item : inputs)
        {
            JsonObject<String, Object> input = (JsonObject<String, Object>) item
            for (entry in input.entrySet())
            {
                coord.put(entry.key, createCellInfo((JsonObject) entry.value))
            }
        }

        return coord
    }

    static CellInfo createCellInfo(JsonObject o)
    {
        String type = (String) o['type']
        String value = (String) o['value']
        return new CellInfo(type, value, o['isUrl'], false)
    }

    static List<CellInfo> createAssertions(Object[] cellInfoMaps)
    {
        List<CellInfo> list = []
        if (ArrayUtilities.isEmpty(cellInfoMaps))
        {
            return list
        }
        for (cellInfoMap in cellInfoMaps)
        {
            list.add(createCellInfo((JsonObject) cellInfoMap))
        }
        return list
    }
}
