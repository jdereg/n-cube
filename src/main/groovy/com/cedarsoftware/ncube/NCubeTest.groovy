package com.cedarsoftware.ncube

import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import groovy.transform.CompileStatic

/**
 * Class used to carry the NCube meta-information
 * to the client.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License");
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
class NCubeTest
{
    private final String name
    private final Map<String, CellInfo> coord
    private final CellInfo[] expected

    NCubeTest(String name, Map<String, CellInfo> input, CellInfo[] expected)
    {
        this.name = name
        coord = new CompactCILinkedMap<>(input)
        this.expected = expected
    }

    String getName()
    {
        return name
    }

    Map<String, CellInfo> getCoord()
    {
        List<String> inputs = new ArrayList(new CaseInsensitiveSet<>(coord.keySet()))
        Collections.sort(inputs)
        Map<String, CellInfo> retCoord = new CompactCILinkedMap<>()
        for (String key : inputs)
        {
            retCoord[key] = coord[key]
        }
        return retCoord
    }

    Map<String, Object> getCoordWithValues()
    {
        Map<String, Object> actuals = new CompactCILinkedMap<>()
        for (item in coord)
        {
            Map.Entry entry = item
            CellInfo cellInfo = (CellInfo) entry.value
            actuals.put((String)entry.key, cellInfo.recreate())
        }
        return actuals
    }

    CellInfo[] getAssertions()
    {
        return this.expected
    }

    List<GroovyExpression> createAssertions()
    {
        List<GroovyExpression> actuals = []
        for (CellInfo item : this.expected)
        {
            actuals.add((GroovyExpression) item.recreate())
        }
        return actuals
    }
}
