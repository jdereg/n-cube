package com.cedarsoftware.ncube.util

import com.cedarsoftware.util.LongHashSet
import groovy.transform.CompileStatic

/**
 * Holds all of the cells values of an NCube.  The keys of the Map are a Set<Long>.  This Set
 * contains all of the column IDs that this cell points to.  The value-side of the map is the
 * actual cell value (a long, a String, a GroovyExpression, etc.)
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
class CellMap<V> extends HashMap<Set<Long>, V>
{
    CellMap() { }

    CellMap(Map<Set<Long>, V> m)
    {
        putAll(m)
    }

    V put(Set<Long> key, V value)
    {
        if (key instanceof LongHashSet)
        {
            return super.put(key, value)
        }
        return super.put(new LongHashSet(key), value)
    }

    void putAll(Map<? extends Set<Long>, ? extends V> map)
    {
        for (entry in map.entrySet())
        {
            put(entry.key, entry.value)
        }
    }
}