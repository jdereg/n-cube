package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

/**
 * Used to compare multiple ranges within NCube cells inside DecisionTables.
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
class RangeList
{
    List<Range> ranges = []

    void addRange(Range range)
    {
        ranges.add(range)
    }

    RangeList duplicate()
    {
        RangeList copy = new RangeList()
        copy.ranges = new ArrayList<>(ranges)
        return copy
    }

    boolean overlaps(Range candidate)
    {
        for (Range range : ranges)
        {
            if (range.overlap(candidate))
            {
                return true
            }
        }
        return false
    }
    
    // Intentionally do not include 'row'.  The 'row' member is a hint for indicating a conflicting row.
    boolean equals(Object other)
    {
        if (this.is(other))
        {
            return true
        }
        if (!(other instanceof RangeList))
        {
            return false
        }

        RangeList that = (RangeList) other
        return ranges.equals(that.ranges)
    }

    int hashCode()
    {
        return ranges.hashCode()
    }
}