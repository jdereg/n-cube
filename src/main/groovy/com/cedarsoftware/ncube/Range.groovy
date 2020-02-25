package com.cedarsoftware.ncube

import com.google.common.base.Splitter
import groovy.transform.CompileStatic

import static com.cedarsoftware.util.Converter.convertToInteger

/**
 * This class is used to represent a 'band' or 'range' of values (numeric, date, etc.)
 * A priority can be assigned to a Range, in which case, two ranges that would normally
 * be considered overlapping, are not considering overlapping if there priorities are
 * different values.
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
class Range implements Comparable<Range>
{
    Comparable low
    Comparable high
    int priority

    Range()
    {
        low = null
        high = null
        priority = 1000
    }

    Range(Comparable low, Comparable high)
    {
        this(low, high, 1000)
    }

    Range(Comparable low, Comparable high, int priority)
    {
        if (low == null || high == null)
        {
            throw new IllegalArgumentException('Range value cannot be null')
        }

        if (low.equals(high))
        {   // Using compareTo() because we know that it HAD to be implemented (whereas .equals() comes free)
            throw new IllegalArgumentException('Range low and high must be different')
        }
        this.priority = priority
        this.low = low
        this.high = high
    }

    Range(String enchilada)
    {
        Iterable<String> bites = Splitter.on('/').trimResults().omitEmptyStrings().split(enchilada)
        String dataType = bites[0]
        low = 'Default' == bites[1] ? null : (Comparable) CellInfo.parseJsonValue(bites[1], null, dataType, false)
        high = 'Default' == bites[2] ? null : (Comparable) CellInfo.parseJsonValue(bites[2], null, dataType, false)
        priority = convertToInteger(bites[3])
    }

    String toString()
    {
        return "[${CellInfo.formatForDisplay(low)} - ${CellInfo.formatForDisplay(high)})"
    }

    String out()
    {
        String lowValue = CellInfo.formatForDisplay(low)
        String highValue = CellInfo.formatForDisplay(high)
        return "${CellInfo.getType(low)}/${lowValue}/${highValue}/${priority}"
    }

    int compareTo(Range that)
    {
        int ret = low.compareTo(that.low)
        if (ret != 0)
        {
            return ret
        }
        ret = high.compareTo(that.high)
        if (ret != 0)
        {
            return ret
        }
        if (priority == that.priority)
        {
            return 0
        }
        return priority - that.priority
    }

    boolean equals(Object other)
    {
        if (!(other instanceof Range))
        {
            return false
        }
        if (this == other)
        {
            return true
        }
        return compareTo((Range)other) == 0
    }

    int hashCode()
    {
        return low.hashCode() + high.hashCode() + priority
    }

    /**
     * @param value to compare with Range to determine if the value is
     * within the Range [low, high).
     * @return -1 if the value is less than the range low point, 0 if
     * the value is within the range, and 1 if the value is greater than or equal
     * to the range high point.
     */
    int isWithin(Comparable value)
    {
        if (value == null)
        {
            return 1
        }

        if (value.compareTo(low) < 0)
        {
            return -1
        }
        else if (value.compareTo(high) >= 0)
        {
            return 1
        }
        return 0
    }

    /**
     * @return boolean true if the line segments represented by these two ranges
     * overlap.  Assumption that the Range objects 'low' value is less than the
     * 'high' value.
     */
    boolean overlap(Range that)
    {
        boolean rangesOverlap = !(high.compareTo(that.low) <= 0 || low.compareTo(that.high) >= 0)
        if (rangesOverlap)
        {
            return priority == that.priority
        }
        return false
    }
}
