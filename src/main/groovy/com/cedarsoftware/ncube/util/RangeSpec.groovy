package com.cedarsoftware.ncube.util

import groovy.transform.CompileStatic

import static com.cedarsoftware.util.Converter.convertToBigDecimal
import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.Converter.convertToDouble
import static com.cedarsoftware.util.Converter.convertToLong

/**
 * Special Set instance that hashes the Set&lt;Long&gt; column IDs with excellent dispersion,
 * while at the same time, using only a single primitive long (8 bytes) per entry.
 * This set is backed by a long[], so adding and removing items is O(n).
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
class RangeSpec
{
    String inputVarName
    String lowColumnName
    String highColumnName
    String dataType

    Comparable convertValueToDataType(Comparable value)
    {
        if (value == null)
        {
            return null
        }
        if (dataType.equalsIgnoreCase('DATE'))
        {
            return convertToDate(value)
        }
        else if (dataType.equalsIgnoreCase('LONG'))
        {
            return convertToLong(value)
        }
        else if (dataType.equalsIgnoreCase('DOUBLE'))
        {
            return convertToDouble(value)
        }
        else if (dataType.equalsIgnoreCase('BIG_DECIMAL'))
        {
            return convertToBigDecimal(value)
        }
        throw new IllegalStateException("Range data type must be one of: DATE, LONG, DOUBLE, BIG_DECIMAL, low column: ${lowColumnName}, high column: ${highColumnName}, data type: ${dataType}")
    }
}
