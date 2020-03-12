package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.codehaus.groovy.runtime.GStringImpl

/**
 * This class is used to substitute a Groovy String (GString) in place of another value.  When
 * the holder of an instance of this class treats it like a String (.equals(), .hashCode(), .compareTo(), etc.)
 * it works like a String.  For methods it does not understand, it delegates to the contained object.
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
class GStringWrapper extends GStringImpl
{
    private Object originalValue

    GStringWrapper(String s, Object value)
    {
        this("${s}", value)
    }

    GStringWrapper(GString s, Object value)
    {
        super(s.values, s.strings)
        while (value instanceof GStringWrapper)
        {   // Do not allow a GStringWrapper as the original value
            value = ((GStringWrapper) value).originalValue
        }
        this.originalValue = value
    }

    Object getOriginalValue()
    {
        return originalValue
    }
    
    def propertyMissing(String name)
    {
        return originalValue.metaClass.getProperty(originalValue, name)
    }

    def propertyMissing(String name, def value)
    {
        originalValue.metaClass.setProperty(originalValue, name, value)
    }

    def methodMissing(String name, Object args)
    {
        return originalValue.invokeMethod(name, args)
    }
}
