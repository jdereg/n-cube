package com.cedarsoftware.ncube.formatters

import com.cedarsoftware.util.SafeSimpleDateFormat
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic

import static com.cedarsoftware.util.IOUtilities.close
import static com.cedarsoftware.util.IOUtilities.flush

/**
 * Base class for NCube formatters
 *
 * @author Ken Partlow (kpartlow@gmail.com)
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
class BaseJsonFormatter
{
    public static final SafeSimpleDateFormat dateFormat = new SafeSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    protected final Writer builder

    BaseJsonFormatter()
    {
        builder = new StringWriter(8192)
    }

    BaseJsonFormatter(OutputStream stream)
    {
        builder = new OutputStreamWriter(stream, "UTF-8")
    }

    void startArray()
    {
        append('[')
    }

    void endArray()
    {
        append(']')
    }

    void startObject()
    {
        append('{')
    }

    void endObject()
    {
        append('}')
    }

    void comma()
    {
        append(',')
    }

    void append(Long id)
    {
        append(Long.toString(id))
    }

    void append(String str)
    {
        builder.write(str, 0, str.length())
    }

    protected void writeObjectKey(String key)
    {
        append(""""${key}":""")
    }

    protected void writeObjectKeyValue(String key, Object value, boolean includeComma) throws IOException
    {
        writeObjectKey(key)
        writeObjectValue(value)
        if (includeComma)
        {
            comma()
        }
    }

    protected void writeObjectValue(Object value) throws IOException
    {
        if (value instanceof String)
        {
            StringWriter w = new StringWriter()
            JsonWriter.writeJsonUtf8String((String) value, w)
            append(w.toString())
        }
        else
        {
            append(value == null ? "null" : value.toString())
        }
    }

    protected void closeStream() throws IOException
    {
        flush(builder)
        close(builder)
    }
}
