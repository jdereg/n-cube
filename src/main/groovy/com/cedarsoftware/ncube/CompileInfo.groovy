package com.cedarsoftware.ncube

import com.cedarsoftware.util.CompactCILinkedMap
import groovy.transform.CompileStatic

/**
 * This class contains information about the cube compilation.
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
class CompileInfo extends CompactCILinkedMap<String, Object>
{
    public static final String EXCEPTIONS = 'EXCEPTIONS'
    public static final String CUBE_NAME = 'CUBE_NAME'

    public static final String EXCEPTION_COORDINATES = 'COORDS'
    public static final String EXCEPTION_INSTANCE = 'EXCEPTION'

    CompileInfo()
    {
        put(EXCEPTIONS,[])
    }

    /**
     * @return a String identifying cube name
     */
    String getCubeName()
    {
        return get(CUBE_NAME)
    }

    /**
     * @return List of all exceptions that occurred during cube compilation. Each exception
     * contains:
     *      CUBE_NAME: a String with name of cube
     *      COORDS: a Map of coordinates that refer to cell being compiled
     *      EXCEPTION: the Exception caught during compile
     */
    List<Map> getExceptions()
    {
        return get(EXCEPTIONS) as List<Map>
    }

    /**
     * Sets name of cube
     */
    protected void setCubeName(String cubeName)
    {
        put(CUBE_NAME,cubeName)
    }

    /**
     * Adds exception to the list
     */
    protected void addException(Map coords, Exception e)
    {
        getExceptions() << [(EXCEPTION_COORDINATES):coords,(EXCEPTION_INSTANCE):e]
    }
}
