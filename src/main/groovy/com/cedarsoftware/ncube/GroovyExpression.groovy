package com.cedarsoftware.ncube

import com.cedarsoftware.util.CaseInsensitiveMap
import com.google.common.base.Joiner
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import ncube.grv.exp.NCubeGroovyExpression

import java.lang.reflect.InvocationTargetException
import java.util.regex.Matcher

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.ncube.NCubeConstants.SYS_PROTOTYPE
import static com.cedarsoftware.util.StringUtilities.hasContent

/**
 * This class is used to create NCubeGroovyExpressions.  This means that
 * the code can start without any class or method signatures.  For
 * example, an expression could look like this:<pre>
 *    1) input.resource.KEY == 10
 *    2) $([BusinessUnit:'finance', state:'OH') * 1.045
 *    3) return input.state == 'OH' ? 1.0 : 2.0
 *    4) output.result = 'answer computed'; return 1.4
 *
 * </pre>
 * The variables (or properties) available to the expression are 'input',
 * 'output', and 'ncube'.  The 'input' variable represents the coordinate Map
 * passed in, the 'output' Map is available to write to, allowing any number
 * of return values to be created, and 'ncube' is a reference to the NCube
 * containing this running code.
 *
 * If desired, the cell can be written with a class { } structure, in which
 * case the class must extend NCubeGroovyExpression.
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
@Slf4j
@CompileStatic
class GroovyExpression extends GroovyBase
{
    public static final String EXP_IMPORTS = "exp.imports"
    public static final String EXP_CLASS = "exp.class"
    public static final String SYS_PROPERTY = "sys.property"

    //  Private constructor only for serialization.
    private GroovyExpression() { }

    GroovyExpression(String cmd, String url = null, boolean cache = false)
    {
        super(cmd, url, cache)
    }

    protected String buildGroovy(Map<String, Object> ctx, String className, String theirGroovy)
    {
        Matcher m = Regexes.hasClassDefPattern.matcher(theirGroovy)
        if (m.find())
        {   // If they include a class ... { in their source, then we do not add the 'buns' around the content.
            return theirGroovy
        }

        Set<String> extractedLines = new LinkedHashSet<>()
        String meat = extractImportsAndAnnotations(theirGroovy, extractedLines)

        // NOTE: CdnClassLoader needs to exclude the imports listed below with '*' (GroovyClassLoader will
        // 'bike-lock' search these for all unexpected tokens in the source being compiled.)
        NCube ncube = getNCube(ctx)

        // Attempt to load sys.prototype cube
        // If loaded, add the import statement list from this cube to the list of imports for generated cells
        String expClassName = 'ncube.grv.exp.NCubeGroovyExpression'
        String addlImports = ''

        if (!SYS_PROTOTYPE.equalsIgnoreCase(ncube.name))
        {
            NCube prototype = ncubeRuntime.getCube(ncube.applicationID, SYS_PROTOTYPE)

            if (prototype != null)
            {
                addlImports = addPrototypeExpImports(ctx, prototype)

                // Attempt to find class to inherit from
                expClassName = getPrototypeExpClass(ctx, prototype) ?: expClassName
            }
        }

        return """\
package ncube
import com.cedarsoftware.ncube.*
import com.cedarsoftware.ncube.exception.*
import com.cedarsoftware.ncube.formatters.*
import com.cedarsoftware.ncube.proximity.*
import com.cedarsoftware.ncube.util.*
import com.cedarsoftware.util.*
import com.cedarsoftware.util.io.*
import groovy.transform.CompileStatic 
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import java.lang.reflect.Method
import org.slf4j.Logger
import org.slf4j.LoggerFactory
${addlImports}
${Joiner.on('\n').join(extractedLines)}
class ${className} extends ${expClassName}
{
    Object run()
    {
    ${meat}
    }
}
"""
    }

    protected static String addPrototypeExpImports(Map<String, Object> ctx, NCube prototype)
    {
        try
        {
            StringBuilder text = new StringBuilder()
            Map<String, Object> input = getInput(ctx)
            Map<String, Object> copy = new CaseInsensitiveMap<>(input, new LinkedHashMap<>(input.size()))
            copy.put(SYS_PROPERTY, EXP_IMPORTS)
            Object importList = prototype.getCell(copy)
            if (importList instanceof Collection)
            {
                Collection<String> impList = (Collection) importList
                for (String importLine : impList)
                {
                    text.append('import ')
                    text.append(importLine)
                    text.append('\n')
                }
            }
            return text
        }
        catch (Exception e)
        {
            log.info("Exception occurred fetching imports from ${SYS_PROTOTYPE}", e)
            return ''
        }
    }

    protected static String getPrototypeExpClass(Map<String, Object> ctx, NCube prototype)
    {
        try
        {
            Map inp = getInput(ctx);
            Map input = new CaseInsensitiveMap(inp, new LinkedHashMap<>(inp.size()))
            input.put(SYS_PROPERTY, EXP_CLASS)
            Object className = prototype.getCell(input)
            if (className instanceof String && hasContent((String)className))
            {
                return (String) className
            }
        }
        catch (Exception e)
        {
            log.info("Exception occurred fetching base class for Groovy Expression cells from ${SYS_PROTOTYPE}", e)
        }
        return null
    }

    protected Object invokeRunMethod(NCubeGroovyExpression instance) throws Throwable
    {
        // If 'around' Advice has been added to n-cube, invoke it before calling Groovy expression's run() method
        NCube ncube = instance.ncube
        List<Advice> advices = ncube.getAdvices('run')
        if (!advices.empty)
        {
            for (Advice advice : advices)
            {
                if (!advice.before(null, ncube, instance.input, instance.output))
                {
                    return null
                }
            }
        }

        Throwable t = null
        Object ret = null

        try
        {
            ret = instance.run()
        }
        catch (ThreadDeath e)
        {
            throw e
        }
        catch (InvocationTargetException e)
        {
            t = e.targetException
        }
        catch (Throwable e)
        {   // Save exception
            t = e
        }

        // If 'around' Advice has been added to n-cube, invoke it after calling Groovy expression's run() method
        final int len = advices.size()
        if (len > 0)
        {
            for (int i = len - 1i; i >= 0i; i--)
            {
                Advice advice = advices.get(i)
                try
                {
                    advice.after(null, ncube, instance.input, instance.output, ret, t)
                    // pass exception (t) to advice (or null)
                }
                catch (ThreadDeath e)
                {
                    throw e
                }
                catch (Throwable e)
                {
                    log.error("An exception occurred calling 'after' advice: ${advice.name}", e)
                }
            }
        }
        if (t == null)
        {
            return ret
        }
        throw t
    }
}
