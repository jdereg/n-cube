package com.cedarsoftware.ncube

import com.cedarsoftware.util.CompactCILinkedMap
import groovy.text.SimpleTemplateEngine
import groovy.text.Template
import groovy.transform.CompileStatic

import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.util.IOUtilities.close
import static com.cedarsoftware.util.IOUtilities.inputStreamToBytes

/**
 * Process NCube template cells.  A template cell contains a String that may have both
 * NCube references to other NCubes within the <% %> or ${ } sections, AND, the
 * template may also contain Groovy template variables.  This class uses the Groovy
 * StandardTemplate which supports ${variable} and <%  %> replaceable tags.  The
 * 'variable' is the name of an NCube input coordinate key.
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
class GroovyTemplate extends ContentCmdCell
{
    private transient Template resolvedTemplate

    //  Private constructor only for serialization.
    private GroovyTemplate() { }

    GroovyTemplate(String cmd, String url = null, boolean cache = false)
    {
        super(cmd, url, cache)
    }

    protected def fetchResult(Map<String, Object> ctx)
    {
        Object data = null

        if (resolvedTemplate == null)
        {
            if (url == null)
            {
                data = cmd
            }
            else
            {
                data = fetchContentFromUrl(ctx)
            }
        }

        return executeInternal(data, ctx)
    }

    void getCubeNamesFromCommandText(final Set<String> cubeNames)
    {
        if (cmd == null)
        {
            return
        }

        Matcher m = Regexes.scripletPattern.matcher(cmd)

        while (m.find())
        {
            GroovyBase.getCubeNamesFromText(cubeNames, m.group(1))
        }

        m = Regexes.velocityPattern.matcher(cmd)

        while (m.find())
        {
            GroovyBase.getCubeNamesFromText(cubeNames, m.group(1))
        }
    }

    /**
     * Go through all <%  %> and ${  }.  For each one of these sections,
     * find all 'input.variableName' occurrences and add 'variableName' to
     * the passed in Set.
     * @param scopeKeys Set to add required scope (key) elements to.
     */
    void getScopeKeys(Set<String> scopeKeys)
    {
        Matcher m = Regexes.scripletPattern.matcher(cmd)  // <%  %>

        while (m.find())
        {
            Matcher m1 = Regexes.inputVar.matcher(m.group(1))
            while (m1.find())
            {
                scopeKeys.add(m1.group(2))
            }
        }

        m = Regexes.velocityPattern.matcher(cmd)          // ${   }

        while (m.find())
        {
            Matcher m1 = Regexes.inputVar.matcher(m.group(1))
            while (m1.find())
            {
                scopeKeys.add(m1.group(2))
            }
        }
    }

    protected def executeInternal(final Object data, final Map ctx)
    {
        // ctx.input, ctx.output, ctx.ncube
        // are ALWAYS set by NCube before the execution gets here.
        if (resolvedTemplate == null)
        {
            String cmd = data == null ? '' : data.toString()

            // Expand code : perform <% @()  $() %> and ${ @()   $() } substitutions before passing to template engine.
            cmd = replaceScriptletNCubeRefs(cmd, Regexes.scripletPattern, '<%', '%>')
            cmd = replaceScriptletNCubeRefs(cmd, Regexes.velocityPattern, '${', '}')

            NCube ncube = getNCube(ctx)
            URLClassLoader urlLoader = ncubeRuntime.getUrlClassLoader(ncube.applicationID, getInput(ctx))
            InputStream inStream = urlLoader.getResourceAsStream("ncube/grv/closure/NCubeTemplateClosures.groovy")
            String groovyClosures = new String(inputStreamToBytes(inStream))
            close(inStream)

            cmd = "<% ${groovyClosures} %>${cmd}"

            // Create Groovy Standard Template
            SimpleTemplateEngine engine = new SimpleTemplateEngine(urlLoader)
            synchronized(GroovyBase.class)
            {
                resolvedTemplate = engine.createTemplate(cmd)
            }
        }

        Map input = new CompactCILinkedMap(getInput(ctx))

        // Copy input before running template.  Protects input Map from being modified.
        ctx.input = input
        String result = resolvedTemplate.make(ctx).toString()
        return result
    }

    private static String replaceScriptletNCubeRefs(final String template, final Pattern pattern, final String prefix, final String suffix)
    {
        Matcher m = pattern.matcher(template)
        StringBuilder newStr = new StringBuilder()
        int last = 0

        while (m.find())
        {
            newStr.append(template.substring(last, m.start()))
            String inner = GroovyBase.expandNCubeShortCuts(m.group(1))
            newStr.append(prefix)
            newStr.append(inner)
            newStr.append(suffix)
            last = m.end()
        }

        newStr.append(template.substring(last))
        return newStr.toString()
    }
}
