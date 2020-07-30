package com.cedarsoftware.ncube

import com.cedarsoftware.servlet.JsonCommandServlet
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.StringUtilities
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.boot.SpringApplication
import org.springframework.boot.SpringBootVersion
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.boot.web.servlet.ServletRegistrationBean
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Profile
import org.springframework.core.SpringVersion
import org.springframework.web.filter.FormContentFilter
import org.springframework.web.filter.GenericFilterBean
import org.springframework.web.filter.HiddenHttpMethodFilter
import org.springframework.web.filter.RequestContextFilter

import javax.servlet.FilterChain
import javax.servlet.ServletException
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse
import javax.servlet.http.HttpServletRequest

import static java.util.Arrays.asList

/**
 * This class defines allowable actions against persisted n-cubes
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
@Slf4j
@ComponentScan(basePackages = ['com.cedarsoftware.config'])
@SpringBootApplication(exclude = [HibernateJpaAutoConfiguration])
@CompileStatic
class NCubeApplication
{
    static void main(String[] args)
    {
        ConfigurableApplicationContext ctx = SpringApplication.run(NCubeApplication, args)
        List<String> requiredProfiles = ['runtime-server', 'storage-server', 'combined-server']
        String[] activeProfiles = ctx.environment.activeProfiles
        if (ArrayUtilities.isEmpty(activeProfiles))
        {
            activeProfiles = [] as String[]
        }

        List<String> profiles = asList(activeProfiles)
        if (requiredProfiles.intersect(profiles).size() != 1)
        {
            ctx.close()
            throw new IllegalArgumentException("Missing active profile or redundant server types listed.  Expecting: one of ${requiredProfiles}.  Profiles supplied: ${profiles}")
        }

        String serverType
        if (profiles.contains('runtime-server'))
        {
            serverType = 'runtime'
        }
        else if (profiles.contains('combined-server'))
        {
            serverType = 'combined'
        }
        else // if (profiles.contains('storage-server'))
        {
            serverType = 'storage'
        }

        // Display server type and key versions
        byte[] mug = [0xf0, 0x9f, 0x8d, 0xba]
        String beer = StringUtilities.createUTF8String(mug)
        log.info("NCUBE ${serverType}-server started ${beer}")
        log.info("  Groovy version: ${GroovySystem.version}")
        log.info("  Java version: ${System.getProperty('java.version')}")
        log.info("  Spring version: ${SpringVersion.version}")
        log.info("  Spring-boot version: ${SpringBootVersion.version}")
    }

    @Bean
    ServletRegistrationBean servletRegistrationBean0()
    {
        ServletRegistrationBean bean = new ServletRegistrationBean(new JsonCommandServlet(), "/cmd/*")
        bean.enabled = true
        bean.loadOnStartup = 1
        bean.order = 1
        return bean
    }

    @Bean()
    @Profile('storage-server')
    FilterRegistrationBean filterRegistrationBean1()
    {
        GenericFilterBean filter = new PassThruFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.addUrlPatterns('/*')
        registration.enabled = true
        return registration
    }
    
    @Bean
    FilterRegistrationBean filterRegistrationBean2()
    {
        GenericFilterBean filter = new HiddenHttpMethodFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.enabled = false
        return registration
    }

    @Bean
    FilterRegistrationBean filterRegistrationBean3()
    {
        GenericFilterBean filter = new FormContentFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.enabled = false
        return registration
    }

    @Bean
    FilterRegistrationBean filterRegistrationBean4()
    {
        GenericFilterBean filter = new RequestContextFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.enabled = false
        return registration
    }

    /**
     * Use to implement exclusion logic.
     */
    private static class PassThruFilter extends GenericFilterBean
    {
        void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException
        {
            HttpServletRequest req = (HttpServletRequest) request
            String uri = req.requestURI

            // Specify what is allowed
            if (uri.startsWith("${req.contextPath}/actuator/") || uri.startsWith("${req.contextPath}/cmd/"))
            {
                chain.doFilter(request, response)
            }
            else
            {   // Give back simple page, no access to static (editor) content on storage-server.
                response.contentType = 'text/html'
                response.writer.println('<html><body>NCUBE storage-server</body></html>')
                response.writer.flush()
            }
        }
    }
}
