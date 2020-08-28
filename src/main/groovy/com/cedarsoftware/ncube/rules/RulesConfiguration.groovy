package com.cedarsoftware.ncube.rules

import com.cedarsoftware.ncube.ApplicationID
import groovy.transform.CompileStatic
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

import javax.annotation.PostConstruct

import static com.cedarsoftware.ncube.ApplicationID.DEFAULT_TENANT
import static com.cedarsoftware.ncube.ApplicationID.HEAD
import static com.cedarsoftware.ncube.ReleaseStatus.RELEASE
import static com.cedarsoftware.util.StringUtilities.isEmpty

@Configuration
@ConfigurationProperties('ncube.rules')
@CompileStatic
class RulesConfiguration
{
    static final String ENGINE_NAME = 'name'
    static final String ENGINE_RULES = 'rules'
    static final String ENGINE_CATEGORIES = 'categories'
    static final String APP_TENANT = 'tenant'
    static final String APP_NAME = 'app'
    static final String APP_VERSION = 'version'
    static final String APP_STATUS = 'status'
    static final String APP_BRANCH = 'branch'

    List<Map<String, String>> engines
    private final Map<String, RulesEngine> rulesEngines = new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
    private volatile Map uiInfo = null

    @PostConstruct
    private void buildEngines()
    {
        if (!engines)
        {
            return
        }

        for (Map<String, String> engine : engines)
        {
            String name = engine[ENGINE_NAME]
            if (isEmpty(name))
            {
                throw new IllegalArgumentException("Rule engine config requires key '${ENGINE_NAME}'")
            }

            String tenant = engine[APP_TENANT] ?: DEFAULT_TENANT
            String app = engine[APP_NAME]
            String version = engine[APP_VERSION]
            String status = engine[APP_STATUS] ?: RELEASE.name()
            String branch = engine[APP_BRANCH] ?: HEAD

            ApplicationID appId = new ApplicationID(tenant, app, version, status, branch)

            String rules = engine[ENGINE_RULES]
            if (isEmpty(rules))
            {
                throw new IllegalArgumentException("Rule engine config requires key '${ENGINE_RULES}'")
            }
            RulesEngine rulesEngine = new RulesEngine(name, appId, rules, engine[ENGINE_CATEGORIES])
            rulesEngines[name] = rulesEngine
        }
    }

    void addRulesEngine(String engineName, ApplicationID appId, String rules, String categories)
    {
        RulesEngine rulesEngine = new RulesEngine(engineName, appId, rules, categories)
        rulesEngines[engineName] = rulesEngine
    }

    RulesEngine getRulesEngine(String name)
    {
        return rulesEngines[name]
    }

    Map<String, RulesEngine> getRulesEngines()
    {
        return rulesEngines
    }

    Set<String> getRulesEngineNames()
    {
        return rulesEngines.keySet()
    }

    Map getInfo()
    {
        if (uiInfo != null)
        {
            return uiInfo
        }

        synchronized (RulesConfiguration.class)
        {
            if (uiInfo == null)
            {
                Map info = [:]
                for (String name : rulesEngineNames)
                {
                    info[name] = getRulesEngine(name).info
                }
                uiInfo = info
            }
        }
        return uiInfo
    }
}
