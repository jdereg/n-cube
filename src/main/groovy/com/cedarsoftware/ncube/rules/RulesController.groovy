package com.cedarsoftware.ncube.rules

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.NCube
import groovy.transform.CompileStatic
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static org.springframework.http.HttpStatus.OK

@RestController
@CompileStatic
class RulesController
{
    private RulesConfiguration rulesConfiguration

    RulesController(RulesConfiguration rulesConfiguration)
    {
        this.rulesConfiguration = rulesConfiguration
    }

    @GetMapping(path = ['ui/rulesInfo'])
    ResponseEntity<Map> getInfo()
    {
        Map info = rulesConfiguration.info
        return ResponseEntity.status(OK).body(info)
    }

    @GetMapping(path = ['ui/rules'])
    ResponseEntity<Map> getBusinessRules(@RequestParam String engine, @RequestParam String group)
    {
        Map rules = rulesConfiguration.getRulesEngine(engine).generateDocumentationForGroups([group])
        return ResponseEntity.status(OK).body(rules)
    }

    @PostMapping(path = ['ui/rulesByCategory'])
    ResponseEntity<Map> getRulesByCategories(@RequestBody Map<String, Object> categories)
    {
        String engine = categories['_engine']
        categories.remove('_engine')
        Map rules = rulesConfiguration.getRulesEngine(engine).generateDocumentation(categories)
        return ResponseEntity.status(OK).body(rules)
    }

    @GetMapping(path = ['ui/ncube'])
    ResponseEntity<Map> getNcubeHtml(@RequestParam String name, @RequestParam String appIdString)
    {
        ApplicationID applicationId = ApplicationID.convert(appIdString)
        NCube ncube = ncubeRuntime.getCube(applicationId, name)
        if (!ncube)
        {
            String error = "NCube: ${name} not found in app: ${applicationId}"
            return ResponseEntity.status(OK).body([html: error] as Map)
        }

        String html = ncube.toHtml('trait', 'traits', 'businessDivisionCode', 'bu', 'month', 'months', 'col', 'column', 'cols', 'columns', 'attribute', 'attributes')
        return ResponseEntity.status(OK).body([html: html] as Map)
    }
}
