var RULES = (function ($)
{
    var group = '';
    var ruleList = $('#ruleList');
    var selectCategories = $('#selectCategories');
    var selectEngine = $('#selectEngine');
    var selectGroup = $('#selectGroup');
    var selectRules = $('#selectRules');
    var categoryForm = $('#categoryForm');
    var engine = '';
    var info = {};

    function getRules()
    {
        var body = {engine: engine, group: group};
        if (!group)
        {
            return;
        }
        callServer('GET', 'ui/rules', body, buildRules);
    }

    function getCategories()
    {
        var div, category, values;
        var body = {_engine: engine};
        var divs = categoryForm.children('div');
        divs.each(function( index ) {
            div = divs[index];
            category = $(this).find('label').text();
            values = $(this).find('select').val();
            if (0 < values.length)
            {
                body[category] = values;
            }
        });
        callServer('POST', 'ui/rulesByCategory', body, buildRules);
    }

    var buildRules = function buildRules(data)
    {
        var i, len, ruleType, rule, ul, ruleTypeAndName, li;
        var ruleTypes = Object.keys(data);
        for (i = 0, len = ruleTypes.length; i < len; i++)
        {
            ruleType = ruleTypes[i];
            rule = data[ruleType];
            ruleTypeAndName = ruleNameTemplate(ruleType, rule['className'])
            ul = buildObject(rule['objects']);
            li = $('<li class="ruleGroup"/>');
            li.append(ruleTypeAndName);
            li.append(ul);
            ruleList.append(li);
            ruleList.append('<hr class="ruleGroupSeparator">')
        }

        $('a').click(function () {
            getNCubeHtml(this.text, this.getAttribute('data-appId'))
        });
    };

    function buildObject(rules) // return ul with object and methods
    {
        var i, len, name, title, object, methods, li;
        var ul = $('<ul/>');
        var objects = Object.keys(rules);
        for (i = 0, len = objects.length; i < len; i++)
        {
            name = objects[i];
            title = ruleTitleTemplate(objects[i]);
            object = rules[name];
            methods = buildMethods(object);
            li = $('<li class="ruleGroup"/>');
            li.append(title);
            li.append(methods);
            ul.append(li);
        }
        return ul;
    }

    function buildMethods(object) // return ul with li of method names
    {
        var i, len, rule, li;
        var ul = $('<ul/>');
        var rules = object['rules'];
        for (i = 0, len = rules.length; i < len; i++)
        {
            rule = rules[i];
            li = $('<li/>');
            li.append(methodTemplate(rule));
            ul.append(li);
        }
        return ul;
    }

    function ruleNameTemplate(ruleType, className)
    {
        return `<span class="ruleType">${ruleType}</span> (<span class="className">${className}</span>)`;
    }

    function ruleTitleTemplate(name)
    {
        return `<span class="ruleTitle">${name}</span>`
    }

    function methodTemplate(rule)
    {
        let ruleHtml = '<span class="ruleMethod">'
        if (rule['name'])
        {
            ruleHtml += `<span class="ruleName"><span>${rule['name']}</span>: </span>`
        }
        if (rule['condition'])
        {
            if (rule['condition'] === "true" || rule['condition'] === "false")
            {
                ruleHtml += `<span class="conditionTrue">${rule['condition']}</span>`
            }
            else
            {
                ruleHtml += `<span class="ruleCondition conditionTip"><em>Condition</em><span class="conditionTipText">${rule['condition']}</span></span>`
            }
        }
        if (rule['noContent'])
        {
            ruleHtml += `<span class="noContent">[Rule has no content]</span>`
            ruleHtml += '</span>'
            return ruleHtml
        }
        if (rule['documentation'])
        {
            ruleHtml += `<span class="docText">${rule['documentation']}</span>`
        }
        if (rule['code'])
        {
            ruleHtml += `<span class="codeTip"><em>Code</em><span class="codeTipText">${rule['code']}</span></span>` // note: Just changed the inner pre to a span
        }
        if (rule['methodName'])
        {
            ruleHtml += `<span class="methodName conditionTip"><em>Method</em><span class="conditionTipText">${rule['methodName']}</span></span>`
        }
        if (rule['ncubes'])
        {
            var i, len, ncube, ref;
            let ncubes = rule['ncubes'];
            if (ncubes.length > 0)
            {
                let appId = rule['appId'];
                var list = ' [';
                for (i = 0, len = ncubes.length; i < len; i++)
                {
                    ncube = ncubes[i];
                    ref = `<a class="ncubes" data-appId="${appId}" href="#">${ncubes[i]}</a>`;
                    list += ref;
                    if (i < len -1)
                    {
                        list += ', '
                    }
                }
                list += ']'
                ruleHtml += list
            }
        }
        ruleHtml += '</span>'
        return ruleHtml
    }

    function resetRules()
    {
        ruleList.empty();
    }

    function getNCubeHtml(ncube, appIdString)
    {
        var callback = function(data)
        {
            var w = window.open('', ncube + appIdString);
            w.document.write(data.html);
        };
        var body = {name: ncube, appIdString: appIdString};
        callServer('GET', 'ui/ncube', body, callback);
    }

    function loadPage()
    {
        resetRules();
        setListeners();
        getInfo();
    }

    function getInfo()
    {
        callServer('GET', 'ui/rulesInfo', null, buildPage)
    }

    var buildPage = function buildPage(data)
    {
        var i, len, listEngine;
        var engines = Object.keys(data);
        info = data;
        engine = engines[0];

        for (i = 0, len = engines.length; i < len; i++)
        {
            listEngine = engines[i];
            selectEngine.append($('<option/>').attr('value', listEngine).text(listEngine));
        }

        buildSelectors();
    };

    function buildSelectors()
    {
        selectGroup.empty();
        categoryForm.empty();
        buildGroups(info[engine]['groups']);
        buildCategories(info[engine]['categories']);
    }

    function buildGroups(groups)
    {
        var i, len, group;
        selectGroup.append($('<option/>').attr('value', '').text(''));
        for (i = 0, len = groups.length; i < len; i++)
        {
            group = groups[i];
            selectGroup.append($('<option/>').attr('value', group).text(group));
        }
    }

    function buildCategories(categoriesMap)
    {
        var i, category, row, label, div, select;
        var categories = Object.keys(categoriesMap);
        var len = categories.length;
        if (0 < len)
        {
            selectRules.show();
        }
        else
        {
            selectRules.hide();
        }

        for (i = 0, len; i < len; i++)
        {
            category = categories[i];
            row = $('<div class="form-group row"/>');
            label = $('<label class="col-sm-2 col-form-label"/>');
            label.text(category);
            row.append(label);
            select = buildCategorySelect(categoriesMap[category]);
            div = $('<div class="col-sm-10"/>');
            div.append(select);
            row.append(div);
            categoryForm.append(row)
        }
    }

    function buildCategorySelect(categoryValues)
    {
        var i, len, value;
        var select = $('<select multiple  class="form-control"/>');
        for (i = 0, len = categoryValues.length; i < len; i++)
        {
            value = categoryValues[i];
            select.append($('<option/>').attr('value', value).text(value));
        }
        return select;
    }

    function setListeners()
    {
        selectGroup.on('change', function () {
            group = selectGroup.val();
            resetRules();
            getRules();
        });

        selectCategories.on('click', function() {
            resetRules();
            getCategories();
        });

        selectEngine.on('change', function() {
            engine = selectEngine.val();
            resetRules();
            buildSelectors();
        });
    }

    function callServer(type, method, body, callback)
    {
        var ajax = {
            type: type,
            async: true,
            url: '/' + method,
            dataType: 'json',
            cache: false,
            timeout: 600000,
            success: function (data)
            {
                if (callback)
                {
                    callback(data);
                }
            },
            error: function (data)
            {
                resetRules();
                // TODO - handle errors generating docs better
            }
        };

        if (body)
        {
            if ('POST' === type)
            {
                ajax.data = JSON.stringify(body);
                ajax.headers = {
                    'Accept': 'application/json',
                    'Content-Type': 'application/json'
                };
            }
            else
            {
                ajax.data = body;
            }
        }

        $.ajax(ajax);
    }

    loadPage();

})(jQuery);