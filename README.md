n-cube
======
<!-- [![Build Status](https://travis-ci.org/jdereg/n-cube.svg?branch=master)](https://travis-ci.org/jdereg/n-cube) -->
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.cedarsoftware/n-cube/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.cedarsoftware/n-cube)
[![Javadoc](https://javadoc.io/badge/com.cedarsoftware/n-cube.svg)](http://www.javadoc.io/doc/com.cedarsoftware/n-cube)

n-cube is a Rules Engine, Decision Table, Decision Tree, Templating Engine, and Enterprise Spreadsheet, built as a hyper-space.  The Domain Specific Language (**DSL**) for rules is [**Groovy**](http://www.groovy-lang.org/). To include in your project:

```
<dependency>
  <groupId>com.cedarsoftware</groupId>
  <artifactId>n-cube</artifactId>
  <version>4.7.5</version>
</dependency>
```

### Sponsors
[![Alt text](https://www.yourkit.com/images/yklogo.png "YourKit")](https://www.yourkit.com/.net/profiler/index.jsp)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.

<a href="https://www.jetbrains.com/idea/"><img alt="Intellij IDEA from JetBrains" src="https://s-media-cache-ak0.pinimg.com/236x/bd/f4/90/bdf49052dd79aa1e1fc2270a02ba783c.jpg" data-canonical-src="https://s-media-cache-ak0.pinimg.com/236x/bd/f4/90/bdf49052dd79aa1e1fc2270a02ba783c.jpg" width="100" height="100" /></a>
**Intellij IDEA**
___
#### The image below is a Visual Summary of the main capabilities of n-cube.
![Alt text](https://raw.githubusercontent.com/jdereg/n-cube/master/n-cubeImage.png "n-cube Capabilities")

What are the components of an n-cube?
An n-cube has a set of axes (plural of axis), each of which adds a dimension. For example, in Excel, there are two axes, one axis numbered [rows] and one lettered [columns]. Within n-cube, each axis has a name, like 'State', 'Date', 'year', 'Gender', 'Month', etc.

Each axis contains columns.  In excel, the columns are just numbers and letters.  In n-cube, the columns can be a set of values like the months of a year, years, age ranges, price ranges, dates, states, coordinates (2D, 3D, lat/lon), expressions, and so on.

A column can be a simple data type like a String, Number, Date, but it can also represent a Range [low, hi) as well as a Set (a combination of discrete values and ranges).  A column can also contain expressions or any class that implements Java's Comparable interface.  It is these columns that input coordinates match (bind to).

Finally, there are cells.  In a spreadsheet, you have a row and column, like B25 to represent a cell.  In n-cube, a cell is similarly represented by a coordinate.  A Java (or Groovy) `Map` is used, where the key is the name of an axis, and the value is the value that will 'bind' or match a column on the axis.  If a value does not match any column on an axis, you have the option of adding a 'default' column to the axis.  Here's an example `[age:24, state:'CA', date:'2012/12/17']`.  The format given here for a `Map` is the declarative form used by Groovy.  In Java, that would be `map.put("age", 24)`, `map.put("state", "CA")` and so on.  Because the Groovy form is much shorter, it will be used from here on out to represent coordinate maps.  Because a `Map` is used as the input coordinate, you can have as many dimensions (keys) as desired.

Once an n-cube is set up, and a coordinate is also set up (e.g. Map coord = [age:24, state:'CA']), the most basic API to access it is `ncube.getCell(coord)`.  The return value will be the value of the cell at the given coordinate.  If the cell contains a simple value (`String`, `integer`, `Date`, `floating point number`, `boolean value`), it is returned.  If the cell contains an expression (written in Groovy), the expression is executed.  The return value for the cell in this case is the return value of the expression.

Expressions can be a simple as: `input.age > 17`, which would return `true` if the 'age' key on the input coordinate (map) was greater than 17, or `false` if not.  Expressions can be as complex as an entire `Class` with multiple methods (that can use other classes).  Expressions are written in Groovy.  See http://www.groovy-lang.org/.  Groovy was chosen because it is essentially Java (has Java syntax, compiles and runs at Java speed), but has many syntactic short-cuts that result in shorter code as compared to Java.

A cell in an n-cube can reference another cell within the same n-cube, like you might do in Excel.  For example, you may have a formula in Excel like this: `=b25 + b32 * 2`, stored say in `A1`.  The value for `A1` would be computed using the formula stored in `A1`.  N-cube allows these same capabilities, plus more (code / business logic).  A cell could have an `if` statement in it, a `for-loop`, `switch statement`, reference other cells within the same cube, or it can reference cells within different n-cubes.  The referenced cell can then be another formula, reference other cells, and so on.

### Rule Engine ([Developer Guide](README-rules.md))
When used as a rule engine, at least one axis within the n-cube is marked as as 'Rule' axis type.  In that case, each column is written as a condition (in Groovy).  For example, `input.age < 18`.  When a Rules n-cube is executed, each condition on the Rule axis is evaluated.  If the value is `true` (as how Groovy considers truth: http://www.groovy-lang.org/semantics.html#Groovy-Truth), then the associated cell is executed.  If no conditions are executed, and there is a default column on the rule axis, then the statement associated to the default column is executed.

To kick off the Rule execution, `ncube.getCell(coord, output)` is called. The conditions along the Rule axis are executed linearly, in order. Condition columns can reference values passed in on the input map (using `input.age`, `input.state`, etc.) as well as cells within other cubes.

The input coordinate map is referenced through the variable `input`.  The output map is referenced through the variable `output`.  Both can be referenced in the condition as well as in the cell (for expression, method, and template cells).  Typically, when used in rule mode, as conditions fire, the corresponding cell that is executed writes something to the output map.  For example in an pricing application, `state =='CA' || state == 'TX'` as the condition, the corresponding cell may have `output.productCost *= 1.07`.  The tax condition, for example.

The condition column can contain multiple statements.  Think of it like a method body.  The value of the last statement executed is evaluated as the condition. Your code has access to the input coordinate (map), output map, and the n-cube in which the code resides.  All Java code libraries and Groovy can be accessed as well.  For example, `println` from Groovy can be added to the conditions for debugging (as well as added to the executed cell).  The Groovy expression (or methods) in the executed cell can write multiple outputs to the output map.

As each condition on the Rule axis is executed, the n-cube rule engine writes information to a "_rule" entry into the output map.  This _rule entry is a `Map` which includes the condition name executed, the condition expression executed, and other useful information.  This allows you to evaluate the rule exection while developing the rules, to see rules fired.  This Map can be cast to `RuleInfo`, which has explicit APIs on it to retreive values from it, eliminating the need to know the keys.

In general, as cells execute, they write to the `output` map.  The `input` coordinate could be written to as well.  If it is modified, and a further n-cube is referenced, any modifications to the `input` coordinate will remain in place until that execution path returns.  When the execution path of the rules finishes and returns, the `input` map is restored to it's prior condition before execution. When returning then to an outer n-cube (or the code that called `ncube.getCell()`), that code will see no changes to the `input` map.  The `output` map will, of course, contain whatever changes were written to it.

Both condition columns and executed cells can tell the rule engine to restart execution of the conditions as well as to terminate any further conditions from being executed.  This is a linear rules execution flow, and intentionally not the RETE algorithm.

### Decision Table
When using n-cube as a decision table, each axis represents a decision variable.  For example, a state axis with all of the states of a country.  When accessed, the `input` coordinate would have the 'state' variable as a key in the input map, and the associated value to state would be a state, for example, 'KS' (Kansas).  If the data changes over time, it is common to add a 'date' axis.  Meaning that at one point in time, say for Kansas, the value 10 was returned, but within a different time frame, perhaps 11 is returned.

Common decision variables are country, state / providence, date, business unit, business codes, user role, actions, resources, and so no.  There is no limit to the number of axes that an n-cube can have (other than memory).

Decision tables are great and work best when all variable combinations make sense (a * b * c ... * n).  If the problem space has some combinations that do not make sense, then you may want to use n-cube's Decision Tree capability.  n-cube allows you to combine decision tables, decision trees, rules, and so on, ad infinitum.

### Decision Tree
A good example for a decision tree, is modeling the continents and countries of the world.  Not all continents have the same countries.  Therefore, it would not make sense to have an n-cube with one axis as 'continents' and another axis as 'countries.'  Instead, the initial (entry or outer n-cube) 'world' would have an axis 'continents', with columns Africa, Antarctica, Asia, Australia, Europe, North America, South America.  For each continent column, it's corresponding cell is a reference to a 'country' n-cube for that continent.  The cell reference is written like this: `@NorthAmericaCountries[:]` for example.  When this cell is executed, it in turn calls the 'NorthAmericaCountries' n-cube with the same input as was passed to the original ncube.  The `[ : ]` means that no modifications are being made to the input.  Additional inputs could be added here, as well as existing inputs could be changed before accessing the joined n-cube.

In the 'NorthAmericaCountries' n-cube, the cells would return a value (or if a subdivision of the countries is needed like 'States', the cells would join to yet further n-cubes modeling those subdivisions).  In order to 'talk to' or 'use' this n-cube decision tree, the code would look like this: `Map coord = [Continent:'NA', Country:'USA', State:'OH']` for example.  This would hit the North America column in the world n-cube, that cell would call the NorthAmericaCountries n-cube, which would then join to the 'UsaStates' n-cube.  To reach a Canadian province, for example, the input coordinate would look like this: `Map coord = [Continent:'NA', Country:'Canada', Province:'Quebec']`.  Notice that the 3rd parameter to the input is not state but province.  Both inputs work, because at each decision level, the appropriate n-cubes join to each other.

At each n-cube along the decision path, it could have additional 'scope' or dimensionality.  For example, a product axis may exist as a second axis on the cubes (or some of the cubes).  Think of a decision tree as stitching together multiple decision tables.  The cells are whatever you need them to be (Strings, numbers, Java objects, Groovy code to executed, etc.) In the case of code, think of your execution path of your program as going through a 'scope router' or 'scope filter' before the appropriate code is selected and executed.

### Template Engine
n-cube can be used to return templates (think of a template as an HTML page, for example, with replaceable parts - like mail merge.  Not limited to HTML, it could be any text file.)  When a template cell is executed, variables within the template are replaced (like mail merge).  If you have used the Apache project's Velocity project, Groovy templates, or have written JSP / ASP files, then you already have an idea on how to use templates.

Snippets written like this `<%  code or variable references   %>` or `${code / variable references}` can be added to the template.  Before the template is returned (think HTML page), these variable sections are executed.  The replaceable sections can reference n-cubes, for example, to get language specific content, region specific content, mobile / non-mobile content, browser specific content, and so on, to then fill-in a variable portion of the page.

Instead of actually storing the HTML, Groovy Code, etc. directly in an n-cube cell, the content can be referenced via a URL.  This allows the HTML page to be stored on a CDN (Content Delivery Network), and then selectively retrieved (per language, state, business unit, date, etc.) and then substitutions within the page made as well (if needed, using the templating mechanism).  Image files can be referenced this way as well, allowing different images to be retrieved depending on state, date, language, product, and so on.

### Creating n-cubes
Use either the Simple JSON format to create n-cubes, or the nCubeEditor to editing the pages.  At the moment, there is no cloud-based editor for n-cube, so you need to set up the nCubeEditor as a web-app within a Java container like tomcat or jetty.  See the sample JSON files in the test / resources directories for examples.

These are read in using the `NCubeManager.getNCubeFromResource()` API.  You can also call `ncube.fromSimpleJson(String json)`.

#### Licensing
Copyright 2012-2019 Cedar Software, LLC

Licensed under the Apache License, Version 2.0

See [changelog.md](/changelog.md) for revision history.

By: John DeRegnaucourt
