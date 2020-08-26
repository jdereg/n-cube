### Revision History
* 4.8.2
  * Added support for ncube.legacy.grv.exp configuration for backwards compatibility in `NCubeGroovyExpression`
  * Fixed `NCube.getMap` not copying the coordinate map before modifying it
* 4.8.1
  * Updated `RulesEngine` documentation to handle orchestration NCubes that end in the same string after the last`.` in the name.
* 4.8.0
  * Added support for `AND`, `OR`, `NOT`, `NAND`, `NOR` to `DecisionTable`.  Can be used with both regular input columns and range input columns.  Also, multiple values can be used with `AND`, `OR`, `NAND`, and `NOR`.  `NOT` can only have one value.  See the [Decision Table Developer Guide](README-decision.md) for more information. 
  * Added `BusinessRule.runTargetRules()` that traverses the data model running the rules for a target on a given source.
  * Updated `RulesEngine` documentation to handle the new method.
* 4.7.8
  * Updated `BusinessRule` to include a `getLog()` method to return the `org.slf4j.Logger` so that all classes extending it can use the same logger (helpful when writing in Java and `@Slf4j` isn't available).  Also added it to `input` during `init()` so that it's available from orchestration NCubes as `input.log.info()`.
  * Marked `RulesEngine.execute(Closure, Object, Map, Map)` as deprecated in preparation to replace the `RulesEngine` categories `NCube` to be treated as a `DecisionTable`.
  * Updated `RulesEngine` documentation generation:
    * Code changes:
        * Fixed stack overflow when a rule expression called another rule that had already been processed (added visitor pattern).
        * Fixed issue with reading the Default column of a ruleAxis.
        * Fixed NPE when a rule expression has no code. 
    * UI changes:
        * Added a `rules.css` to define display styles for each piece of rule info.
        * The name of a ruleGroup will now display in a dark blue.  The `BusinessRule` class name will appear next to it in a monospace font.
        * Each NCube rule orchestration name will be displayed in bold.
        * Each rule in a ruleGroup will now display the rule column name followed by the rule condition followed by the `@Documentation` value.  
        * If a rule condition is a primitive boolean, it will display the boolean.  Otherwise, it will display "*Condition*", with a hover tooltip that shows the condition expression as code. 
        * If a rule expression references another ruleGroup, provide the name of the rule column and a "Code" blob with a tooltip to see the code.
        * If a rule expression is not simply a reference to a method on the `BusinessRule` (ie: `input.rule.someMethod()`, the UI will also display the "Code" blob with tooltip.
        * If a rule expression is empty, it will display `[Rule has no content]`.
        * Each ruleGroup will be separated by a line for easier visibility.
  * Updated `DecisionTable`:
    * Renamed `DecisionTable.getOutputColumnNames()` to `DecisionTable.getOutputKeys()` to better match `DecisionTable.getInputKeys()`.
    * Removed `DecisionTable.getRequiredColumnNames()` and `DecisionTable.getInputColumnNames()` as they were redundant.
    * Made `DecisionTable.getOutputKeys()` return a copy to prevent accidental modification.
  * Updated to consume `java-util 1.52.0`
* 4.7.7
  * Updated `RulesEngine` to throw an IllegalArgumentException if it encounters a cell expected to contain an `NCube` name, but for which there is no `NCube`.  Avoids `NullPointerException` when you make a typo.
  * Updated `JsonHttpProxy.requestToken()` to throw a `RuntimeException` if it fails to get a token.
  * Updated `DecisionTable` to allow getting the names of the required columns, input columns, and output columns.
  * Updated `DecisionTable` to more specifically check the metaProperties `required`, `output_value`, and `input_value` for a value of `true` before treating them as "enabled".
  * Updated `DecisionTable` to support multiple inputs being associated to a decision variable.  In this case, all values associated to the input variable must be present in the cell associated to the input variable (or all must fit within the range).
  * Updated to consume `Spring 5.2.6`
  * Updated to consume `Spring boot 2.2.7` 
* 4.7.6
  * `DecisionTable` updated with a new interface allowing you to pass in an Iterable<Map> to `getDecision()`.  Results of each inputMap are merged and returned.  The 'OR' case.
  * `DecisionTable` updated so that if an input_value is not provided for an optional key, it will no longer prevent rows where that column has a value from being included in the results.
  * If you define your `input_value` metaProperty with a `false` value, the `DecisionTable` will ignore it.  Allows you to temporarily disable an input_value for testing.
  * Updated to consume `java-util 1.50.0`
  * Almost all `CaseInsenstiveMaps` converted to use the new `CompactCILinkedMap` or `CompactCIHashMap` from `java-util`.   
* 4.7.5
  * `DecisionTable` updated to allow `null` for values in the `output_value` columns (in addition to `Comparable` values).
  * Updated to consume `java-util 1.43.0`  
* 4.7.4
  * Added `fastutil` as gradle `api` instead of `implementation`.
  * Updated to consume `java-util 1.42.0`
* 4.7.3
  * Updated `JsonHttpProxy` to properly return http connections to the connection pool.
  * Added `NCube.getDecisionTable()` to construct a `DecisionTable`.
* 4.7.2
  * `DecisionTable.getDecision()` - when no input is supplied for optional input, input is automatically assigned a value that will only match blank (wildcard) cells.
  * `NCube.mapReduce()` - when `MAP_REDUCE_SHOULD_EXECUTE` is `false`, defaults are applied (column default, then table, then default argument) as they are normally done in `getCell()`. 
  * Performance: validation `DecisionTable` performance improvements. 
  * `NCube.mapReduce()` - Now works when non-existent names are passed in the `MAP_REDUCE_COLUMNS_TO_SEARCH` or `MAP_REDUCE_COLUMNS_TO_RETURN` options.  Log will show which columns do not exist at `WARN` level.  If the `colAxisName` parameter is not a valid axis, an `IllegalStateException` will be thrown.
* 4.7.1
  * Rules engine modified to enforce that the rule-axis is named "rules" (case insensitive).
  * `GStringWrapper` updated to support 'set property' delegation (it already supported get property delegation).
* 4.7.0 
  * `DecisionTable` officially available. See [Decision Table Developer Guide.](README-decision.md)
  * `DecisionTable` supports multiple discrete and relational (greater than, less than) inputs supported.
  * `DecisionTable` can be validated for overlapping rules - it will indicate any such cases.
  * Bug fix: Bound rule name was not updating on non-executed rules (condition false), which caused re-entrant access `[use()]` to fail. Defect found by: @HonorKnight 
* 4.6.14
  * Ensured `DecisionTable` input keys are returned in the defined order.
  * On `DecistionTable`, `output_columns` can have `data_type` meta-property, ensuring their return type will match, regardless of value stored in the table (ncube).
  * Many more tests added for DecisionTable.
  * `DecisionTable.getRequiredKeys()` API added that returns the required (minimum set) of keys required to call `getDecision()`.
  * Bug fix: `ignore` and `priority` now working correctly on `DecisionTable`.
* 4.6.13
  * Performance: validating `DecisionTable` has been sped up significantly.
  * More validation checks are performed against the `NCube` that the `DecisionTable` is built from.
  * `DecisionTable.getInputKeys()` API added that returns all input keys (columns) to access the `DecisionTable`. 
* 4.6.12
  * Added `DecisionTable` class.  See [Decision Table Developer Guide.](README-decision.md)
  * Test are still being added for DecisionTable, so it is considered 'beta' at this point. 
* 4.6.11
  * Updated `JsonHttpProxy` to add OAuth2 capability
* 4.6.10
  * Updated `RulesEngine` documentation to provide better exception messages and to call new `java-util` `ReflectionUtils.getNonOverloadedMethod()`.
  * Updated to consume `java-util 1.40.0`
  * Updated to consume `json-io 4.12.0`
  * Updated to consume `guava 28.2-jre`
  * Updated to consume `org.apache.ivy 2.5.0`
  * Updated to consume `jackson-databind 2.10.2`
  * Updated to consume `httpclient 4.5.11`
* 4.6.9
  * Failed to release to Maven Central  
* 4.6.8
  * Added Spring OAuth2 authentication support.
  * Updated `RulesEngine` to use `ReflectionUtils.getMethod()` from `java-util`
* 4.6.7
  * Configured NCube's spring beans to only make `NCubePersister` available to profiles that use it: `storage-server`, `combined-server`, `combined-client`.
  * Configured NCube's spring beans to remove `NCubeCacheManager` from `storage-server` profile.
  * Updated to consume `json-command-servlet 1.9.0` (Gradle build version).
  * Added missing `implementation 'org.springframework:spring-web:5.2.2.RELEASE'` which transitively being included from `json-command-servlet`.
  * Updated to consume `Spring Framework 5.2.2`
  * Updated to consume `Spring Boot 2.2.2`    
* 4.6.6
  * Added `BusinessRule.init()` for setting appId, input, output.
* 4.6.5
  * `RulesEngine` now allows each rule group to be declared to throw or not throw an exception if an error is added during that rule groups execution.
  * Bug fix: Groovy/Java doc was not publishing correctly.
* 4.6.4
  * Updated `RulesEngine` to have the option of throwing `RulesException` after each rule group or continuing on and simply accumulating errors.
  * Groovydoc now includes both Groovy and Java sources.
  * Removed unused `spring` / `spring-boot` libraries from Gradle build.
  * `java-util` added to API
  * `json-io` added tp API
  * `trove4j` added to API   
* 4.6.3
  * Made `groovy-all` api not implementation, because `Closure` is part of NCube API.
* 4.6.2
  * Updated build from Maven to Gradle. 
* 4.6.1
  * Enhancement: `LongHashSet` now always keeps its elements in order from lowest to highest. Consider part of API contract.
  * Performance: `LongHashSet` reduced from 32 bytes in memory to 16 bytes.  Removed caching of hash (8 byte savings) and converted from Groovy to Java (another 8 bytes saved).
  * Performance: `LongHashSet` performance further improved.  Keeping elements ordered allows equals to always perform in O(N).  Contains() now uses binary search (log n).
  * Performance: Column reduced from 40 bytes to 32 bytes by converting from Groovy to Java.  Again, because so many instances are created. 
* 4.6.0
  * RulesEngine added.  See [Rule Engine Developer Guide.](README-rules.md)
  * Small reduction in memory used by coordinate keys.
* 4.5.2
  * Reference axis transforms provide more detailed error messages for `addaxis` type when the given appId, n-cube name, or axis name is not found.
  * Using `@Transactional` to manage transactions, no longer using a beans.xml type specification for database transaction boundaries.
  * ApplicationID constructor that takes a string specification (e.g. `tenant/app/version/branch/release`) now performs detailed validation on passed in string.
  * Logging is now exclusively done via Spring's `@Slf4j` throughout entire code base.
  * Eliminated out-dated beans.xml spring configuration by employing `@Configuration` [NCubeConfiguration], `@Transactional`, and only a few `@AutoWire` annotations.
  * Eliminated all uses of `NCubeAppContext.getBean()` within non-test code.
  * Eliminated `NCubeAppContext.containsBean()`
  * Updated to consume `Spring boot 2.2.1.RELEASE`
  * Updated to consume `json-io 4.11.1`  
* 4.5.1
  * Spring Environment Property `ncube.track.bindings` added.  If `true`, all coordinate bindings to a cell will be added to output map (existing behavior).  If `false`, these bindings are not written.  Defaults to `true`.
  * Using groovy-all.jar 2.4.17.
  * Updated to `Spring Boot 2.2.0.RELEASE`
  * Updated to `tomcat 9.0.27` (brought in by spring boot 2.2.0)
* 4.5.0
  * Bug fix: When using `at()`, if the destination cell changed values on the input `Map`, then those changes were reflected on the input `Map` upon return.  Going forward, all calls to `at()`, `go()`, and `use()` all protect input from being modified by the target cell (unpon return) - no side effects to input `Map` keys or values.  Keep in mind, that although the value associated to a key cannot be changed, fields on a structured value-side object could be changed.
  * Bug fix: When compile Groovy cells, the compilation is synchronized on `L2CacheKey` - meaning that all compilation is in parallel except of the exact same cell (SHA-1 of source or SHA-1 of URL).
  * Bug fix: No attempts to re-load a dynamic class can happen now. The compilation synchronization on same code / URL plus check of loaded classes in the `CdnClassLoader` prevent this from happening.  It was benign in `Java 1.8`, but causes JVM crash in `Java 11`.
  * Bug fix: `CommandCellException` is no longer wrapped through each unwinding of the stackframe - meaning shorter log file entries when an exception occurs.  The originating exception is what is wrapped in `CommandCellException`.
  * N-Cube source is now compiling in `Groovy 2.4.17` and `Groovy 2.5.8`.  Small changes were made due to stricter syntax enforcement by `Groovy 2.5.8`.
  * Updated to consume `Groovy 2.4.17`
  * Updated to consume `Springboot 2.1.8.RELEASE` 
  * Updated to consume `Spring 5.1.9.RELEASE`
  * Updated to consume `Guava 28.1-jre`
  * Updated to consume `tomcat 8.5.46`
  * Updated to consume `java-util 1.36.0`
* 4.4.1
  * Updated to consume `Spring 2.1.1.RELEASE` 
  * Updated to consume `Spring 5.1.3.RELEASE`
* 4.4.0
  * Updated to consume `Spring 5.1.2.RELEASE`
* 4.3.3
  * Bug fix: Updated `NCubeController.saveJson()` to allow saving multiple NCubes
  * Update to consume `guava 26.0-jre`
  * Updated pom.xml build plugins  
  * Updated to consume `tomcat 8.5.32`
* 4.3.2
  * Bug fix: Pull requests could not be created for users without commit permissions.
  * Updated to consume `spring-boot 1.5.12.RELEASE`
  * Updated to consume `spring 4.3.16.RELEASE`
  * Updated to consume `tomcat 8.5.31`
* 4.3.1
  * Regexes updated to better locate NCube names in APIs that search for them in source code.  The `use()` method is now searched.  Nested calls are not searched.  Extract nested call to local variable, and then use that value in 2nd call. Relevant only if you care about searching sources for NCube names.
  * Updated to consume `groovy 2.4.15`
  * Updated to consume `spring-boot 1.5.10.RELEASE`
* 4.3.0
  * `NCubeRuntime` now has the option to cache .json files locally of `NCubes` that have been loaded. Subsequent requests will potentially load from the cache, following the options listed below.  If no `application.properties` entry exists for this, nothing is cached.  Otherwise set the key `ncube.cache.snapshotPolicy` equal to one of the values from `SnapshotPolicy` `enum` and set the key `ncube.cache.dir` to the location of where the files should be stored. Valid cache policy options:
     * `RELEASE_ONLY`: prevent caching of snapshots; only cache release versions (default)
     * `OFFLINE`: only use cubes found in file cache; exceptions are thrown for missing cubes
     * `UPDATE`: snapshot cubes are only loaded if not found in the file cache; manual cleanup of cache required
     * `FORCE`: sha1 check is performed before loading cubes from file cache to ensure latest snapshot is used 
* 4.2.5
  * `NCubeManager.getPullRequests()` can now accept a pull request ID to find if it's otherwise outside of the date range.
* 4.2.4
  * `NCube.mapReduce()` - input/output Map when passed in was not getting used if it was empty (Groovy truth - empty map is false - threw off comparison). @gmorefield.
  * `NCube.mapReduce()` - when ran against a rule cube, the row and column axis names need to be added to the tracking map.  @gmorefield.
* 4.2.3
  * Updated `NCube.mapReduce()` to return errors in cells vs ending the entire call when error occurs.
  * Updated `NCube.mapReduce()` to include an option for executing cells while running (default is no execute).
* 4.2.2
  * Updated to consume `json-command-servlet 1.8.3`
  * Updated to consume `spring 4.3.13.RELEASE`
  * Updated to consume `spring-boot 1.5.9.RELEASE`
  * Updated to consume `tomcat 8.5.24`
* 4.2.1
  * Remove old sha1 algorithm and sha1 versioning as it is no longer needed.
  * Bug fix: `NCube.mapReduce()` not properly handling case-sensitive columns.
* 4.2.0
  * Bug fix: Updated sha1 algorithm for an issue with symmetrical cubes. Application property is required to set running sha1 version until we can convert the records in our DB.
* 4.1.30
  * `NCube.mapReduce()` - If input is bound to the axis marked as the rowAxis (on the input map), then the columns on the rows axis will be matched to it - either directly by value (meaning only 0 or 1 row will be returned) or a Collection can be bound to the row axis, in which the only rows considered for `mapReduce()` must also be in the `Collection`.  If nothing is bound to the rowAxis on input, then all rows will be attempted to be matched. Supplied by Greg Morefield.
* 4.1.29
  * `@PostConstruct` added to `NCubeConfiguration` to set compiled cache directories, acceptedDomains, and max stack size string after Spring intialize.  These were getting set too soon in the app start up phase preventing consuming apps from properly using their values.  Fix provided by Greg Morefield.
  * Bug fix: `NCubeRuntime.prepareCube()` was caching the `NCube` before applying advices, allow another thread to pull an `NCube` from the cache before the advices were applied.  Fix provided by Greg Morefield. 
* 4.1.28
  * Removed `String.join()`, which is Java 8 functionality, from `GroovyExpression` and replaced it with Guava's `Joiner` so that consumers using `<classifier>jre7</classifier>` can use NCube.
  * Bug fix: Updated `NCube.createStubCube()` so it doesn't try to delete columns reference axes. Reference axes on "stub NCubes" will now contain all columns. Non-reference axes continue to have no columns.
  * Cleaned up `NCubeJdbcPersister.createCube()`
* 4.1.27
  * `NCube.mapReduce()` allows any number of dimensions (one must act as the 'where' axis).
  * `NCube.mapReduce()` takes new 'options' `Map` where the additional arguments are passed.  
  * Consumed java-util 1.34.0
* 4.1.26
  * Add `NCube.hyperMapReduce` that allows mapReduce with multiple unknown axis values.
  * Add global menu `sys.menu.global` in `sys.app` which provides a template for all apps across the system. Application-specific menus override properties of the global menu.
  * Bug fix: During rule execution, the name of the rule column is placed on input.  When a rule references another cell via `use()`, `at()`, or `go()` this will ensure that the appropriate rule is executed.
* 4.1.25
  * Bug fix: `NCubeRuntime.isCached()` (just added) was giving incorrect results.  Test and fix added.
* 4.1.24
  * Enhancement: `NCube.getCells()` API added to support *Values* mode (default is *Formulas* mode).
  * Enhancement: `NCubeRuntime.isCached()` API added to allow testing whether an n-cube is cached.  Useful in unit tests. 
* 4.1.23
  * Enhancement: `NCube.mapReduce()` now allows a defaultValue to be passed in (similar to `getCell()`), and the execution stack is maintained just like `getCell()`, and axes with default columns do not need to have input bound to them.
  * Moved search logic from `NCubeController` to `NCubeRuntime`.
  * Disallow search closure from NCE server.
  * Storage server can cache for the life of one operation to prevent the same cubes being loaded from reference axes.
  * Bug fix: Fix issues with default cell url and default values on default columns in NCube.fromSimpleJson.
  * Bug fix: Tests were not merged if the branch cube was changed.
  * `create_hid` on database records when copying branches now shows current user instead of copied user id.
  * Accepted domains for the CdnClassLoader is now an application property instead of part of `NCUBE_PARAMS`.
  * Made transitive dependency on logback, optional in pom.xml.  This will stop the dependency from become a dependency to those who consume n-cube as a library.
  * Improved speed when parsing cells
* 4.1.22
  * `NCube.fromSimpleJson(inputStream)` has been sped up by about 33% and uses much less memory when loading an n-cube.
  * Replaced a few `search()` calls with `doCubesExist()`.
  * Added ability to define advice for an app using `sys.advice` cube.
* 4.1.21
  * Bug fix: Return `null` from deprecated `NCubeController` methods `getCubeRawJson()` and `getCubeRawJsonBytes()` when `NCube` does not exist.
  * Added better exception and error message for invalid reference axes and transforms.
  * Added `NCubeController.getUrlContent()` to resolve issues viewing files on the CDN.
  * Sped up `NCube` parsing and formatting.  Technique 1) using casting instead of 'as' when LHS is known, 2) Using Map's of Closures instead of if-else or swtich statments where the decision variable is a String, and 3) Sped up `NCube.ensureFullCoordinate()` by converting code that was performing as X * Y to X + Y iterations. 
* 4.1.20
  * Enhancement: Removed cube data from `NCubeInfoDto` for searches that did not specifically request the `SEARCH_INCLUDE_CUBE_DATA` option.
* 4.1.19
  * Added Java 7 compatible jar. To use as a dependency, add `<classifier>jre7</classifier>`.
  * Bug fix: PR Notes now work for Commit
  * Enhancement: Content search uses less CPU and RAM
  * Enhancement: Added optional limiting clauses to specific SQL queries
* 4.1.17/4.1.18
  * Enhancement: Allow notes on pull requests that are tied to revision history.
  * Enhancement: Allow a `Closure` to be sent as a search option when retrieving records from the database in order to eliminate the need to return to the database to perform additional processing.   
  * Enhancement: APIs that returned `NCube` now return a `Map` with entries like appId, bytes, cubeName, sha1, testData. The old APIs are now `@Deprecated`
  * Enhancement: Added `deleteApp` API only for sys admins to remove apps that have no released version.
  * Pushed work being done in `NCubeController` methods `promoteRevision()`, `fetchJsonRevDiffs()` and `fetchJsonBranchDiffs()` to `NCubeManager`.
  * Removed unnecessary `NCubeJdbcPersister.loadCube()` method in lieu of `search()`
  * Removed unnecessary regex wildcard characters that match JSON.
  * Bug fix: Inline `GroovyExpressions` did not recognize `import static` statements in regex.
* 4.1.16
  * Enhancement: Updated `NCubeRuntime.clearCache()` to take a second argument `Collection<String>` of NCube names which defaults to null. Passing null retains previous functionality, whereas passing NCube names will only evict the specified NCubes from the cache.
  * Consumed `json-command-servlet 1.8.2`
* 4.1.15
  * Enhancement: Added `NCubeManager.isSysAdmin()` to allow global admin permissions.
  * Enhancement: Cache admin/sysAdmin permissions to eliminate frequent calls.
  * Enhancement: SQL queries in `NCubeJdbcPersister` methods `doCubesExist()`, `getAppNames()`, `getVersions()`, and `getBranches()` sped up by matching sys.info cube name exact, not using `LOWER()`. 
  * Updated permissions cache time-to-live (TTL) from 2 to 3 minutes. 
  * Bug fix: Fix `NCubeManager.fastCheckPermissions()` to not load cubes when permissions are already cached.
  * Bug fix: Catch `BranchMergeException` during `NCubeManager.mergePullRequest()`, obsolete the pull request and re-throw the exception.
  * Bug fix: Admin could be cached before test setup permissions cubes properly.
* 4.1.14
  * Enhancement: Improved error handling when comparing a cube against a branch where it does not exist.
  * Enhancement: Added `ApplicationID.asBootVersion()` to avoid repetition.
  * Bug fix: `NCubeManager.mergePullRequest()` handles merge exceptions by obsoleting the pull request within the transaction boundary.
  * Bug fix: Test updated metaproperty was changing even when test data was not updated.
  * Bug fix: `NCubeManager.generatePullRequestHash()` was not using `runSystemRequest()` for one of its calls, forcing a failure when making tx cubes unreadable.
  * Removed all remaining references and support of using transforms with a method axis.
  * Enhancement: Consumed `json-command-servlet 1.8.0` which provides faster response serialization and uses less memory during the process. 
* 4.1.13
  * Enhancement: `JsonHttpProxy` connects the `HttpClient's` inputStream directly to `JsonReader` so that construction of objects from JSON begins immediately as data arrives.
  * Enhancement: Improved performance of `NCubeJdbcPersister.doCubesExist()` by including sys.info in where clause
  * Bug fix: Added `createSysInfoCube()` to `pullToBranch()` in `NCubeJdbcPersister`
* 4.1.12
  * Updated to consume `json-command-servlet 1.7.1`
* 4.1.11
  * Updated to consume `json-command-servlet 1.7.0`
* 4.1.10
  * Removed `LOG` static member from `NCubeGroovyExpression` as it conflicts with existing, derived classes.
* 4.1.9
  * Added `StandardHttpRequestRetryHandler` to `JsonHttpProxy` to handle retrying `HttpNoResponseException` due to client connection being stale.
  * Increased default value from 6 to 100 for client side HTTP connection pool used by `JsonHttpProxy`.
  * To reduce memory usage, removed code that created servlet session.  It was doing this only to track connected users, not store session data.
  * Added `LOG` to `NCubeGroovyExpression`.
  * Bug fix: Release process inhibited by `copyBranch()` not allowing HEAD for target
* 4.1.8
  * NCE timeout to NCUBE updated to wait 420 seconds max, instead of the default 30 seconds.
  * JDBC Query Timeout changed to 300 seconds, up from default of no timeout.
  * `NCubeRuntimeClient.getJson()` now calls the backend `getCubeRawJsonBytes()` API.
  * Updated `ncube-beans.xml` to order parameters of `JsonHttpProxy` correctly.
  * Performance: Remove superfluous persister calls in `mergeCubesIfPossible`
* 4.1.7
  * Performance: `NCubeRuntime.createCube()` / `NCubeRuntime.updateCube()` now pass gzipped `byte[]` to storage server.
* 4.1.6
  * Performance: The `NCube` storage server now ships `NCubes` to the NCE server in compressed `byte[]` form. Reduces CPU load on `NCube` storage servers.
* 4.1.5
  * Enhancement: Added better logging around jdbc connections and more jdbc connection information to `NCubeController.health()`
  * Bug fix: Shallow branch copy failed when cube was changed then changed back in a later revision.
  * Bug fix: Copy branch would not return the correct error message if target branch already existed.
  * Bug fix: `ApplicationID.validateBranchIsNotHead()` did not match lowercase `head`. 
* 4.1.4
  * Switched to using Spring's `FastByteArrayOutputStream` (no synchronized, fast access to internal `byte[]`) instead of JDK's `ByteArrayOutputStream`.
* 4.1.3
  * Consumed `java-util 1.29.0`
  * Consumed `json-command-servlet 1.6.5`
* 4.1.2
  * Enhancement: Added support for compressed HTTP POST data
  * Consumed `json-command-servlet 1.6.4`
* 4.1.1
  * Enhancement: Cube filter search uses regex instead of converting JSON to map.
  * Consumed `json-command-servlet 1.6.3`
* 4.1.0
  * The `NCube` cache in the `NCubeRuntime` environment only caches a cube if it has never been added (in the current session). Before, if a cube was loaded subsequently, the cache entry was updated.  From a runtime client point of view, they should not be working with n-cubes that are changing.  If they need to accept updates or changes, then the client can call the `NCubeRuntime.clearCache()` API.
  * Increased the time the execution thread will wait to obtain the per-cell lock before it can dynamically compile a cell.  Value increased from 10 seconds to 60 seconds.
* 4.0.24
  * Added `use()` API to `NCube` and `NCubeGroovyExpression`.  This API allows the user to point a cell to another cell, which is then fetched, but executed in the input context of the calling cell.  This allows creating reference cells that can be re-used.  Often these are placed on Default columns, or an `NCube` can be created with reference cells in it, and other cells can `use()` these reference cells (again their input will be used when calling the other cell).
  * Bug fix: `getReferenceAxes()` and `updateReferenceAxes()` would fail when cubes could not be loaded due to invalid references.
  * Bug fix: contains search would not return anything if an invalid reference axis was encountered.
* 4.0.23
  * Introduced hidden sys.info n-cube in all n-cube applications. This n-cube will help speed up queries used by `NCubeController` methods `getAppNames()`, `getVersions()` and `getBranches()`. 
* 4.0.22
  * Enhancement: Added reliable method to obtain n-cube and n-cube-editor versions
* 4.0.21
  * Enhancement: Included n-cube-editor version number in `NCubeController.health()`
  * Enhancement: Release cubes and version no longer require a new SNAPSHOT to be made.
  * Enhancement: `NCube` cache within `NCubeRuntime` is now refreshed such that cubes marked with `evict=false` (also `sys.classpath`) will no longer be evicted.  Set `application.properties` entry `ncube.cache.refresh.min=75`, for example, to refresh every 75 minutes.  The interval should be less than the duration (typically about 1/3 of the duration.) 
  * Separated out server stats from `NCubeController.heartBeat()` into `NCubeController.health()`
  * Updated `JsonHttpProxy` constructor to take `org.apache.http.HttpHost` parameter instead of individual hostname, port, scheme parameters in order to construct the `HttpHost`
  * Bug fix: `sys.branch.permissions` not being created when creating branch in sys boot version.
  * Bug fix: A new column would lose any metaproperties associated with it on merge / update.
  * Bug fix: Custom sys.menu would not open properly when viewing a released version.
  * Bug fix: Read default Spring configuration properties correctly
* 4.0.20
  * Bug fix: Updated `NCube.convertExistingAxisToRefAxis()` to not throw NPE when axis and axis to reference do not have any overlapping columns
  * `NCube` stack traces, when listing the coordinate, only show the first 'n' characters of the value (default 1000 - can be overridden in `application.properties`). 
* 4.0.19
  * Consumed `json-command-servlet 1.5.4`
  * Enhancement: Handle non-json responses from REST calls via `JsonHttpProxy`
  * Bug fix: Standardize argument types for permissions checking methods
  * Bug fix: Update `NCube.mapReduce()` to copy input to the execution of the result row fetch 
  * Bug fix: copy branch only records the most recent appId copied from
  * `NCube.mapReduce()` updated to take a `Closure` instead of String for `where` clause.  `Closure` is passed a `Map` of the input and evaluates for each record.
* 4.0.18
  * Enhancement: Create a reference axis from an existing axis
  * Enhancement: mapReduce() - the 'where' parameter (condition test) can be a Closure or a String.
* 4.0.17
  * Added getters for `NCubeRuntimeClient` and `NCubeMutableClient` on `NCubeGroovyExpression`
  * Bug fix: updated property defaults in Spring beans configuration
  * NCube with METAPROPERTY_TEST_DATA now has same SHA-1 as with no test data.
* 4.0.16    
  * Added methods to `NCubeController` that were previously added to runtime and mutable interfaces. Added test to ensure that methods added to interfaces are also added to the controller in the future.
  * Updated `NCubeManager` exception messages with more information
  * Enhancement: Add default, sorted, and fireAll to `addAxis`
  * Test Enhancement: Updated tests to use an embedded Tomcat container for remote urls, replacing `files.cedarsoftware.com`
    * This allows tests to be executed without being connected to a network
    * If the entry `ncube.tests.baseRemoteUrl=http://remote.site.com` is set in `application.properties`, the tests will run against that site instead of the embedded container
  * Bug fix: Merging a pull request with a new cube with an axis reference after other references were updated in HEAD could cause reference version mismatch in HEAD.
  * Performance: Compiling is slightly faster - code that builds class around user's statement block improved.
  * Consumption as a library for a Springboot app simplified.  Overrideable options are easily overridden in the consuming Springboot app's application properties.  All properties start with 'ncube' and modern IDEs will actually suggest available properties when editing the consuming application's `application.properties` file.   
* 4.0.15
  * Bug fix: Updated version fetching code
  * Updated n-cube related application properties to be prefixed with ncube.*
* 4.0.14
  * Added n-cube version to `NCubeController.heartBeat()`
  * Bug fix: Removed "Content-Length" header in `JsonHttpProxy`. Added `ncube.proxy.headersToRemove` application.property in case other headers need to be removed.
  * Bug fix: Removing the last `NCubeTest` on an `NCube` would not save properly.
* 4.0.13
  * L3 Cache support - dynamically compiled script classes are cached in an L3 cache. The L3 cache survives server restarts and allows the JVM to load already compiled classes without requiring the compilation step.
    * Usage: In the `application.properties` file, if the entry `ncube.classes.dir=/x/y/classes` is set, the L3 cache is activated and compiled scripts will be stored here.
    * If the entry `ncube.sources.dir=/x/y/source` is found, then the generated Groovy source will be written to the path specified.
    * In an upcoming release, the L3 CacheManager will overrideable, allowing alternative storage like Redis, SQL, etc.
* 4.0.12
  * Bug fix: Check for `ncubeManager` bean before trying to set userId on it
* 4.0.11
  * Performance fix: removed Upper() from SQL statements on SHA-1 columns.
  * Added `NCube.mapReduce()` API.  Filter rows of an n-cube.  Use this API to fetch a subset of an n-cube, similar to `SQL SELECT`.
* 4.0.10
  * Separated out server.port property. Renamed server.* properties for `JsonHttpProxy` to target.*.
  * Added `NCubeTest` as part of `DeltaProcessor`.
  * Added `loadCubeWithTests` to get both the cube and its tests together.
  * Removed `saveTests` from `NCubeManager`; this is now wrapped into `updateCube` in the persister.
  * Bug fix: Reference Axes not get checked properly on pull request merge vs straight commit branch.
  * Added appId to a couple `InvalidCoordinateExceptions` for more clarity in logs
  * Some system requests override user-based permissions, focused mainly around pull requests.
* 4.0.9
  * Bug fix: Separated `checkPermissions()` and `checkMultiplePermissions()` from overridden API to two distinct APIs.
* 4.0.8
  * Bug fix: no longer need branch permissions to merge a pull request from that branch.
  * Bug fix: check permissions for multiple permissions now only makes one server call.
* 4.0.7
  * Add ability to get and run all tests for an app.
  * Add username to all persister methods.
  * Equivalent primitive values (including Strings and Dates) are folded into one instance (interned) for cells and column values.
* 4.0.6
  * Removed Oracle jdbc driver dependency
  * Removed explicit spring dependencies in pom.xml that are consumed transitively via spring-boot-starter-* dependencies
  * Changed log statements for method calls to include 125 (was 50) characters per argument
  * Changed log level from INFO to DEBUG when sys.menu is not found
  * Enhancement: `saveTests` creates a new db record and uses the testUpdated `NCube` metaproperty.
  * Updated `NCube.duplicate()` to include `ApplicationID`
  * Update `hsqldb 2.4.0` and `mysql 6.0.6` dependencies
* 4.0.5
  * Support mode for json format when getting raw json (json-pretty, json)
  * Update to Spring Boot 1.5.3, Spring 4.3.8, and Groovy 2.4.11
* 4.0.4
  * `NCubeController.commitCube()` was passing an incompatible argument to commitBranch. Fixed bug and added test coverage for commitCube. 
* 4.0.3
  * `NCubeManager.getReferenceAxes()` was incorrectly validating reference versions. 
* 4.0.2
  * Changed `NCubeRuntime.getResourceAsString()` to use Groovy APIs to get stream from Spring Boot executable jar, due to  zipfs execption using regular resource APIs.
* 4.0.1
  * Removed redundant executable flag in pom.xml related to making a spring boot executable jar.
  * Added optional beanName to `NCubeRuntime` constructor
  * Updated pom.xml to create executable jar
  * Bug fix: Removed unnecessary URL encoding in `JsonHttpProxy`  
* 4.0.0
  * Built as a Spring Boot application
  * `NCubeManager` converted from having static APIs to being a Spring injected instance (accessible through interface)
  * Enhancement: Changed way pull request data is accessed for efficiency.
  * Enhancement: Added search options for start and end create date parameters to cut down on cubes searched.
  * Bug fix: `NCubeController` - `promoteRevision` calls `updateCube` directly and `saveJson` loop
  * Bug fix: Update n-cube changed flag during fast-forward based on whether sha1 matches headSha1
* 3.6.17
  * Bug fix: Update n-cube changed flag during fast-forward based on whether sha1 matches headSha1
* 3.6.16
  * Bug fix: Added delta processing for meta-properties on n-cube, axis, and column.
* 3.6.15
  * Bug fix: Batch updating reference axes would remove transforms
* 3.6.13/3.6.14
  * Enhancement: Updated reference axes transforms so they won't execute code on the server (support for transforms that execute code remains)
  * Enhancement: Axis supports a new ValueType (`CISTRING` - case insenstive string)
* 3.6.12
  * Bug fix: Committing merged cube into branch was not resetting the change flag in the case when the n-cube SHA-1 was the same as the HEAD n-cube's SHA-1.
* 3.6.11
  * Bug fix: Duplicate cube was not setting changed flag.
* 3.6.10
  * Bug fix: Merge accept theirs incorrectly setting head_sha1 and changed flag.
* 3.6.9
  * Bug fix: Merge column order delta would overwrite all column changes on the axis.
* 3.6.8
  * Bug fix: Contains search now searches column values for reference axes.
  * Bug fix: Contains search now does not return a result when the text matches only the cube name.
  * Enhancement: Greatly improved speed of getting all reference axes.
  * Enhancement: Added ability for admins of an app to impersonate users to debug user- or group-specific issues.
    * Note: All database transactions use real user for a proper audit trail.
* 3.6.7
  * Enhancement: Easily remove a transform from a reference axis
  * Enhancement: `NCube` serialized through json-io now brings along `ApplicationID`
* 3.6.6
  * Enhancement: Added custom reader and writer for `NCube` when used with json-io.
* 3.6.5
  * Enhancement: Rewrite copy branch in persister to allow a thin copy of branch instead of with full history.
* 3.6.4
  * Enhancement: Create `NCubeConstants`
* 3.6.3
  * Enhancement: Re-ordering of columns on a non-sorted axis are now handled in all cases, API: `DeltaProcessor.getDeltaDescription()` and `NCube.mergeDeltas()`.  Reorder column delta's only show up when there are no outstanding `ADD` or `DELETE` deltas for the same axis.
* 3.6.2
  * Enhancement: Added additional information to the output map - for each call to `ncube.getCell()`, `at()`, or `go()`, if an input coordinate fails to bind to an axis value, and instead bind to the Default column (or completely misses [`CoordianteNotFoundException` case]), the n-cube name, axis name, and input bind-value are added to a list.  This list grows as cells call other cells until it pops-the-stack to the original call.  Use the `RuleInfo.getUnboundAxesList()` and `.getUnboundAxesMap()` to see these values. `RuleInfo.getUnboundAxesList()` shows them in order of occurrence, whereas `.getUnboundAxesMap()` packages this information up by n-cube name and axis name.
* 3.6.1
  * Bug fix: In `VersionControl` - When user modified cube, then deleted it, and there was no change in head, it was being incorrectly marked as a conflict.
  * Bug fix: Fixed bug in self healing code for rule names.
* 3.6.0
  * Columns on a rule axis must have a name.  The name must be unique on the axis.  This is now enforced with an IllegalArgumentException.
  * When a new branch is created, sys.permissions is set to allow UPDATE action by default.  @tpollack 
  * Re-ordering support added for delta/merge.
  * Bug fix: `NCubeManager.updateAxisMetaProperties()` was temporarily not working on a non-reference axis.  Fixed and test added.  @jsynder4 
* 3.5.6
  * Bug fix: When merging updated reference axis, the cells from deleted columns were being attached to the default column if present.
  * Bug fix: When merging reference axis, the column meta-properties were being overwritten from the new column on the updated reference.  Now those properties are brought over, and then the existing meta-properties are overlaid.
  * Bug fix: When merging cells, if the cell was not able to be located by iD (same id on both ncubes), then it was failing to use its old-id to new-id map for locating the cells on the new (transmitting changes) ncube.
* 3.5.5
  * Improved the robustness of `DeltaProcessor.getDelta()`.  Renaming a colum in case only now supported.  @jsnyder4
  * Improved the robustness of `DeltaProcessor.getDeltaDescription()` API.  It handles adding and removing the default column on regular or reference axis, detecting meta-property changes.
  * Improved the robustness of `NCube.mergeDeltas()` API.  It handles applying delta changes in many more cases, and makes attempts to locate target items first by ID, and if not found, by value.
* 3.5.4
  * Enhancement: JDBC statement fetchSize is set before query is executed, and ResultSet uses same value.  Set to 1,000.
* 3.5.3
  * Bug fix: When adding the same reference axis twice or more to an n-cube strange things would happen - due to column IDs not being set differently.  Rare, but sometimes it makes sense to use same reference axis more than once on the same n-cube.
  * Bug fix: Allow the default column to be deleted from a reference axis.
* 3.5.2
  * Enhancement: Rule Orchestration support added.  Rules can orchestrated three (3) new ways in addition to the existing two (*all* or *start with named*).  Note that orchestration is supported for n-cubes with one or more rule axes.  Pass the orchestration argument by associating it to the rule axis name.  In the examples below, the rule axis is named *ruleAxis*
    * Orchestration #1: Example: `.getCell([ruleAxis:['br1', 'br4', 'br7', 'br1'], state:`OH`])` or `.at(...)` will select the named rules in the listed order for execution. The rules are passed in a `Collection<String>` names of the rules.  Each of the selected rules will have their associated conditions run, and if evaluated to `true`, associated statement will be executed.  A rule can be listed more than once and it will execute for each time listed.  The order of the Collection dictates the execution order of the rules.
    * Orchestration #2: Example: `.getCell([ruleAxis: { Axis axis -> List<Columns> columns; ...; return columns}, state:'OH']` will select the rules returned by the passed in `Closure`. The Closure takes one argument, the `Axis`. It is the responsibility of the closure to interact (iterate, filter) the` Axis` for `Column` instances and return those in a `List<Column>`.  A Column can be placed into the List more than once, and in any order.  The returned List is the orchestration order that will be used for executing the selected rules.
    * Orchestration #3: Example: `.getCell([ruleAxis: constraintMap, state:'OH']` will select the columns that have all of the meta-property keys and values from the passed in constraint `Map`.  If the value of a constraint is Axis.DONT_CARE, then only the meta-property key must be present.  The filtered rule set will be executed in the order the rules are listed on the rule axis.  A rule column will be selected once or not at all.  Unlike the other two orchestrations which allow unlimited ordering, and repeating of rules, this option filters the rules by meta-property.
  * Enhancement: Adding axis no longer clears cells.  New axes are added with a Default column. Existing cells are placed in the default column of the new Axis.
  * Enhancement: When deleting a branch, the 0.0.0 branch is also deleted.  Only non-HEAD branches can be deleted.    
  * Added additional information on input.* for reference axis transforms.
    * 'refCube' is a reference to the Cube that contains the referenced axis.
    * 'refAxis' is a reference to the Referenced Axis on the referenced cube
    * 'referencingAxis' is a reference to the referencing Axis (the one being built from the transformation's output.columns Collection
  * AddColumn(Column column) added so that an existing column (e.g. from a Referenced Axis) can be added en masse.  The code will attempt to re-use the existing ID, but if it matches an ID of an existing column, then an ID will be generated from the value of the column.
* 3.5.1
  * Bug fix: NCube.sha1() now takes reference axis meta-properties into account.
  * Bug fix: NCube.hydrateCube() was not converting the value side of reference axis meta-properties from JsonObject to CellInfo.  
* 3.5.0
  * Reference Axis columns can have meta-properties added, removed, etc.
  * Parallel compilation for Groovy expressions.  
  * Deprecated methods removed.
* 3.4.122
  * Refinement: To add meta-properties, just use the standard APIs to add them to the Column instance that represents the default column on an axis.  In the JSON format, they are stored on the Axis meta-properties prefixed with 'd3fault_c0lumn_*'.  However, all APIs, including RESTful APIs expect them to be read and written via the standard meta property APIs.
  * Enhancement: Parallel compilation has been added, and the L3 cache work removed.  There are too many cases when a consumer would have to know when to clear their L3 cache, that it would have been burdensome on the author to answer many questions related to when and why.  The parallel compilation will greatly speed up the 'warm up' phase of the application after a server restart.
* 3.4.121
  * Enhancement: default value can be added to a `Default` column.  To do so, add a meta-property named `d3fault_c0lumn_default_value` to the axis containing the `Default` column to which you want to add a default value.
  * Enhancement: Axis ID's are now written to the JSON formats.  If they are not present, the `Axis` ids are number 1 through number of axes, in order.
* 3.4.120
  * Enhancement: When `NCUBE_PARAM.tempDir` is not set, the L3 cache is stored in memory.  With this change, developers who have not yet set their `tempDir`, won't have a bunch of .class files in an unknown location.
* 3.4.119
  * Bug fix: all compile code paths setRunnableCode().
* 3.4.118
  * L3 Cache implementation complete.  Dynamically compiled code within n-cube cells is now written out to a temp folder and re-loaded (instead of being recompiled).  Set the `NCUBE_PARAMS.tempDir` value to where you would like the compiled .class files to go.  If not set, they will be placed in the system property 'java.io.tmpdir'.
* 3.4.117
  * Enhancement: When updating an n-cube, if the update makes the n-cube's SHA-1 match the SHA-1 of the HEAD n-cube it was based on, reset the dirty (changed) flag to 0 (not changed).  If someone manually edits an n-cube to get back to the same state it was when it was pulled from HEAD, it will reset the dirty flag - same as modern IDEs do when you edit the file and it matches it's original state.
  * Enhancement: When updating your branch from HEAD, if an n-cube in your branch is fast-forwarded (meaning that it now matches HEAD because of a change someone else committed to HEAD), the changed flag on the n-cube in your branch is cleared.
* 3.4.116
  * Remove L3 cache (turn it off until it is completed)
  * Inside `NCubeManger.getUrlClassLoader()`, duplicate the input coordinate before calling `.getCell()` on the `sys.classpath.cube`. Also, pass a dummy output Map.  This way, `userId` and `env_level` do not end up on utilized scope.
* 3.4.115
  * Bug fix: DeltaProcessor was not merging columns of other axes, when there was a reference axis.
  * Bug fix: DeltaProcessor was incorrectly reporting a difference when there was a reference axis present with no changes.
* 3.4.114
  * tag matching more robust for Strings and other data types
* 3.4.113
  * tag processing to support CellInfo 
* 3.4.112
  * Enhancement: InvalidateCoordinateException added (it is a subclass of IllegalArgumentException and is thrown when a coordinate does not have all of the required axes).  The exception contains fields to indicate which ncube and what is missing.
  * Dynamically loaded classes are compiled and cached.  More work to come - specify location of cache. 
  * `NCubeManager.search()` now allows the returned `NCubeInfoDto` list to be filtered by a set of tags.  `NCubes` that specify the meta-property `cube_tags`, which is a comma and/or space separated list of tags, will be matched against a list of tags passed in the `search()` options. The `NCubeManager.search()` option `NCubeManager.SEARCH_FILTER_INCLUDE` key is mapped to a comma and/or space separated list of tags.  NCubes that have these tags will be returned.  If no tags are specified for the include option, then all NCubes are included (other filtering will still be applied). Also, `NCubeManager.SEARCH_FILTER_EXCLUDE` can be set as well. Any `NCubes` that have a tag that is on this list, will not be returned.  The exclude filter will override the include filter. In addition to using a space or comma separated list to specify tags, tags can also be specified a `Collection` of `Strings`.
 
    ```groovy 
    Map options = [:]
    options[(NCubeManager.SEARCH_FILTER_INCLUDE)] = 'foo bar, baz'
    options[(NCubeManager.SEARCH_FILTER_EXCLUDE)] = 'qux'
    
    // Only cubes with the `cube_tags` including `foo`, `bar`, or `baz` will be 
    // returned, however, if the cube has the tag `qux` it will not be returned.
    NCubeManager.search(appId, null, null, options)
      
    // The search could be further refined by including a cube name
    // pattern, as well as a 'contains' pattern.  In this case, all
    // criteria must be matched for the NCubeInfoDto to be returned.
    ```
        
* 3.4.111
  * Bug fix: when only the cache flag is changed on a cell, it should show up as a cell delta (difference).
* 3.4.110
  * Bug fix: when loading by SHA-1, the version and status should not be ANDed in (a CUBE with the same SHA-1 may not changing as versions increase.)
* 3.4.109
  * Bug fix: when merging from another user's branch and 'accepting their changes' over yours, the 'changed' flag in your branch was not being set to their changed flag value.  This issue manifests as that file not showing up in your commit box when committed to HEAD.
* 3.4.108
  * Enhancement: All dynamically compiled classes (`GroovyExpression` cells) are now cached in the temp folder under /target/SHA-1.class. The SHA-1 value is derived from the groovy source code, plus the JVM target version, plus a true/false indicating if the code was compiled in debug (true) or not (false).
  * Use the `NCUBE_TARGET_JVM_VERSION` environment variable to set the JVM version (e.g., "1.8" or "1.7").  Default is 1.8
  * Use the `NCUBE_CODEGEN_DEBUG` environment variable to instruct compilation in debug or non-debug mode. Use "true" for debug, "false" for non-debug.  Default is false. 
* 3.4.107
  * Bug fix: Merging axis columns when there was a Default Column, failed.
* 3.4.106
  * Bug fix: Code that locates 'rollback' revision now locates across versions / releases.  
  * Enhancement: Date comparison used in locating 'rollback' revision needed to be 'less than or equal' not 'equal' because the test cases could run so fast that a millisecond time change did not occur causing a false failure.
* 3.4.105
  * Enhancement: Newly added columns added to the same axis (in the n-cube-editor) will have unique ids.  This improves n-cube merging.
  * Bug fix: displayOrder was not being set correctly on newly added columns.
* 3.4.104
  * Enhancement: `NCubeJdbcPersister.mergeAcceptTheirs()` now handles both HEAD and non-HEAD branches.
  * Bug fix: `VersionControl.mergeCubesIfPossible()` now correctly looks up HEAD cube SHA-1, as opposed to incorrectly looking in the branch.
* 3.4.103
  * Performance improvement: Further improve performance of permissions checks.
* 3.4.102
  * Performance improvement: Sped up performance of `NCubeManager.getCube()` (because it can be called so frequently.)
* 3.4.101
  * Bug fix: when merging n-cubes, it was looking in the branch, not HEAD branch, for finding the base HEAD n-cube (computing the HEAD delta).
* 3.4.100
  * Bug fix: Support for updating branch from a single HEAD cube.
* 3.4.99
  * `NCubeManager.clearCache()` now clears the permissions cache.
  * Bug fix in update branch code.
* 3.4.98
  * Significant improvement to the version control APIs.  APIs exist for getting changes between a branch and HEAD, HEAD and branch, as well as branch to branch.
  * Performance improvement: Permission checks are now performed more quickly (cached short-lived per user).
* 3.4.97
  * `CdnClassLoader` supports Grapes (dynamic class loader).  White-list of accepted domains must be specified (via `CdnClassLoader` constructor or environment variable `NCUBE_ACCEPTED_DOMAINS`).
  * When a cell is set to `null` (meaning it exists with 'key' for it, but the associated value is null), the JSON version of that looks like `{"type":"string", "value": null}` as opposed to an empty cell which is `{"type":null, "value":null}`
  * Advice that matches an n-cube that has no method specified, will match the run() method, allowing support for Advice around expressions.
* 3.4.96
  * `CdnClassLoader` without Grapes support, but with resource cache (relative to full qualified URL map).
  * Uses Groovy 2.4.7
* 3.4.95
  * `CdnClassLoader` supports dynamic class loading (Groovy Grapes).
  * Uses Groovy 2.4.7
* 3.4.94 
  * `CdnClassLoader` same as in 3.4.93, plus `@CompileStatic` added, and `findClass()` always throws `ClassNotFoundException(name)`.
  * Gauva imports removed from `GroovyExpression`'s 'wrapper'
  * Uses Groovy 2.4.3
* 3.4.93 
  * `CdnClassLoader` restored to version from back in Feb 2016.  This fixes performance issues, but prevents code from using Groovy Grapes (`@Grab`).
* 3.4.92
  * `CdnClassLoader` looks first in local cache / parent class loader, and if not found, then does a super.findClass().  It was spamming the network for local classes when a URL was in the classpath.
* 3.4.91
  * NCubeManager.updateBranch(appId, Object[] cubeNames, String sourceBranch) added.  This API allows a subset (cubeNames) to be updated from HEAD (or source branch - not yet supported, but coming soon)
  * NCubeManager.updateBranch(appId) is also available, in which case it acts as before, all cubes that can be updated, will be.
  * NCubeManager.updateBranch(...) returns adds, deletes, updates, merges, and conflicts.  Before adds, updates, and deletes were all considered 'updates'.
  * `CdnClassLoader` only looks for classes remotely, if and only if, the system property (or environment variable) NCUBE_GRAPES_SUPPORT=true.   
* 3.4.90
  * NCubeManager.getHeadChangesForBranch() added to allow fetching the 'update' changes from HEAD (only fetches the change list, does not auto-merge them).
* 3.4.89
  * Meta-properties on columns now 'pull through' on Axis reference.
  * HtmlFormatter differentiates null as a cell value from an empty cell.
  * NCubeJdbcPersister now retrieves full revision history when branch is HEAD (all versions).
* 3.4.88
  * Packaging of JavaDoc into javadoc.jar not groovydoc.jar
* 3.4.87
  * Minor tweak [removed one use of LOWER()]to main SELECT statement in the NCubePersister which fetches n-cubes matching various patterns.
* 3.4.86
  * Rename cube - now allows name to be only changed by case.
  * Bug fix: Meta-property values that have the URL or CACHE flag set to true now retain the setting.
* 3.4.85
  * The value-side of meta-properties on n-cube, axis, or column can now be any Java primitive type, String, Date, BigDecimal, BigInteger, Point2D, Point3D, LatLon, byte[], or any CommandCell     
* 3.4.84
  * A default cell value can be specified by a column meta-property ('default_value').  If columns on different axes specify a default value, then the value at the intersection is the same as the column default if the intersecting columns have the same value, otherwise the n-cube level default is returned.
  * The priority for cell value is 1) specified cell value, 2) column-level default, 3) n-cube default, 4) passed in default value to `at()`, `getCell()`.
* 3.4.83
  * `CdnClassLoader` updated to allow Groovy Grape annotations to fetch external content.
  * `CdnClassLoader` caches resource URLs (relative URL Strings that were expanded to fully qualified URLs against the classpath).  These are cleared when the class loader cache is cleared.
  * `CdnClassLoader` caches classes (dynamically loaded classes - using Groovy's @Grab as well as failed attempts [ClassNotFoundException]).  These are cleared when the class loader cache is cleared.
  * `CdnClassLoader` cache is per `ApplicationID`, therefore it's internal caches are non-static (NCube uses a classpath per ApplicationID - allowing for variations in the same class between versions - multi-tenant support).
* 3.4.82
  * Content search now supports wildcards.
  * Grape annotations extracted from inline Groovy to outside class definition.  This is to support Groovy's @Grab (dependency support). Additional changes forthcoming.
* 3.4.81
  * Fixed column ambiguously defined issue with updated search query.
* 3.4.80
  * Improved search query for SQL persister (uses left outer join instead of sub-select.  Sub-select was causing flip-flopping execution plan with Oracle query optimizer).
* 3.4.79
  * Converted remaining .java files to .groovy
* 3.4.78 
  * Transaction ID shared across persister APIs when necessary to bundle persister calls into same transaction.
  * Removed code related to generating multiple n-cube tests at once.
  * Converted NCubeTest code to Groovy
  * Removed StringValuePair class. NCubeTests now use CaseInsensitiveMap instead of Array of Map Entries.
* 3.4.76
  * Transaction ID added to all batch queries (Commit, Rollback, Delete, Restore, Update).  The ID is a unique ID that is added to the notes of each n-cube involved in the transaction.  This allows searching for all n-cubes with the same transaction ID.
* 3.4.75
  * Many performance optimizations
* 3.4.74
  * getCube(cubeName) API available to expression cells (inherited from NCubeGroovyExpression) now allows an optional 2nd argument whether the API should return null or throw an exception if the named cube does not exist.
  * Reduced the amount of memory used to store the Columns on an Axis.
* 3.4.73
  * 'at()' and 'go()' added to Regular expressions that scan for n-cube names
  * Used Groovy default arguments to eliminate multi-argument overloaded methods
  * Added API ncube.getNumPotentialCells() which returns the maximum number of cells in the hyperspace.
  * Reduced the definition of regular expressions to a shorter Groovy-style
  * Added many unit tests
* 3.4.72
  * Changed the internal storage of n-cube's cell keys to reduce their storage requirements.  Memory consumption by cell keys reduced by half.
  * Groovified NCube (from .java to .groovy) [@CompileStatic used to maintain full execution speed]
* 3.4.71
  * NCubeManager.copyBranch() added - copy from one ApplicationID to another ApplicationID (all cubes).  The target branch is always copied to in SNAPSHOT mode.  The target branch must be empty.
  * NCubeManager.getBranchCount() added.  This API will ask the persister directly how many n-cubes are in a particular branch.
  * CellInfo as Map - Converts a CellInfo into a Map instance
* 3.4.70
  * Performance enhancement: Permissions checks are faster
  * Persister logging showing method names and key arguments
* 3.4.69
  * Performance enhancement: Duplicate cube does not test for need of creating permission cubes if source and target ApplicationID are the same
  * Log persister APIs and times to correlate to REST calls (1st cut)
* 3.4.68
  * Performance enhancement: Only create cube and duplicate cube need to possibly create permissions cubes.  This check was being done on all updates, when it only should be done on create / duplicate.
* 3.4.67
  * Moved lock check code into separate call from assertPermissions.
* 3.4.66 
  * Performance enhancement: Ensure that sys.lock is never checked inside loop (lockBy value is obtained before any loops).
* 3.4.65
  * Simplified permissions checks
  * Changed NCubeManager cache to only cache NCube as opposed to NCube or NCubeInfoDto.
  * Changed NCubeManager cache - hid internals of .toLowerCase()
  * SQL quries always use LOWER() on n_cube_nm (used to only be Oracle)
  * Ensured that sys.lock is never cached
* 3.4.64
  * NCubeManager.releaseVersion(), check for SNAPSHOT versions removed (SNAPSHOT version will exist when this is called.)  Release check is still made.  
  * Delay removed.
* 3.4.63
  * tenant_cd compared with equals (=) and RPAD().  In a future release, the RPAD comparison will be dropped.
  * moveBranch() and releaseVersion() added to NCubeManager so that the release process can be done iteratively (looping through branches before calling releaseVersion() to prevent massive database update).
* 3.4.62
  * Fixed release cubes to NOT set the SHA1 to null on the HEAD branch
  * CREATE_DT needs to always be now.
* 3.4.59
  * Trim whitespace from userId in NCubeManager, because HTTP header source (or other sources) may surprise you with a trailing space, causing permissions lookups to fail.
* 3.4.49-58
  * Deubgging versions
* 3.4.48
  * Revert releaseCubes() / moveBranch() changes
* 3.4.47
  * Bug fix: Merging n-cubes with reference axes, need to handle both head to branch and branch to head.  More tests added.
* 3.4.46
  * Bug fix: Merging n-cubes with reference axes was not working correctly.  The HEAD cube's reference axis was not getting updated with the updated reference version.  Also, the 'sorted/not sorted' setting was not being auto-merged. 
* 3.4.45
  * Admin permissions are required to call clearCache().
* 3.4.44
  * Bug fix: Updating the version number of a transform cube was throwing an error.
  * Added check to Batch Update Reference Axis so that n-cubes that have been hand edited to an incorrect state, will be ignored (but logged).
* 3.4.43
  * Additional check added to NCubeManaer.releaseCubes() to ensure that the new version does not already exist.
  * NCubeJdbcPersister updated to use triple double quotes so that ${methodCall()} can be used instead of using string concatenation (+)
  * bug fix: duplicateCube() now auto-creates the persmissions cubes if the new cube causes a new app to be created.
* 3.4.42
  * During release of an Application, a sys.lock n-cube is add/updated to indicate that the release process is running and all mutable operations are blocked by it. 
* 3.4.41
  * bug fix: NCubeManager's constants were inadvertently made private when it was converted from Java to Groovy.  They are now public.
* 3.4.40
  * Added getMap() API that takes an output Map and default value.
* 3.4.39
  * Added branch permission support.  Users can elect to allow others read/update access to the n-cubes in their branch.
* 3.4.38
  * Added permissions checks to NCubeManager API.  If the `sys.usergroups` and `sys.permissions` n-cubes exist in an application, they will be consulted to determine whether or not the current user can execute the given API.  In addition, lists of cubes will be filtered based on these permissions.
* 3.4.37
  * Bug fix: Axis.updateColumns() should not allow duplicates - 2nd way found and fixed.
* 3.4.36
  * Bug fix: Axis.updateColumns() should not allow duplicates.
* 3.4.35
  * Bug fix: NCubeManager.updateReferenceAxes() must use RELEASE / HEAD as the status / branch. 
* 3.4.34
  * Added NCubeManager.updateReferenceAxes() which allows batch updating reference axes.
  * Extracted n-cube schema from JUnit test to it's own .sql file. 
* 3.4.33
  * Added NCubeManager.getReferenceAxes(appId), which returns all the NCube/Axis pairings as List<AxisRef>.  AxisRef contains the source (pointing) appId, cube name, and axis name, as well as the target app, version, cube name, and axis name, plus the optional transformation reference info.
* 3.4.32
  * New at() and go() APIs available on the NCubeGroovyExpression (cell).  These allow you to target n-cubes in other applications without referencing the NCubeManager.
  * Synchronization lifted for URL resource fetching (the serialization portion).  The resolution of the URL (relative to absolute) remains synchronized per URL String interned, e.g. parallel on different URLs, serial on a given URL.  
* 3.4.31
  * mergeAcceptMine() and mergeAcceptTheirs() can now process an array of cubes.  Makes it easy for front ends to allow user to merge a bunch of cubes at once.
  * Updated to use json-io 4.4.0.
* 3.4.30
  * Updated to pick up new version of java-util (1.20.5).
* 3.4.29
  * Input key tracking now available.  After a call to .getCell(), fetch the RuleInfo from the output Map and call ruleInfo.getInputKeysUsed() and it will return all the scope keys (Strings) that were referenced with either a .get() or a .containsKey().  This includes keys referenced from conditions on a Rule axis as well as meta-properties that are expressions.
* 3.4.28
  * Updated: removed NCubePersister.getAppVersions() - use getVersions()
  * bug fix: GroovyMethod cells were returning always as cacheable = true
* 3.4.27
  * Updated: NCubeManager.getAppVersions() only looks at app and tenant
  * Updated: NCubeManager.getVersions() only looks at app and tenant
* 3.4.26
  * Added NCubeManager.getBranches(appId), which looks at Tenant, App, Version, and Status to filter branch list.
  * Added NCubeManager.getVersions(String app) which returns a Map with two keys, 'SNAPSHOT' associated to List<String> of all SNAPSHOT version numbers, and 'RELEASE' associated to a List<String> of all RELEASE version numbers.
  * Changed NCubeManage.getAppNames(tenant) which returns all application names for the given tenant.
  * Future: NCubeManager.getAppVersions() will likely be dropped. 
* 3.4.25
  * bug fix: Axis.updateColumns() was dropping the Default column (not re-indexing it), causing cells associated to the default column to be orphaned.
* 3.4.24
  * When an n-cube is loaded, orphaned cells are now logged when found, but no longer throw an exception.  A cell can become orphaned if the Reference `Axis` it points to were to be modified.  Next version will not allow reference axis to non-RELEASE version and this becomes a mute point.
  * Enhancement: `ncube.breakAxisReference()`.  This API breaks the reference and effectively copies columns of the referenced axis to the referring axis.  
  * bug fix: The `CdnDefaultHandler` now ensures that it is not adding the same value more than once to the same axis.  Before, an `AxisOverlapException` was being thrown if more than one thread went after the same resource at the same time.  The blocked threads would still attempt to add the column to the axis.
  * bug fix: Situation where deadlock could occur between the `synchronized(this)` in `UrlCommandCell` and the `synchronized(GroovyBase.class)` in the `GroovyBase` compile.  The synchronization around compile is now tightened up against just the compile (parseClass) method, which does not re-enter any of n-cube code.  Further, the synchronization that was done at the execute() level in `UrlCommandCell` has been removed, and instead the synchronization is now around the URL resource resolution and resource fetching (using the String URL as the lock), the only places where dead-lock was occurring.
* 3.4.23
  * Enhancement: Reference Axis support added. Now, an axis can be defined once in a single cube (for example, all US states), and then that axis can be re-used on other n-cubes without redefining the axis (nor duplicating the storage).  Changing the original axis, will change all references.  Very use for 'availability' matrices (e.g. "which products are available by state"), managing 'exclusivity' between items ("which items work with each other"), etc. 
* 3.4.22
  * Enhancement: All axis types now load, delete, and update in `O(1)` or `O(Log n)`. They find within `O(1)` or `(O Log n)` excluding RULE cubes, where the conditions are linearly executed.  `Date` and `Number` axisValueTypes on `NEAREST` access in `O(Log n)`, the others are linear.
  * Enhancement: The n-cube merge process (`DeltaProcessor`) now handles more cases by locating columns by value, not ID (except with a RULE column that has no name).
  * For code executing in a 'exp' cell, use `at()` to pass on the current input and `go()` to start with a new map.  Both take a `Map`, but `at()` starts with the current input, whereas `go()` requires it to be fully built-up. `at()` is normally the API you want to fetch content from (at) another cell.
  * bug fix: When using a `GroovyTemplate`, if your code running within the template attempted to access a relative URL, it was not using the classpath from `sys.classpath` to anchor it.  Now, it uses the sys.classpath defined `CdnClassLoader` to anchor it.
  * `NCubeGroovyExpression` and `NCubeTemplateClosures.groovy` have two new APIs on them to allow quick and easy access to fetching content from a URL.  The `sys.classpath` entries will be used if these are relative.
  * Guava added as a dependency to the pom.xml file.
* 3.4.21
  * Renamed APIs added in 3.4.20 from 'at' to 'go'.
* 3.4.20
  * Within an n-cube `GroovyExpression` cell, at(Map coord, String cubeName [default this cube], Object defaultValue [default null]) can be used to reference another cell in same or another cube.  Use `at()` as `runRuleCube()`, `getRelativeCubeCell()`, and `getRelativeCell()` will be deprecated.
  * Within an n-cube `GroovyExpression` cell, atCoord(Map coord, String cubeName [default this cube], Object defaultValue [default null]) can be used to reference another cell in same or another cube.  Use `atCoord()` as `getFixedCell()` and `getFixedCubeCell()` will be deprecated.
* 3.4.19
  * Enhancement: `getCell()` now takes an optional parameter of defaultValue.  This is an additional way to provide a default value for when there is not cell at the passed in coordinate.
  * `getRequiredScope()` updated to take input and output Maps.  Because required scope can be an expression, this allows additional scope to be supplied to the expression.
  * `getOptionalScope()` updated to take input and output Maps.  Because optional scope can be an expression, this allows additional scope to be supplied to the expression.
* 3.4.18
  * `NCube.updateColumns()` now takes a String Axis Name as a `Collection` of Columns, rather than accepting an `Axis` which was not expected to be in proper order.  Simimlarly, the `Axis.updateColumns()` API now takes a `Collection` of `Columns`.
  * Continued work on Axis in support of a Reference Axis option.
* 3.4.17
  * Delta processing methods moved from `NCube` to `DeltaProcessor` class.  Use the static public methods on `DeltaProcessor` like this: `DeltaProcessor.getDelta(ncube1, ncube2)`, etc. 
* 3.4.16
  * Bug fix: `getTestDate()` failing.  Need to use JDBC-style API to access test_data_bin instead of pure Groovy approach of acecss (row.getBytes() not row['test_data_bin']) 
* 3.4.15
  * Enhancement: Cubes can now be merged from one branch to another (before merges were only between HEAD and branch).
  * Changed persister to use 'sub-select' statements instead of 'left outer joins' when locating highest revision number cube.  Seeing which of the two approaches is faster in a product environment.
* 3.4.14
  * Finalized test build-out in preparation for 3.5.0 release
  * Enhancement: Default columns now merge in (and out) along with cells associated to them.
* 3.4.13
  * The rest of the column merge tests added.
  * Added check to ensure that an axis with a duplicate ID cannot be added to the same n-cube.
  * Removed deprecated 4-argument `ApplicationID` constructor (it existed before there was branch support).
  * Bug fix: When merging rule axis and there was a column update (condition changed), it was merging it as a remove.
* 3.4.12
  * Bug fix: Merging columns between two cubes where they both added the same column value and had non-conflicting cell changes was getting reported as a merge conflict.  This is fixed.
  * More merge tests added, still more to come.
* 3.4.11
  * All SQL queries now include the originating method name in a SQL comment before the query text.  Helpful for performance monitoring.
  * `NCubeJdbcPersisterAdaptor` moved to Groovy and updated to use lambda function.  This removed all the boiler plate code and makes the Adaptor code much smaller.
  * Binding class updated to provide public API access to ALL of its fields including depth and value.
  * Added another test for merging columns.  Many more tests to come.
* 3.4.10
  * Increased robustness of colum merge.  Rule axes use rule name for locating rules and fall back to ID.
  * Uniqueness of rule names enforced - per axis.
* 3.4.9
  * Update / Commit branch - further increased instanes where automatic merge can be done (better rule axis support).
* 3.4.8
  * Update / Commit branch - increased instances where automatic merge can be done, including columns being added, deleted, or changed.  
  * Bug fix: When many threads accessed an expression cell at the same time, each thread was compiling the same code after it had its 'turn', rather than the first one getting it compiled, and then later threads picking up the compiled code.
  * Bug fix: When an expression cell is executed, the constructor and 'run()' method objects were being cached.  By changing the code not to cache these reflective objects, it eliminated an issue where a class cast exception was occurring on the same class (but loaded by different class loaders).  This was caused by a combination of different threads compiling a class, and the 'inflation-based' accessors that Java creates for reflective methods after 15 calls. 
* 3.4.7
  * Bug fix: `updateNotes()` and `updateTestData()` used SQL supported by HyperSonic and Oracle, but not MySQL.  Reverted to prior technique (2 queries - one to find max, one to update).
* 3.4.6
  * Added retry logic to URL resolution (tries twice - logs warning on each failed attempt).  If fails both times, an error is marked in the cell and it will not be tried again (Malformed URL, `sys.classpath` issue, etc).  
  * Added retry logic to URL content fetching (tries twice - logs warning on each failed attempt).  If it fails both times, future attempts at fetching will still be tried.
  * Added new JSON format that places the axes, columns, and cells in the JSON in a way that axes, columns, and cells are directly accessible as String keys in a `Map` - O(1).  Useful for sending to Javascript clients for quick access to the n-cube content.
  * Bug fix: Upating axis URL was not causing cube to be updated in database as SHA-1 was not being cleared.
  * Bug fix: `Column` order changes were not included in the SHA-1, causing no save to happen when only columns were re-ordered.
  * Bug fix: Parsing SET columns was failing in many cases.  JSON Format is used for `SET` columns as it is robust.  Requires `String` and `Date` variables to be quoted.  Instructions in NCE have been updated.
* 3.4.5
  * Bug fix on regex that parses import statements.  Import statements that were on the same line as code (code after the import statement) did not work.
* 3.4.4
  * `NCubeJdbcPersister` now supports batching for many APIs, including using batching via prefetch as well as SQL Update batching.  Completely re-written in Groovy to take advantage of Groovy's marvelous SQL support.  No checked exceptions reduces need for all the explicit SQL Exception handlers.  These are caught as runtime exceptions later.
  * `NCubeJdbcPersister` now works entirely with Lists of items instead of single items, where it makes sense.  This allowed batching support to be added for Delete, Restore, and Rollback.  Batching support added elsewhere in terms of SQL prefetch.
* 3.4.3
  * The `NCubeJdbcPersister` now allows for a `ConnectionProvider` that returns a null connection.  This can happen in a simple file-based persister, for example.
  * `NCubeManager.loadCube(NCubeInfoDto)` loaded the cube by id (parameter overkill).  The API is now `NCubeManager.loadCubeById(long id)`.  Useful for getting a specific revision of a cube.  User picks from revision list, code then loads the revision cube by ID.
* 3.4.2
  * Removed use of JDK 1.8 specific API to maintain 1.7 compatibility.  
  * Removed redundant API from NCubeManager (getCubeRecordsFromDatabase - use search() instead).
  * Fixed issue where cast of ClassLoad to GroovyClassLoader needed to be instanceof checked to prevent ClassCastException.
* 3.4.1
  * Improved exception handling of n-cube. When an exception is known (e.g. `CoordinateNotFound`) it is thrown as is. All unknown exceptions are caught, the n-cube call stack is added, and then it is rethrown in `CommandCellException`.  Call `.getCause()` on this exception to determine underlying exception.
  * All `CommandCell` related code and `CellInfo` converted from Java to Groovy.
  * Moved `recreate()` and `getType()` methods from `CellTypes` to `CellInfo`, and remove the CellTypes enumeration.
* 3.4.0
  * Added ability for n-cube Applications to define import list to make available to expression 'exp' cells / columns.  Add desired import classes to sys.prototype cube.  The cube has at least one axis (sys.property) and the column sys.imports.  The cell at this location returns a List of import classes or pacakges (do not include the 'import' keyword) and these packages will be added to the source of the compiled cell.
  * Added ability for n-cube Applications to define the class that expression cells inherit from (currently must be subclass of NCubeGroovyExpression).  Methods added to this class can be called in your source, allowing reduced source code size.  Add desired class name to sys.prototype cube.  On the sys.property axis, add a column sys.class and in the associated cell, place the String name of the class.  You can fully qualify the name, or use the shorter class name and add the package name to the import list (above bullet point).  NOTE: The class must be defined in the sys.classpath.
  * Added `NCubeManager.loadCube(appId, cubeName)` API which loads the cube from the persister (bypassing the cache).  It will cache the cube, but all calls to `loadCube()` will always use the persister to load.
  * Fixed issue where `NCubeManager.updateBranch()` was not skipped classes that you updated, but where not updated in the HEAD branch.  Nuisance issue, as it created additional revision of your modified cubes each time you ran update branch.
* 3.3.13
  * Update the 'UpdateBranch' code so that it handles the situation where the branch cube's SHA-1 matches the HEAD cube's SHA-1, yet the branch cube has a HEAD SHA-1 that no longer matches the HEAD.  This case can happen cubes are inserted into a branch directly in the database without going through the NCubeManager, leaving the headSha1 field null on that branch.  UpdateBranch detects this and Fast-Forwards the branch by simply updating the HEAD SHA-1 on the branch to be the same as the HEAD SHA-1.
  * Restore cube now adds the restored cube to the cache.  This may change in the future, as the n-cube-editor should not really be using the NCubeManager's cache, only a runtime n-cube instance should. 
* 3.3.12 
  * Removed looping-style APIs from persister (it should be focused on atomic changes) and moved that logic to the Manager so that all persisters leverage that code. These were the updateBranch (now updateCube), commitBranch (commitCube), rollbackCube (now rollbackCube).
  * Simplified coordinate names for columns that have a 'name' meta-property as they appear in a 'delta' difference description.
  * Made `NCube.getCoordinateFromColumnIds()` public.
  * Made some read-only getter APIs on `ApplicationID` public.
* 3.3.11
  * Bug fix: NPE was occurring when attempting to check two n-cubes for conflicts and the delta was null (non-comparable cubes).
* 3.3.10
  * Bug fix: Created, deleted and then restored cubes were incorrectly showing as conflicts, when there was no original head cube.
  * Bug fix: When choosing 'Accept Mine' in merge conflict resolution, the code was incorrectly pushing the branch cube to head.  Instead, it should have copied the head SHA1 to the branch so that it was updated for commit-overwrite on future commit.
  * Bug fix: NPE occurring when `Groovy Templates` (with empty content) were being scanned for cube-name references.  This happened during searching the cell content for cube references. Fixed.
  * Bug fix: NPE when computing SHA1 of column that had null value.  Granted, only the Default column should ever have a null, but to prevent NPE, null is converted to "" before .toString() for SHA1.
* 3.3.9
  * Bug fix: `HtmlFormatter` could throw an NPE when encoding the content of a cell.
* 3.3.8
  * Enhancement: Update Branch - no longer throws a BranchMergeException when there is a conflict.  Instead a Map is returned with keys of 'updates', 'merges', and 'conflicts'.  The 'updates' and 'merges' are now committed.  The conflicts are not committed and will therefore show up each time you run Update Branch until each one is resolved.
  * Enhancement: Rule condition parsing improved so that a condition on an 'expression' Axis can be passed in with the notation url|http://foo.bar.baxz.  The 'url' prefix is recognized and used to indicate that the subsequent text is a URL, not expression code.  Similarly, the prefix 'cache' can be used, which will indicate that the expression should be marked as cached.  Both 'url' and 'cache' can be combined --> url|cache|com/foo/bar/baz.groovy.  The order of 'url' or 'cache' prefix does not matter, and 0, 1, or both can be used. 
  * Bug fix: In the HTML view of an n-cube, rule condition formatting fixed when a rule condition was specified with a URL.  The anchor tag was including both the rule name and the URL (rule name should not have been included in the anchor tag).
  * The rule name in the HTML view is now highlighted in a such a way that the 'name:' text is no longer needed, making the condition look nicer as their is less text.  The rule name is set off from the page with a different background color.
* 3.3.7
  * `GroovyExpression` clears compiled class when cache=true (as it will only be used once).  Useful for cells containing expressions that returns Lists or Maps of data.
  * Added a small amount of padding to left and right side of column headers in the generated HTML.
  * Template API - increased APIs available to code running in `GroovyTemplate` replacement sections, e.g. `${ code }` and `<% code %>`.  APIs for `getCube()`, `getAxis()`, `getColumn()` as well as all Cedar Software APIs are imported so they do not need to be imported or qualified in your code. 
  * Bug fix: When an expression ended with '@foo[:]' and had another line of code below it, the preprocessor code dropped the newline causing this code not to compile.  The only work around was to add a semi-colon for the line.  This has been fixed.
  * Bug fix: `ContentCommandCell` now sets followRedirects flag (honors HTTP 301 / HTTP 302 redirect requests).
* 3.3.6
  * Restored Oracle specific code to allow for case-insensivity when comparing n-cube names.
* 3.3.5
  * Restore Oracle to it's default (case-sensitive) state.  This is temporary until the session-based case-insensitivity is introduced.
* 3.3.4
  * Fixed Oracle bug where name wasn't getting lowered correctly inside `NCubeJdbcPersister`
* 3.3.3
  * Default n-cube (table) level value - the default value can be a Groovy lookup expression, specified by URL or value, cache / not cached, etc.  The same value types as a cell are supported. 
  * Consolidated persister APIs to use search.
  * Updated Persister to be Oracle-aware so that n-cube names are handled case-insensitively.
* 3.3.2
  * Updated `NCubeManager.search()` API to be case-insensitive
* 3.3.1
  * Added API to allow retrieval of specific revision of an n-cube
* 3.3.0
  * Changed all `NCubeManager` APIs that returned `Object[]` containing `NCubeInfoDto`s to instead return `List<NCubeInfoDto>`.
  * Changed all persister APIs that returned `Object[]` containing `NCubeInfoDto`s to instead return `List<NCubeInfoDto>`.
  * Changed `NCubeManager.getAppNames()` and `NCubeManager.getAppVersions()` to return `List<String>` instead of `Object[]`.
  * These changes were necessary to prevent conversion from `List` to `Object[]` from occurring inside the database transaction, holding it open longer than necessary.
* 3.2.1
  * `NCubeReadOnlyPersister.loadCube()` API that took a long cube ID now takes an `NCubeInfoDto` instead, allowing the `NCubeJdbcPersister` to still use the n-cube ID, where as a test read only file persister may used fields from the `NCubeInfoDto` object instead.
* 3.2.0
  * This release adds complete support for branching.  All branch functionality is complete and unit tested, with > 98% code coverage.
  * Merge happens at cell - level - two people can work on the same cube without merge conflict if they change different cells.
  * The branch facility is implemented with the new branching APIs on `NCubeManager` (`createBranch`, `deleteBranch`, `updateBranch`,`merge`, etc.)
  * `NCUBE_PARAMS` is now read as either an environment variable or -D system property as a JSON map.  The keys in this map allow overriding the tenant, application, version, status, or branch.  Override support for other values may be added in the future.  This allows developer testing to switch Apps, verisons, etc., without having to change the `sys.bootstrap` cube.
  * All APIs that take a matching pattern on `NCubeManager` expect  * or ? (match any, or match one).  These are converted internally within the respective persister to yield the expected behavior.
  * Performance: Support for memoization (caching) of `GroovyExpression` results has been added.  If the 'cache' value for an 'exp' (`GroovyExpression`) cell is true, the cell is executed, and the return value is cached (NOTE: The values in the input Map are NOT used as a key for the cached value).
  * Performance: Cubes are stored as compressed `byte[]` (gzip) of JSON when using the JDBC persister.
  * Performance: Cubes are serialized from Streams from the database, reducing the working set memory required when loading a cube.
  * Performance: Removed many instances of 'synchronized' (used for read only cache) and instead using `ConcurrentMap.putIfAbsent()` API.
  * Performance: Failed requests for an n-cube are cached - performance enhancement.
  * Performance: Two database queries reduced to into one query on straight up getCube() call when the cube was not in the cache and needed to be loaded.
  * Performance: Setting/Getting 133,000 cells reduced by a factor of 3 - takes about 1 second on Mac Book Pro 2.4Ghz.  Used to take 3.2 seconds.
  * Internal `Map` containing all cells is now `Map<Set<Long>, Object>` where the key is a set of column identifiers on each axis.  Before it was actual `Column` instances - less flexible.
  * New API: ncube.getPopulatedCellCoordinates().  This returns all cells that actually have values in them.  
* 3.1.7
  * Performance: `NCubeManager.getCube()` - caches failed fetches so that subsequent retries do not hit database again and again.
  * Performance: `NCube.getCell()` - when accessing a non-`CommandCell`, not need to set up the context (`Map` containing `NCube`, `Input`, and `Output`) 
* 3.1.4
  * Range and Set parsing for `Axis` values less picky and much more robust.  Useful when input for these columns are passed in from a GUI.
  * All unit tests are now completely written in Groovy.
  * Any .groovy file that is used internally for the implementation of the library now uses `@CompileStatic` for improved speed.
  * Further development on `GitPersister` (not complete, unaccessible).
* 3.1.3
  * Bug fix: Fixed bug in rule engine where `Boolean.equals()` was being called instead of `isTrue()` - which uses proper Groovy Truth.  This bug was introduced in 3.1.1.
* 3.1.2
  * Bug fix: Tightened up regex pattern match that is used to expand short-hand relative references into `getCell()` calls.  This prevents it from matching popular Java/Groovy annotations in the source code wtihin an expression cell.
  * Started work on `GitPersister`
* 3.1.1
  * Bindings to rule axis with a name is O(1) - directly starts evaluating the named condition.
  * Rule axis now has `fireAll` (versus fire once).  Fire all conditions is the default and what previously existed.  If the `fireAll` property of the `Axis` is set false on a Rule `Axis`, then the first condition that fires, will be the only condition that fires on that axis.
  * `NCubeInfoDto` now includes the SHA1 of the persisted n-cube.
  * bug fix: HTML and JSON formatters handle when the cell contents are a classLoader, as in the case of sys.classpath after it has been loaded.
  * bug fix: rule's with condition of null are now converted to false, allowing the JSON cube that contained such a condition to be loaded.
  * Rule Engine execution performance improvement - evaluation of the current axis to column binding set stops immediately if the rule axis is not bound (for the current condition being evaluated).
  * Many new tests added, including more concurrency tests
  * Moved to Log4J2
* 3.1.0
  * All JUnit test cases converted from Java to Groovy.
  * Improvement in classloader management.  Initially, a classloader per App (tenant, app, version) was maintained.  This has been further refined to support any additional scope that may have been added to the `sys.classpath` cube.  This allows a different URL set per AppID per additional scope like a business unit, for example.
* 3.0.10
  * Attempting to re-use `GroovyClassLoader` after `clearCache(appId)`. Discovered that the URLs do not clear.
* 3.0.9
  * Internal work on classpath management.  Fixing an issue where clearing the cache needed to reset the URLs within the `GroovyClassLoader`.
* 3.0.8
  * Bug fix: Threading issue in `NCubeManager` during initialization.  `GroovyClassLoaders` could be accessed before the resource URLs were added to the `GroovyClassLoader`.
  * Bug fix: `CdnClassLoader` was allowing .class files to be loaded remotely, which 1) is too slow to allow (.class files are attempted to be loaded with HTTP GET which fails very slowly with a 404, and 2) is insecure.  Instead, a future version will allow a 'white-less' of acceptable classes that can be remotely loaded.
* 3.0.6 / 3.0.7
  * Changed `getDeltaDescription()` to return a list of `Delta` objects, which contain the textual difference as well as the location (NCube, Axis, Column, Cell) of the difference and the type of difference (ADD, DELETE, UPDATE).
* 3.0.5
  * Added `getDeltaDescription()` to `NCube` which returns a `List` of differences between the two cubes, each entry is a unique difference.  An empty list means there are no differences.
* 3.0.4
  * Test results now confine all output to the RuleInfo (no more output in the output keys besides '_rule').
  * Formatting of test output now includes `System.out` and `System.err`.  `System.err` output shows in dark red (typical of modern IDEs).
* 3.0.3
  * Added `NCubeInfoDto` to list of classes that are available to Groovy Expression cells, without the author having to import it (inherited imports).
  * Added checks to NCubeManager to prevent any mutable operation on a release cube. Added here in addition to the perister implementations.
* 3.0.2
  * Improved support for reading cubes that were stored in json-io serialized format.
* 3.0.1
  * `NCubeManager` has new API, `resolveRelativeUrl()`.  This API will take a relative URL (com/foo/bar.groovy) and return an absolute URL using the sys.classpath for the given ApplicationID.
  * Bug fix: test data was being cleared when an update cube happened.  The test data was not being copied to the new revision.
* 3.0.0
  * `NCubeManager` no longer has `Connection` in any of it's APIs. Instead a `NCubePersister` is set into the `NCubeManager` at start up, and it uses the persister for interacting with the database.  Set a `ConnectionProvider` inside the `Persister` so that it can obtain connections.  This is in preparation of MongoDB persister support.
  * Cubes can now be written to, in addition to being read from while executing.  This means that n-cube can now be used for transactional data.
  * Caching has been greatly improved and simplified from the user's perspective.  In the past, `NCubeManager` had to be told when to load cubes, and when it could be asked to fetch from the cache.  With the new caching strategy, cubes are loaded with a simple `NCubeManager.getCube()` call.  The Manager will fetch it from the persister if it is not already in it's internal cache.
  * Revision history support added. When a cube is deleted, updated, or restored, an new record is created with a higher revision number.  Cubes are never deleted.  This enabled the new restore capability as well as version history.
  * `NCubeManager` manages the classpath for each Application (tenant, app, version, status).  The classpath is maintained in the `sys.classpath` cube as `List` of `String` paths (relative [resource] entries as well as jar entries supported).
  * `NCubeManager` manages the Application version.  `NCubeManager` will look to the 0.0.0 SNAPSHOT version of the `sys.bootstrap` cube for the App's version and SNAPSHOT. This makes it simple to manage version and status within this single cube.
  * When loading a `GroovyExpression` cell that is specified by a relative URL that ends with .groovy, and attempt will be made to locate this class already in the JVM, as might be there when running with a code coverage tool like Clover.
  * `ApplicationID` class is available to `GroovyExpression` cells.
  * Classpath, Method caches, and so forth are all scoped to tenant id.  When one is cleared, it does not clear cache for other tenants (`ApplicationID`s).
  * Removed unnecessary synchronization by using `ConcurrentHashMap`s.
  * JVM now handles proxied connections.
  * Many more tests have been added, getting code coverage to 95%.
* 2.9.18
  * Carrying ApplicationID throughout n-cube in preparation for n-cube 3.0.0.  This version is technically a pre-release candidate for 3.0.0.  It changes API and removes deprecated APIs.
* 2.9.17
  * Updated CSS tags in Html version of n-cube
  * bug fix: Removed StackOverflow that would occur if an n-cube cell referenced a non-existent n-cube.
  * n-cube names are now treated as case-retentive (case is ignored when locating them, however, original case is retained).
  * Improved unit test coverage.
* 2.9.16
  * Rule name is now displayed (if the 'name' meta-property on column is added) in the HTML.
  * Updated to use Groovy 2.3.7 up from 2.3.4
  * Added more exclusions to the CdnClassLoader to ensure that it does not make wasteful requests.
  * getCubeNames() is now available to Groovy cells to obtain the list of all n-cubes within the app (version and status).
* 2.9.15
  * Added getCube(), getAxis(), getColumn() APIs to NCubeGroovyExpression so that executing cells have easy access to these elements.
  * Added many n-cube classes and all of Java-util's classes as imports within NCubeGroovyExpression so that executing cells have direct access to these classes without requiring them to perform imports.
* 2.9.14
  * Required Scope and Optional Scope supported added.  Required scope is the minimal amount of keys (Set<String>) that must be present on the input coordinate in order to call ncube.getCell().  The n-cube meta property requiredScopeKeys can be set to a Groovy Expression (type="exp") in order to return a List of declared required scope keys.  Optional scope is computed by scanning all the rule conditions, and cells (and joining to other cubes) and including all of the scope keys that are found after 'input.'  The required scope keys are subtracted from this, and that is the full optional scope. Calls to ncube.setCell() need only the scope keys that the n-cube demands (values for Axes that do not have a Default column and are not Rule axes).  The declared required scope keys are not required for setCell(), removeCell(), or containsCell().
  * The 'RUN' feature (Tests) updated to use custom JSON written format, insulating the code from changes.
  * The 'RUN' feature obtains the Required Scope using the new Required Scope API.
* 2.9.13
  * The sys.classpath n-cube (one per app) now allows processing for loading .class files
  * Bug fix: not enough contrast between text URLs and background color on expression cells using URL to point to code.
* 2.9.12
  * Groovy classes from expression and method cells are compiled in parallel.
* 2.9.11
  * output.return entry added to output Map when getCell() / getCells() called.  The value associated to the key "return" is the value of the last step executed.  If it is a table of values, it is the value that was accessed.
* 2.9.8-2.9.10
  * Improvements in HTML display when a cell (or Column) has code in it.
* 2.9.7
  * Bug fix: Axis.updateColumns() should not have been processed (it is 'turned' on / off at Axis level).  This caused cells pointing to it to be dropped when the columns were edited.
* 2.9.6
  * The n-cube API that supports batch column editing (updateColumns()) has been updated to support all the proper parsing and range checking.
  * The HTML n-cube has been updated to include data-axis tags on the columns to support double-click column editing in NCE.
  * The top column row supports hover highlight.
* 2.9.5
  * SHA1 calculation of an n-cube is faster using a SHA1 MessageDigest instance directly.
  * Consolidated JsonFormatter / GroovyJsonFormatter into JsonFormatter.
  * Code moved from JsonFormatter to CellType Enum.
  * RuleInfo now a first-rate class. Dig into it (found on output map of getCell()) for rule execution tracing.
* 2.9.4
  * Rule execution tracing is complete, including calls to sub-rule cubes, sub-sub-rule cubes, etc.  It includes both 'begin>cubeName' and 'end>cubeName' markers as well as an entry for all rules that executed (condition true) in between.  If other rule cubes were called during rule execution, there execution traces are added, maintaining order.  The number of steps execution for a given rule set is kept, as well as all column bindings for each rule (indicates which columns pointed to the rule executed).
  * n-cube sha1() is now computed when formatted into JSON.  It is added as a meta-property on n-cube.  The SHA1 will be used along with NCE to determine if an n-cube has changed (basic for optimistic locking).
  * APIs added to generate test case input coordinates for all populated cells.
  * Improved HTML formatting for display in NCE (eventually NCE will do this in Javascript, and the JSON for the n-cube only will be sent to the client).
  * Code clean up related to formatting values and parsing values.
  * NCubeManager support for ApplicationID started, but not yet complete.
  * NCubeManager support for MongoDB started, but not yet complete.
  * MultiMatch flag removed from n-cube.  If you need multi-match on an axis, use a Rule axis.
* 2.9.3
  * SET and NEAREST axis values are now supported within Axis.convertStringToColumnValue().  This allows in-line editing of these values in the n-cube editor.
  * Many more tests added getting line coverage up to 96%.
  * NCube.setCellUsingObject() and NCube.getCellUsingObject() APIs removed.  Instead use NCube.objectToMap, and then call getCell() or getCells() with that Map.
* 2.9.2
  * jump() API added to expression cells.  Call jump() restarts the rule execution for the currently executing cube.  Calling jump([condition:ruleName, condition2:ruleName2, ...]) permits restarting the rule execution on a particular rule for each rule axis specified.
  * rule execution: If a rule axis is specified in the input coordinate (it is optional), then the associated value is expected to be a rule name (the 'name' field on a Column).  Execution for the rule axis will start at the specified rule.
  * Added new API to n-cube to fetch a List containing all required coordinates for each cell.  The coordinates are in terms of variable key names, not column ids.  This is useful for the n-cube Editor (GUI), allowing it to generate the Test Input coordinates for any cube.
  * N-cube hashcode() API was dramatically simplified.
  * Updated to use json-io 2.7.0
* 2.9.1
  * Added header 'content-type' for when CDN files are loaded locally from a developer's machine.  Providing the mime-type will quiet down browser warnings when loading Javascript, CSS, and HTML files.
  * Added new loadCubes() API to NCubeManager. This permits the caller to load all cubes for a given app, version, and status at start up, so that calls to other n-cubes from Groovy code will not have to worry about the other cubes being loaded.
  * Deprecated [renamed] NCubeManager.setBaseResourceUrls() to NCubeManager.addBaseResourceUrls().  It is additive, not replacing.
  * NCubeManager, Advice, Rules, and Axis tests have been separated into their own test classes, further reducing TestNCube class.
* 2.9.0
  * Bug fix: HTTP response headers are now copied case-insensitively to CdnRouter proxied HTTP response
  * New CdnDefaultHandler available for CDN content routers which dynamically adds logical file names to the CDN type specific routing cache.
* 2.8.2
  * Bug fix: CdnRouter now calls back to the CdnRoutingProvider on new API, doneWithConnection(Connection c) so that the provider knows that it can close or release the connection.
* 2.8.1
  * Bug fix: Exception handler in CdnRouter was chopping off the stack trace (only error message was getting reported).
  * Test case added for the case where an n-cube modifies itself from within execution of getCell().  The example has an axis where the cell associated to the default Column on an axis adds the sought after column.  It's akin to a Map get() call that does a put() of the item if it is not there.
* 2.8.0
  * Rule Execution: After execution of cube that had one or more rule axes (call to getCells()), the output Map has a new entry added under the "_rule" section, named "RULES_EXECUTED".  This entry contains the names (or ID if no name given) of the condition(s) and the return value of the associated statement. More than one condition could be associated to a statement in the case of a cube with 2 or more rule axes.  Therefore the keys of the Map are List, and the value is the associated statement return value.
  * Rule Execution: A condition's truth (true or false) value follows the same as the Groovy language.  For details, see http://www.groovy-lang.org/semantics.html#Groovy-Truth
  * Java 1.7: Template declarations updated to Java 1.7 syntax (no need to repeat collection template parameters a 2nd time on the RHS).
* 2.7.5
  * Added ability to turn Set<Long> into Map<String, Object> coordinate that will retrieve cell described by Set<Long>.  Useful for n-cube editor.
* 2.7.4
  * Bug fix: reloading n-cubes now clears all of its internal caches, thereby allowing reloading Groovy code without server restarts.
  * Bug fix: NCubeManager was not rethrowing the exception when a bad URL was passed to setBaseResourceUrls().
  * Bug fix: If a URL failed to resolve (valid URL, but nothing valid at the other end of the URL), an NPE occurred.  Now an exception is thrown indicating the URL that failed to resolve, the n-cube it resided within, and the version of the n-cube.
* 2.7.3
  * Added GroovyClassLoader 'file' version so that the .json loaded files do not need to call NCubeManager.setUrlClassLoader()
* 2.7.2
  * New API added to NCubeManager, doesCubeExist(), which returns true if the given n-cube is stored within the persistent storage.
  * HTML-syntax highlighting further improved
* 2.7.1
  * Dynamically loaded Groovy classes (loaded from URL), load much faster.
  * The HTML representation of n-cube updated to differentiate URL specified cells and expression cells, from all other cells.  Very basic syntax highlighting if you can call it that.
* 2.7.0
  * New capability: key-value pairs can be added to n-cube, any axis, and any column.  These are picked up from the JSON format or set via setMetaProperty() API.  This allows you to add additional information to an ncube, it's axis, or a column, and it will be stored and retrieved with the n-cube and can be queried later.
  * New capability: NCubeManager has a new API, setUrlClassLoader() which allows you to set a List of String URLs to be added to the Groovy class path that is used when a class references another class by import, extends, or implements.  The URL should point to the fully qualified location up to but just before the code resource (don't include the /com/yourcompany/... portion).
  * New capability: n-cube can be used as a CDN router.  Used in this fashion, fetches to get content will be routed based on the scope of the cube used to route CDN requests.  In order to use this feature, it is expected you have a servlet filter like urlRewrite from Tuckey, which will forward requests to the CdnRouter class.  Furthermore, you need to set up a CdnRouterProvider.  See the code / comments in the com.cedarsoftware.ncube.util package.
* 2.6.3
  * CdnUrlExecutor updated to handle Classpath for resolving URL content (in addition to the existing HTTP support).
* 2.6.2
  * GroovyShell made static in GroovyBase.  GroovyShell is re-entrant.  The GroovyShell is only used when parsing CommandCell URLs to allow for @refCube[:] type expansions.
* 2.6.1
  * Refactor CommandCell interface to have fewer APIs.
  * Bug fix: null check added for when 'Command Text' is empty and attempting search for referenced cube names within it.
* 2.6.0
  * An Executor can be added to a call to getCell(), getCells() where the Executor will be called instead of fetching the cell.  The defaultCellExecutor will execute the cell as before.  It can be overridden so external code can be executed before the cell is returned or after it is executed.
  * runRuleCube() API added to the NCubeGroovyExpression so that a rule can run other rules in other cubes.
  * Potential concurrency bug fixed if the URL: feature of an n-cube was used, and the cell content was not cached.
* 2.5.0
  * Advice can be specified to cube and method name, using wildcards.  For example, `*Controller.save*()` would add the advice to all n-cube Controller classes methods that start with `save`.
  * `containsCellValue()` API added to NCube.  This will return `true` if, and only if, the cell specified by the coordinate has an actual value located in it (defaultCellValue does not count).
  * `containsCell()` API changed.  It will return `true` if the cell has a value, including the defaultCellValue, if it is not null.
  * Both `containsCell()` and `containsCellValue()` throw `CoordinateNotFoundException` if the specified coordinate falls outside the n-cube's defined hyper-space.
  * New public API added to Axis, `promoteValue(AxisValueType type, Comparable value)`.  If the value passed in (e.g. an int) is of the same kind as AxisValueType (e.g. long) then the returned value will be the the larger AxisValueType (long in this example).  If the `valueType` is not of the same basic nature as `value`, an intelligent conversion will be done.  For example, String to Date, Calendar to Date, Date to String, Long to String, Integer to String, String to BigDecimal, and so on.
  * When a Rule cube executes, the output map always contains an `_rule` entry.  There is a constant now defined on NCube (`RULE_EXEC_INFO`) which is used to fetch this meta-information Map.  A new Enum has been added, `RuleMetaKeys` which contains an enum for each rule-meta information entry in this Map.
* 2.4.0
  * Advice interface added.  Allows before() and after() methods to be called before Controller methods or expressions are called.  Only expressions specified by 'url' can have advice placed around them.
  * NCube now writes 'simple JSON' format.  This is the same JSON format that is used in the test resources .json files.  NCubes stored in the database are now written in simple JSON format.  This insulates the format from member variable changes to n-cube and its related classes.
  * Expression (Rule) axis - now supports default column.  This column is fired if no prior expressions fired.  It is essentially the logical NOT of all of the expressions on a rule axis.  Makes it trivial to write the catch-if-nothing-fired-condition.
  * Expression (Rule) axis - conditions on Rule axis can now be specified with URL to the Groovy condition code.  This allows single-step debugging of lengthy conditions.
  * URL specified commands can use res:// URLs for indicating that the source is located within the Java classpath, or http(s).
  * URL specified commands can use @cube[:] references within the URL to allow the hostname / protocol / port to be indicated elsewhere, insulating URLs from changing when the host / protocol / port needs to be changed.
  * NCube methods and expressions specified by URL can now be single-stepped.
  * In the simple JSON format, type 'array' is no longer supported.  To make a cell an Object[], List, Map, etc., make the cell type 'exp' and then specify the content in List, Map, Object[], etc.  For an Object[] use "[1, 2, 3] as Object[]". For a List, use "['This', 'is', 'a list']".  For a Map, use "[key1:'Alpha', key2:'Beta']"
  * Expression cells or controller methods that have identical source code, will use the same Groovy class internally.  The source code is SHA1 hashed and keyed by the hash internally.
* 2.3.3
  * RenameNCube() API added to NCubeManager.
  * The regex that locates the relative n-cube references e.g. @otherCube[x:val], improved to no longer incorrectly find annotations as n-cube references.
  * Levenshtein algorithm moved to CedarSoftware's java-util library. N-cube already had a dependence on java-util.
* 2.3.2
  * HTML formatting improved to handle all cell data types
  * Parse routine that fetches n-cube names was matching too broad a string for n-cube name.
* 2.3.1
  * Date axis be created from, or matched with, a String that passed DateUtilities.parseDate().
  * String axis can be created from, or matched with, a Number instance.
  * Axis.promoteValue() has been made public.
* 2.3.0
  * Groovy expression, method, and template cells can be loaded from 'url' instead of having content directly in 'value' field.  In addition, the 'cacheable' attribute can be added.  When 'true', the template, expression, or method, is loaded and compiled once, and then stored in memory. If 'cacheable' attribute is 'false', then the content is retrieved on each access.
  * Within the url, other n-cubes can be referenced.  For example, `@settings[:]/html/index.html`.  In this example, the current input coordinate that directed access to the cell containing the URL reference, is passed as input to the referenced n-cube(s).  This allows a 'settings-type' n-cube to be used to keep track of actual domains, ports, contexts, etc., leaving the URLs in all the other cubes not needed to be changed when the domain, port, etc. is changed.
* 2.2.0
  * Axis update column value(s) support added
  * NCubeInfoDto ncube id changed from long to String
  * NCubeManager now caches n-cube by name and version, allowing two or more versions of the same named n-cube to be loaded at the same time.  Useful in multi-tenant environment.
  * NCube has the version it was loaded from 'stamped' into it (whether file or disk loaded). Use n-ube's getVersion() API to retrieve it.
* 2.1.0
  * Rule conditions and statements can stop rule execution.  ruleStop() can be called from the condition column or from a Groovy expression or method cell.
  * Output Map is written to in the '_rule' key of the output map, which is a Map, with an entry to indicate whether or not rules where stopped prematurely.  In the future, other useful rule execution will be added to this map.
  * id's specified in simple JSON format can be long, string, double, or boolean. Allows aliasing columns to be referenced by cells' id field.
  * HTML formatting code moved into separate internal formatters package, where other n-cube formatters would be placed.
* 2.0.1
  * 'binary' type added to simple JSON format.  Marks a cell to be returned as byte[].  'value' should be set to hex digits 'CAFEBABE10', or the 'url' should be set to point to location returning binary content.
  * 'cacheable' flag added to 'string' and 'binary' cells (when specified as 'url').  If not specified, the default is cacheable=true, meaning that n-cube will fetch the contents from the URL, and then hold onto it.  Set to "cacheable":false and n-cube will retrieve the content each time the cell is referenced.
* 2.0.0
  * Initial version
