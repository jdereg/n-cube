package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

/**
 * @author Josh Snyder
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
interface NCubeConstants
{
    final String ERROR_CANNOT_MOVE_000 = 'Version 0.0.0 is for system configuration and cannot be moved.'
    final String ERROR_CANNOT_MOVE_TO_000 = 'Version 0.0.0 is for system configuration and branch cannot be moved to it.'
    final String ERROR_CANNOT_RELEASE_000 = 'Version 0.0.0 is for system configuration and cannot be released.'
    final String ERROR_CANNOT_RELEASE_TO_000 = 'Version 0.0.0 is for system configuration and cannot be created from the release process.'

    final String SEARCH_CREATE_DATE_START = 'createStartDate'
    final String SEARCH_CREATE_DATE_END = 'createEndDate'
    final String SEARCH_INCLUDE_CUBE_DATA = 'includeCubeData'
    final String SEARCH_INCLUDE_TEST_DATA = 'includeTestData'
    final String SEARCH_ONLY_TEST_DATA = 'onlyTestData'
    final String SEARCH_INCLUDE_NOTES = 'includeNotes'
    final String SEARCH_DELETED_RECORDS_ONLY = 'deletedRecordsOnly'
    final String SEARCH_ACTIVE_RECORDS_ONLY = 'activeRecordsOnly'
    final String SEARCH_CHANGED_RECORDS_ONLY = 'changedRecordsOnly'
    final String SEARCH_EXACT_MATCH_NAME = 'exactMatchName'
    final String SEARCH_FILTER_INCLUDE = 'includeTags'
    final String SEARCH_FILTER_EXCLUDE = 'excludeTags'
    final String SEARCH_CLOSURE = 'closure'
    final String SEARCH_OUTPUT = 'output'
    final String SEARCH_ALLOW_SYS_INFO = 'allowSysInfo'
    final String SEARCH_CHECK_SHA1 = 'checkSha1'

    final String SYS_ADVICE = 'sys.advice'
    final String SYS_APP = 'sys.app'
    final String SYS_BOOTSTRAP = 'sys.bootstrap'
    final String SYS_PROTOTYPE = 'sys.prototype'
    final String SYS_PERMISSIONS = 'sys.permissions'
    final String SYS_USERGROUPS = 'sys.usergroups'
    final String SYS_LOCK = 'sys.lock'
    final String SYS_BRANCH_PERMISSIONS = 'sys.branch.permissions'
    final String SYS_TRANSACTIONS = 'tx.*'
    final String SYS_CLASSPATH = 'sys.classpath'
    final String SYS_INFO = 'sys.info'

    final String SYS_MENU = 'sys.menu'
    final String GLOBAL_MENU = 'sys.menu.global'
    final String MENU_TITLE = 'title'
    final String MENU_TAB = 'tab-menu'
    final String MENU_NAV = 'nav-menu'
    final String MENU_TITLE_DEFAULT = 'Enterprise Configurator'

    final String ROLE_ADMIN = 'admin'
    final String ROLE_USER = 'user'
    final String ROLE_READONLY = 'readonly'

    final String AXIS_ROLE = 'role'
    final String AXIS_USER = 'user'
    final String AXIS_RESOURCE = 'resource'
    final String AXIS_ACTION = 'action'
    final String AXIS_SYSTEM = 'system'
    final String AXIS_ATTRIBUTE = 'attribute'

    final String PROPERTY_CACHE = 'cache'

    final String CUBE_TAGS = 'cube_tags'
    final String CUBE_EVICT = 'evict'

    final String NCUBE_PARAMS = 'NCUBE_PARAMS'
    final String NCUBE_PARAMS_BYTE_CODE_DEBUG = 'byteCodeDebug'
    final String NCUBE_PARAMS_BYTE_CODE_VERSION = 'byteCodeVersion'
    final String NCUBE_PARAMS_BRANCH = 'branch'

    final String REQUIRED_SCOPE = 'requiredScopeKeys'
    final String OPTIONAL_SCOPE = 'optionalScopeKeys'

    final String PR_PROP = 'property'
    final String PR_STATUS = 'status'
    final String PR_APP = 'appId'
    final String PR_CUBE = 'prCube'
    final String PR_CUBES = 'cubeNames'
    final String PR_REQUESTER = 'requestUser'
    final String PR_REQUEST_TIME = 'requestTime'
    final String PR_ID = 'prId'
    final String PR_MERGER = 'commitUser'
    final String PR_MERGE_TIME = 'commitTime'
    final String PR_OPEN = 'open'
    final String PR_CLOSED = 'closed'
    final String PR_CANCEL = 'closed cancelled'
    final String PR_COMPLETE = 'closed complete'
    final String PR_OBSOLETE = 'obsolete'
    final String PR_TXID = 'txid'

    final String RUNTIME_BEAN = 'ncubeRuntime'
    final String MANAGER_BEAN = 'ncubeManager'
    final String CONTROLLER_BEAN = 'ncubeController'
    final String NCUBE_CLIENT_BEAN = 'ncube-client'
    final String DATA_SOURCE_BEAN = 'dataSource'

    final int LOG_ARG_LENGTH = 125

    final String DATA_TYPE = 'data_type'
    final String INPUT_VALUE = 'input_value'
    final String OUTPUT_VALUE = 'output_value'
    final String INPUT_LOW = 'input_low'
    final String INPUT_HIGH = 'input_high'
    final String PRIORITY = 'priority'
    final String IGNORE = 'ignore'
}