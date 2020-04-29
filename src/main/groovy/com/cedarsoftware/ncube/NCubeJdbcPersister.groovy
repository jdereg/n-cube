package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.formatters.JsonFormatter
import com.cedarsoftware.ncube.formatters.NCubeTestReader
import com.cedarsoftware.util.AdjustableGZIPOutputStream
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.CompactCILinkedMap
import com.cedarsoftware.util.SafeSimpleDateFormat
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.sql.Blob
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.Deflater

import static com.cedarsoftware.ncube.NCubeConstants.*
import static com.cedarsoftware.util.Converter.convertToBoolean
import static com.cedarsoftware.util.Converter.convertToDate
import static com.cedarsoftware.util.Converter.convertToLong
import static com.cedarsoftware.util.Converter.convertToString
import static com.cedarsoftware.util.Converter.convertToTimestamp
import static com.cedarsoftware.util.IOUtilities.uncompressBytes
import static com.cedarsoftware.util.StringUtilities.createUTF8String
import static com.cedarsoftware.util.StringUtilities.equalsIgnoreCase
import static com.cedarsoftware.util.StringUtilities.getUTF8Bytes
import static com.cedarsoftware.util.StringUtilities.hasContent
import static com.cedarsoftware.util.StringUtilities.isEmpty
import static com.cedarsoftware.util.StringUtilities.wildcardToRegexString
import static com.cedarsoftware.util.UniqueIdGenerator.uniqueId

/**
 * SQL Persister for n-cubes.  Manages all reads and writes of n-cubes to an SQL database.
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
class NCubeJdbcPersister
{
    static final SafeSimpleDateFormat DATE_TIME_FORMAT = new SafeSimpleDateFormat('yyyy-MM-dd HH:mm:ss')
    static final String PR_NOTES_PREFIX = 'PR notes: '
    static final String CUBE_VALUE_BIN = 'cube_value_bin'
    static final String TEST_DATA_BIN = 'test_data_bin'
    static final String NOTES_BIN = 'notes_bin'
    static final String HEAD_SHA_1 = 'head_sha1'
    static final String CHANGED = 'changed'
    private static final long EXECUTE_BATCH_CONSTANT = 35
    private static final int FETCH_SIZE = 1000
    private static final String METHOD_NAME = '~method~'
    private static volatile AtomicBoolean isOracle = null
    private static volatile AtomicBoolean isMySQL = null
    private static volatile AtomicBoolean isHSQLDB = null

    static List<NCubeInfoDto> search(Connection c, ApplicationID appId, String cubeNamePattern, String searchContent, Map<String, Object> options)
    {
        List<NCubeInfoDto> list = []
        Pattern searchPattern = null
        Map<String, Object> copyOptions = new CompactCILinkedMap<>(options)
        boolean hasSearchContent = hasContent(searchContent)
        boolean keepCubeData = (boolean)options[SEARCH_INCLUDE_CUBE_DATA] // look at original options value (not coerced value) for SEARCH_INCLUDE_CUBE_DATA
        boolean includeCubeData = hasSearchContent || keepCubeData
        
        if (hasSearchContent)
        {
            String contentPattern = wildcardToRegexString(searchContent)
            contentPattern = contentPattern[1..-2]                   // remove ^ and $
            searchPattern = Pattern.compile(contentPattern, Pattern.CASE_INSENSITIVE)
        }

        // Convert INCLUDE or EXCLUDE filter query from String, Set, or Map to Set.
        copyOptions[SEARCH_FILTER_INCLUDE] = getFilter(copyOptions[SEARCH_FILTER_INCLUDE])
        copyOptions[SEARCH_FILTER_EXCLUDE] = getFilter(copyOptions[SEARCH_FILTER_EXCLUDE])
        Set includeTags = copyOptions[SEARCH_FILTER_INCLUDE] as Set
        Set excludeTags = copyOptions[SEARCH_FILTER_EXCLUDE] as Set

        // If filtering by tags, we need to include CUBE DATA, so add that flag to the search
        includeCubeData |= includeTags || excludeTags
        copyOptions[SEARCH_INCLUDE_CUBE_DATA] = includeCubeData
        copyOptions[METHOD_NAME] = 'search'
        runSelectCubesStatement(c, appId, cubeNamePattern, copyOptions, { ResultSet row ->
            getCubeInfoRecords(appId, searchPattern, list, copyOptions, row, keepCubeData)
        })
        return list
    }

    static NCubeInfoDto loadCubeRecordById(Connection c, long cubeId, Map options)
    {
        if (!options)
        {
            options = [:]
        }
        if (!options.containsKey(SEARCH_INCLUDE_CUBE_DATA))
        {
            options[SEARCH_INCLUDE_CUBE_DATA] = true
        }
        String selectCubeData = options?.get(SEARCH_INCLUDE_CUBE_DATA) ? ",${CUBE_VALUE_BIN}" : ''
        String selectTestData = options?.get(SEARCH_INCLUDE_TEST_DATA) ? ",${TEST_DATA_BIN}" : ''
        NCubeInfoDto record = null
        Map map = [id: cubeId]
        Sql sql = getSql(c)
        sql.withStatement { Statement stmt -> stmt.fetchSize = 10 }
        sql.eachRow(map, """\
/* loadCubeRecordById */
SELECT n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, create_dt, create_hid, revision_number, changed, sha1, head_sha1, notes_bin ${selectCubeData} ${selectTestData}
FROM n_cube
WHERE n_cube_id = :id""", 0, 1, { ResultSet row ->
            record = createDtoFromRow(row, options)
            record.tenant = row.getString('tenant_cd')
        })
        if (record)
        {
            return record
        }
        throw new IllegalArgumentException("Unable to find cube with id: " + cubeId)
    }

    static NCube loadCubeBySha1(Connection c, ApplicationID appId, String cubeName, String sha1)
    {
        Map map = appId as Map
        map.cube = buildName(cubeName)
        map.sha1 = sha1.toUpperCase()
        map.tenant = padTenant(c, appId.tenant)
        NCube cube = null
        Sql sql = getSql(c)
        sql.eachRow(map, """\
/* loadCubeBySha1 */
SELECT ${CUBE_VALUE_BIN}, sha1, ${TEST_DATA_BIN}
FROM n_cube
WHERE ${buildNameCondition('n_cube_nm')} = :cube AND app_cd = :app AND tenant_cd = :tenant AND branch_id = :branch AND sha1 = :sha1""",
                0, 1, { ResultSet row ->
            cube = buildCube(appId, row, true)
        })

        if (cube)
        {
            return cube
        }
        throw new IllegalArgumentException("Unable to find cube: ${cubeName}, app: ${appId} with SHA-1: ${sha1}")
    }

    static List<NCubeInfoDto> getRevisions(Connection c, ApplicationID appId, String cubeName, boolean ignoreVersion)
    {
        Map map = appId as Map
        map.tenant = padTenant(c, appId.tenant)
        map.cube = buildName(cubeName)
        Sql sql = getSql(c)
        String selectVersion = ignoreVersion ? '' : 'AND version_no_cd = :version AND status_cd = :status'
        String orderByVersion = ignoreVersion ? 'version_no_cd DESC,' : ''

        String sqlStatement = """\
/* getRevisions */
SELECT n_cube_id, n_cube_nm, notes_bin, version_no_cd, status_cd, app_cd, create_dt, create_hid, revision_number, branch_id, sha1, head_sha1, changed
FROM n_cube
WHERE ${buildNameCondition('n_cube_nm')} = :cube AND app_cd = :app AND tenant_cd = :tenant AND branch_id = :branch ${selectVersion}
ORDER BY ${orderByVersion} abs(revision_number) DESC
"""

        List<NCubeInfoDto> records = []
        sql.eachRow(map, sqlStatement, { ResultSet row ->
            getCubeInfoRecords(appId, null, records, [:], row)
        })

        if (records.empty)
        {
            throw new IllegalArgumentException("Cannot fetch revision history for cube: ${cubeName} as it does not exist in app: ${appId}")
        }
        return records
    }

    static NCubeInfoDto insertCube(Connection c, ApplicationID appId, String name, Long revision, byte[] cubeData,
                                   byte[] testData, String notes, boolean changed, String sha1, String headSha1,
                                   String username, String methodName) throws SQLException
    {
        PreparedStatement s = null
        try
        {
            s = c.prepareStatement("""\
/* ${methodName}.insertCubeBytes */
INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
sha1, head_sha1, create_dt, create_hid, ${CUBE_VALUE_BIN}, ${TEST_DATA_BIN}, notes_bin, changed)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
            long uniqueId = uniqueId
            s.setLong(1, uniqueId)
            s.setString(2, appId.tenant)
            s.setString(3, appId.app)
            s.setString(4, appId.version)
            s.setString(5, appId.status)
            s.setString(6, appId.branch)
            s.setString(7, name)
            s.setLong(8, revision)
            s.setString(9, sha1)
            s.setString(10, headSha1)
            Timestamp now = nowAsTimestamp()
            s.setTimestamp(11, now)
            s.setString(12, username)
            s.setBytes(13, cubeData)
            s.setBytes(14, testData)
            String note = createNote(username, now, notes)
            s.setBytes(15, getUTF8Bytes(note))
            s.setInt(16, changed ? 1 : 0)

            NCubeInfoDto dto = new NCubeInfoDto()
            dto.id = Long.toString(uniqueId)
            dto.name = name
            dto.sha1 = sha1
            dto.headSha1 = sha1
            dto.changed = changed
            dto.tenant = appId.tenant
            dto.app = appId.app
            dto.version = appId.version
            dto.status = appId.status
            dto.branch = appId.branch
            dto.createDate = now
            dto.createHid = username
            dto.notes = note
            dto.revision = Long.toString(revision)

            int rows = s.executeUpdate()
            if (rows == 1)
            {
                return dto
            }
            throw new IllegalStateException("Unable to insert cube: ${name} into database, app: ${appId}, attempted action: ${notes}")
        }
        finally
        {
            s?.close()
        }
    }

    static NCubeInfoDto insertCube(Connection c, ApplicationID appId, NCube cube, Long revision, byte[] testData, String notes,
                                   boolean changed, String headSha1, String username, String methodName)
    {
        long uniqueId = uniqueId
        Timestamp now = nowAsTimestamp()
        final Blob blob = c.createBlob()
        OutputStream out = blob.setBinaryStream(1L)
        OutputStream stream = new AdjustableGZIPOutputStream(out, 8192, Deflater.BEST_SPEED)
        new JsonFormatter(stream).formatCube(cube, null)
        PreparedStatement s = null

        try
        {
            s = c.prepareStatement("""\
/* ${methodName}.insertCube */
INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
sha1, head_sha1, create_dt, create_hid, ${CUBE_VALUE_BIN}, test_data_bin, notes_bin, changed)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
            s.setLong(1, uniqueId)
            s.setString(2, appId.tenant)
            s.setString(3, appId.app)
            s.setString(4, appId.version)
            s.setString(5, appId.status)
            s.setString(6, appId.branch)
            s.setString(7, cube.name)
            s.setLong(8, revision)
            s.setString(9, cube.sha1())
            s.setString(10, headSha1)
            s.setTimestamp(11, now)
            s.setString(12, username)
            s.setBlob(13, blob)
            s.setBytes(14, testData)
            String note = createNote(username, now, notes)
            s.setBytes(15, getUTF8Bytes(note))
            s.setBoolean(16, changed)

            NCubeInfoDto dto = new NCubeInfoDto()
            dto.id = Long.toString(uniqueId)
            dto.name = cube.name
            dto.sha1 = cube.sha1()
            dto.headSha1 = cube.sha1()
            dto.changed = changed
            dto.tenant = appId.tenant
            dto.app = appId.app
            dto.version = appId.version
            dto.status = appId.status
            dto.branch = appId.branch
            dto.createDate = new Date(System.currentTimeMillis())
            dto.createHid = username
            dto.notes = note
            dto.revision = Long.toString(revision)

            int rows = s.executeUpdate()
            if (rows == 1)
            {
                return dto
            }
            throw new IllegalStateException("Unable to insert cube: ${cube.name} into database, app: ${appId}, attempted action: ${notes}")
        }
        finally
        {
            s?.close()
        }
    }

    static boolean deleteCubes(Connection c, ApplicationID appId, Object[] cubeNames, boolean allowDelete, String username)
    {
        boolean autoCommit = c.autoCommit
        PreparedStatement stmt = null
        try
        {
            c.autoCommit = false
            int count = 0
            if (allowDelete)
            {   // Not the most efficient, but this is only used for testing, never from running app.
                String sqlCmd = "/* deleteCubes */ DELETE FROM n_cube WHERE app_cd = ? AND ${buildNameCondition("n_cube_nm")} = ? AND version_no_cd = ? AND (tenant_cd = ? OR tenant_cd = RPAD(?, 10, ' ')) AND branch_id = ?"
                stmt = c.prepareStatement(sqlCmd)
                for (int i = 0; i < cubeNames.length; i++)
                {
                    stmt.setString(1, appId.app)
                    stmt.setString(2, buildName((String) cubeNames[i]))
                    stmt.setString(3, appId.version)
                    stmt.setString(4, appId.tenant)
                    stmt.setString(5, appId.tenant)
                    stmt.setString(6, appId.branch)
                    count += stmt.executeUpdate()
                }
                return count > 0
            }

            stmt = c.prepareStatement("""\
/* deleteCubes */
INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
sha1, head_sha1, create_dt, create_hid, ${CUBE_VALUE_BIN}, test_data_bin, notes_bin, changed)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

            long txId = uniqueId
            Map<String, Object> options = [(SEARCH_ACTIVE_RECORDS_ONLY): true,
                                           (SEARCH_INCLUDE_CUBE_DATA)  : true,
                                           (SEARCH_INCLUDE_TEST_DATA)  : true,
                                           (SEARCH_EXACT_MATCH_NAME)   : true,
                                           (METHOD_NAME) : 'deleteCubes'] as Map
            cubeNames.each { Object cname ->
                String cubeName = (String) cname
                if (!SYS_INFO.equalsIgnoreCase(cubeName))
                {
                    Long revision = null
                    runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row ->
                        revision = row.getLong('revision_number')
                        addBatchInsert(stmt, row, appId, cubeName, -(revision + 1i), "deleted, txId: [${txId}]", username, ++count)
                    })
                    if (revision == null)
                    {
                        throw new IllegalArgumentException("Cannot delete cube: ${cubeName} as it does not exist in app: ${appId}")
                    }
                }
            }
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                stmt.executeBatch()
            }
            return count > 0
        }
        finally
        {
            c.autoCommit = autoCommit
            stmt?.close()
        }
    }

    static boolean restoreCubes(Connection c, ApplicationID appId, Object[] names, String username)
    {
        boolean autoCommit = c.autoCommit
        PreparedStatement ins = null
        try
        {
            c.autoCommit = false
            ins = c.prepareStatement("""\
/* restoreCubes */
INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
sha1, head_sha1, create_dt, create_hid, ${CUBE_VALUE_BIN}, test_data_bin, notes_bin, changed)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

            Map<String, Object> options = [(SEARCH_DELETED_RECORDS_ONLY): true,
                                           (SEARCH_INCLUDE_CUBE_DATA)   : true,
                                           (SEARCH_INCLUDE_TEST_DATA)   : true,
                                           (SEARCH_EXACT_MATCH_NAME)    : true,
                                           (METHOD_NAME) : 'restoreCubes'] as Map
            int count = 0
            long txId = uniqueId
            final String msg = "restored, txId: [${txId}]"

            names.each { Object cname ->
                String cubeName = (String) cname
                Long revision = null
                runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row ->
                    revision = row.getLong('revision_number')
                    addBatchInsert(ins, row, appId, cubeName, Math.abs(revision as long) + 1, msg, username, ++count)
                })

                if (revision == null)
                {
                    throw new IllegalArgumentException("Cannot restore cube: ${cubeName} as it is not deleted in app: ${appId}")
                }
            }
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                ins.executeBatch()
            }
            return count > 0
        }
        finally
        {
            c.autoCommit = autoCommit
            ins?.close()
        }
    }

    private static void addBatchInsert(PreparedStatement stmt, ResultSet row, ApplicationID appId, String cubeName, long rev, String action, String username, int count)
    {
        byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
        byte[] testData = row.getBytes(TEST_DATA_BIN)
        String sha1 = row.getString('sha1')
        String headSha1 = row.getString('head_sha1')

        long uniqueId = uniqueId
        stmt.setLong(1, uniqueId)
        stmt.setString(2, appId.tenant)
        stmt.setString(3, appId.app)
        stmt.setString(4, appId.version)
        stmt.setString(5, appId.status)
        stmt.setString(6, appId.branch)
        stmt.setString(7, cubeName)
        stmt.setLong(8, rev)
        stmt.setString(9, sha1)
        stmt.setString(10, headSha1)
        Timestamp now = nowAsTimestamp()
        stmt.setTimestamp(11, now)
        stmt.setString(12, username)
        stmt.setBytes(13, jsonBytes)
        stmt.setBytes(14, testData)
        stmt.setBytes(15, getUTF8Bytes(createNote(username, now, action)))
        stmt.setInt(16, 1)
        stmt.addBatch()
        if (count % EXECUTE_BATCH_CONSTANT == 0)
        {
            stmt.executeBatch()
        }
    }

    static List<NCubeInfoDto> pullToBranch(Connection c, ApplicationID appId, Object[] cubeIds, String username, long txId)
    {
        List<NCubeInfoDto> infoRecs = []
        if (ArrayUtilities.isEmpty(cubeIds))
        {
            return infoRecs
        }
        createSysInfoCube(c, appId, username)
        String sql = "/* pullToBranch */ SELECT n_cube_nm, revision_number, branch_id, ${CUBE_VALUE_BIN}, test_data_bin, ${NOTES_BIN}, sha1 FROM n_cube WHERE n_cube_id = ?"
        PreparedStatement stmt = null
        try
        {
            stmt = c.prepareStatement(sql)

            for (int i = 0; i < cubeIds.length; i++)
            {
                stmt.setLong(1, convertToLong(cubeIds[i]))
                ResultSet row = stmt.executeQuery()

                if (row.next())
                {
                    byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
                    String sha1 = row.getString('sha1')
                    String cubeName = row.getString('n_cube_nm')
                    Long revision = row.getLong('revision_number')
                    String branch = row.getString('branch_id')
                    byte[] testData = row.getBytes(TEST_DATA_BIN)

                    byte[] notes = row.getBytes(NOTES_BIN)
                    String notesStr = createUTF8String(notes)
                    String newNotes = "updated from ${branch}, txId: [${txId}]"
                    if (notesStr.contains(PR_NOTES_PREFIX))
                    {
                        newNotes += ", ${notesStr.substring(notesStr.indexOf(PR_NOTES_PREFIX))}"
                    }

                    Long maxRevision = getMaxRevision(c, appId, cubeName, 'pullToBranch')

                    //  create case because max revision was not found.
                    if (maxRevision == null)
                    {
                        maxRevision = revision < 0 ? new Long(-1) : new Long(0)
                    }
                    else if (revision < 0)
                    {
                        // cube deleted in branch
                        maxRevision = -(Math.abs(maxRevision as long) + 1)
                    }
                    else
                    {
                        maxRevision = Math.abs(maxRevision as long) + 1
                    }

                    NCubeInfoDto dto = insertCube(c, appId, cubeName, maxRevision, jsonBytes, testData, newNotes, false, sha1, sha1, username, 'pullToBranch')
                    infoRecs.add(dto)
                }
            }
        }
        finally
        {
            stmt?.close()
        }
        return infoRecs
    }

    static void updateCube(Connection c, NCube cube, String username)
    {
        ApplicationID appId = cube.applicationID
        Map<String, Object> options = [(SEARCH_INCLUDE_CUBE_DATA): true,
                                       (SEARCH_INCLUDE_TEST_DATA): true,
                                       (SEARCH_EXACT_MATCH_NAME): true,
                                       (METHOD_NAME) : 'updateCube'] as Map
        boolean rowFound = false

        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            rowFound = true
            Long revision = row.getLong("revision_number")
            boolean cubeActive = revision >= 0
            byte[] testData = row.getBytes(TEST_DATA_BIN)
            String headSha1 = row.getString('head_sha1')
            String oldSha1 = row.getString('sha1')

            if (cube.metaProperties.containsKey(NCube.METAPROPERTY_TEST_DATA))
            {
                byte[] updatedTestData = getUTF8Bytes(cube.metaProperties[NCube.METAPROPERTY_TEST_DATA] as String)
                if ((updatedTestData || testData) && updatedTestData != testData)
                {
                    cube.setMetaProperty(NCube.METAPROPERTY_TEST_UPDATED, uniqueId)
                    testData = updatedTestData
                }
            }

            if (cubeActive && equalsIgnoreCase(oldSha1, cube.sha1()))
            {
                // SHA-1's are equal and both revision values are positive.  No need for new revision of record.
                return
            }

            boolean changed = !equalsIgnoreCase(cube.sha1() ,headSha1)
            insertCube(c, appId, cube, Math.abs(revision as long) + 1, testData, "updated", changed, headSha1, username, 'updateCube')
        })

        // Add Case - No existing row found, then create a new cube (updateCube can be used for update or create)
        if (!rowFound)
        {
            throw new IllegalArgumentException("Unable to update cube: ${cube.name} in app: ${appId}, cube does not exist")
        }
    }

    static void createCube(Connection c, NCube cube, String username)
    {
        ApplicationID appId = cube.applicationID
        Map<String, Object> options = [(SEARCH_EXACT_MATCH_NAME): true,
                                       (METHOD_NAME) : 'createCube'] as Map

        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            throw new IllegalArgumentException("Unable to create cube: ${cube.name} in app: ${appId}, cube already exists (it may need to be restored)")
        })

        // Add Case - No existing row found, then create a new cube (updateCube can be used for update or create)
        String updatedTestData = cube.metaProperties[NCube.METAPROPERTY_TEST_DATA]
        insertCube(c, appId, cube, 0L, updatedTestData?.bytes, "created", true, null, username, 'createCube')
        createSysInfoCube(c, appId, username)
    }

    static boolean duplicateCube(Connection c, ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName, String username)
    {
        byte[] jsonBytes = null
        Long oldRevision = null
        byte[] oldTestData = null
        String sha1 = null

        Map<String, Object> options = [
                (SEARCH_INCLUDE_CUBE_DATA):true,
                (SEARCH_INCLUDE_TEST_DATA):true,
                (SEARCH_EXACT_MATCH_NAME):true,
                (METHOD_NAME) : 'duplicateCube'] as Map

        runSelectCubesStatement(c, oldAppId, oldName, options, 1, { ResultSet row ->
            jsonBytes = row.getBytes(CUBE_VALUE_BIN)
            oldRevision = row.getLong('revision_number')
            oldTestData = row.getBytes(TEST_DATA_BIN)
            sha1 = row.getString('sha1')
        })

        if (oldRevision == null)
        {   // not found
            throw new IllegalArgumentException("Could not duplicate cube because cube does not exist, app: ${oldAppId}, name: ${oldName}")
        }

        if (oldRevision < 0)
        {
            throw new IllegalArgumentException("Unable to duplicate deleted cube, app: ${oldAppId}, name: ${oldName}")
        }

        Long newRevision = null
        String headSha1 = null

        // Do not include test, n-cube, or notes blob columns in search - much faster
        Map<String, Object> options1 = [(SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'duplicateCube'] as Map
        runSelectCubesStatement(c, newAppId, newName, options1, 1, { ResultSet row ->
            newRevision = row.getLong('revision_number')
            headSha1 = row.getString('head_sha1')
        })

        if (newRevision != null && newRevision >= 0)
        {
            throw new IllegalArgumentException("Unable to duplicate cube, cube already exists with the new name, app:  ${newAppId}, name: ${newName}")
        }

        boolean nameChanged = !equalsIgnoreCase(oldName, newName)
        boolean sameExceptBranch = oldAppId.equalsNotIncludingBranch(newAppId)

        // If names are different we need to recalculate the sha-1
        if (nameChanged)
        {
            NCube ncube = NCube.createCubeFromBytes(jsonBytes)
            ncube.name = newName
            ncube.applicationID = newAppId
            jsonBytes = ncube.cubeAsGzipJsonBytes
            sha1 = ncube.sha1()
        }

        String notes = "Cube duplicated from app: ${oldAppId}, name: ${oldName}"
        Long rev = newRevision == null ? 0L : Math.abs(newRevision as long) + 1L
        insertCube(c, newAppId, newName, rev, jsonBytes, oldTestData, notes, true, sha1, sameExceptBranch ? headSha1 : null, username, 'duplicateCube')
        createSysInfoCube(c, newAppId, username)
        return true
    }

    static boolean renameCube(Connection c, ApplicationID appId, String oldName, String newName, String username)
    {
        byte[] oldBytes = null
        byte[] testData = null
        Long oldRevision = null
        String oldSha1 = null
        String oldHeadSha1 = null

        Map<String, Object> options = [
                (SEARCH_INCLUDE_CUBE_DATA):true,
                (SEARCH_INCLUDE_TEST_DATA):true,
                (SEARCH_EXACT_MATCH_NAME):true,
                (METHOD_NAME) : 'renameCube'] as Map

        NCube ncube = null
        runSelectCubesStatement(c, appId, oldName, options, 1, { ResultSet row ->
            oldRevision = row.getLong('revision_number')
            oldBytes = row.getBytes(CUBE_VALUE_BIN)
            testData = row.getBytes(TEST_DATA_BIN)
            oldSha1 = row.getString('sha1')
            oldHeadSha1 = row.getString('head_sha1')
            ncube = buildCube(appId, row)
        })

        if (oldRevision == null)
        {   // not found
            throw new IllegalArgumentException("Could not rename cube because cube does not exist, app: ${appId}, name: ${oldName}")
        }

        if (oldRevision != null && oldRevision < 0)
        {
            throw new IllegalArgumentException("Deleted cubes cannot be renamed (restore it first), app: ${appId}, ${oldName} -> ${newName}")
        }

        Long newRevision = null
        String newHeadSha1 = null

        // Do not include n-cube, tests, or notes in search
        Map<String, Object> options1 = [(SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'renameCube'] as Map
        runSelectCubesStatement(c, appId, newName, options1, 1, { ResultSet row ->
            newRevision = row.getLong('revision_number')
            newHeadSha1 = row.getString(HEAD_SHA_1)
        })

        ncube.name = newName
        String notes = "renamed: ${oldName} -> ${newName}"

        if (oldName.equalsIgnoreCase(newName))
        {   // Changing case
            Long rev = newRevision == null ? 0L : Math.abs(newRevision as long) + 1L    // New n-cube will start at 0 (unless we are re-using the name of a deleted cube, in which case it will start +1 from that)
            insertCube(c, appId, ncube, rev, testData, notes, true, newHeadSha1, username, 'renameCube')                                        // create new cube
        }
        else
        {   // Changing name (mark cube deleted, create / restore new one)
            Long rev = newRevision == null ? 0L : Math.abs(newRevision as long) + 1L    // New n-cube will start at 0 (unless we are re-using the name of a deleted cube, in which case it will start +1 from that)
            insertCube(c, appId, oldName, -(oldRevision + 1), oldBytes, testData, notes, true, oldSha1, oldHeadSha1, username, 'renameCube')    // delete cube being renamed
            insertCube(c, appId, ncube, rev, testData, notes, true, newHeadSha1, username, 'renameCube')                                        // create new cube

        }
        return true
    }

    static NCubeInfoDto commitMergedCubeToBranch(Connection c, ApplicationID appId, NCube cube, String headSha1, String username, long txId)
    {
        Map options = [(SEARCH_INCLUDE_TEST_DATA):true,
                       (SEARCH_EXACT_MATCH_NAME):true,
                       (SEARCH_INCLUDE_NOTES): true,
                       (METHOD_NAME) : 'commitMergedCubeToBranch'] as Map

        NCubeInfoDto result = null
        boolean changed = !equalsIgnoreCase(cube.sha1(), headSha1)

        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            Long revision = row.getLong('revision_number')
            revision = revision < 0 ? revision - 1 : revision + 1

            byte[] testData = row.getBytes(TEST_DATA_BIN)
            if (cube.metaProperties.containsKey(NCube.METAPROPERTY_TEST_DATA))
            {
                byte[] updatedTestData = getUTF8Bytes(cube.metaProperties[NCube.METAPROPERTY_TEST_DATA] as String)
                if ((updatedTestData || testData) && updatedTestData != testData)
                {
                    testData = updatedTestData
                }
            }

            byte[] notes = row.getBytes(NOTES_BIN)
            String notesStr = createUTF8String(notes)
            String newNotes = "merged to branch, txId: [${txId}]"
            if (notesStr.contains(PR_NOTES_PREFIX))
            {
                int pos = notesStr.indexOf('PR notes: ')    // If I put PR_NOTES_PREFIX here, I receive red error in Intellij
                newNotes += ", ${notesStr.substring(pos)}"
            }

            result = insertCube(c, appId, cube, revision, testData, newNotes, changed, headSha1, username, 'commitMergedCubeToBranch')
        })
        return result
    }

    static NCubeInfoDto commitMergedCubeToHead(Connection c, ApplicationID appId, NCube cube, String username, String requestUser, String txId, String notes)
    {
        final String methodName = 'commitMergedCubeToHead'
        Map options = [(SEARCH_INCLUDE_TEST_DATA):true,
                       (SEARCH_EXACT_MATCH_NAME):true,
                       (METHOD_NAME) : methodName]

        ApplicationID headAppId = appId.asHead()
        NCubeInfoDto result = null
        String noteText = "merged-committed from [${requestUser}], txId: [${txId}], ${PR_NOTES_PREFIX}${notes}"

        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            Long revision = row.getLong('revision_number')

            // get current max HEAD revision
            Long maxRevision = getMaxRevision(c, headAppId, cube.name, methodName)

            if (maxRevision == null)
            {
                maxRevision = revision < 0 ? -1L : 0L
            }
            else if (revision < 0)
            {
                // cube deleted in branch
                maxRevision = -(Math.abs(maxRevision as long) + 1)
            }
            else
            {
                maxRevision = Math.abs(maxRevision as long) + 1
            }

            byte[] testData = row.getBytes(TEST_DATA_BIN)
            // ok to use this here, because we're writing out these bytes twice (once to head and once to branch)
            byte[] cubeData = cube.cubeAsGzipJsonBytes
            String sha1 = cube.sha1()

            insertCube(c, headAppId, cube.name, maxRevision, cubeData, testData, noteText, false, sha1, null, username, methodName)
            result = insertCube(c, appId, cube.name, revision > 0 ? ++revision : --revision, cubeData, testData, noteText, false, sha1, sha1, username,  methodName)
        })
        return result
    }

    static List<NCubeInfoDto> commitCubes(Connection c, ApplicationID appId, Object[] cubeIds, String username, String requestUser, String txId, String notes)
    {
        List<NCubeInfoDto> infoRecs = []
        if (ArrayUtilities.isEmpty(cubeIds))
        {
            return infoRecs
        }

        ApplicationID headAppId = appId.asHead()
        Sql sql = getSql(c)
        Sql sql1 = getSql(c)
        def map = [:]
        String searchStmt = "/* commitCubes */ SELECT n_cube_nm, revision_number, cube_value_bin, test_data_bin, sha1 FROM n_cube WHERE n_cube_id = :id"
        String commitStmt = "/* commitCubes */ UPDATE n_cube set head_sha1 = :head_sha1, changed = 0, create_dt = :create_dt WHERE n_cube_id = :id"
        String noteText = "merged pull request from [${requestUser}], txId: [${txId}], ${PR_NOTES_PREFIX}${notes}"

        for (int i = 0; i < cubeIds.length; i++)
        {
            Object cubeId = cubeIds[i]
            map.id = convertToLong(cubeId)
            sql.eachRow(searchStmt, map, 0, 1, { ResultSet row ->
                byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
                String sha1 = row.getString('sha1')
                String cubeName = row.getString('n_cube_nm')
                Long revision = row.getLong('revision_number')
                Long maxRevision = getMaxRevision(c, headAppId, cubeName, 'commitCubes')

                //  create case because max revision was not found.
                String changeType = null
                if (maxRevision == null)
                {
                    if (revision < 0)
                    {   // User created then deleted cube, but it has no HEAD corresponding cube, don't promote it
                    }
                    else
                    {
                        changeType = ChangeType.CREATED.code
                        maxRevision = 0L
                    }
                }
                else if (revision < 0)
                {
                    if (maxRevision < 0)
                    {   // Deleted in both, don't promote it
                    }
                    else
                    {
                        changeType = ChangeType.DELETED.code
                        maxRevision = -(maxRevision + 1)
                    }
                }
                else
                {
                    if (maxRevision < 0)
                    {
                        changeType = ChangeType.RESTORED.code
                    }
                    else
                    {
                        changeType = ChangeType.UPDATED.code
                    }
                    maxRevision = Math.abs(maxRevision as long) + 1
                }

                if (changeType)
                {
                    byte[] testData = row.getBytes(TEST_DATA_BIN)
                    NCubeInfoDto dto = insertCube(c, headAppId, cubeName, maxRevision, jsonBytes, testData, noteText, false, sha1, null, username, 'commitCubes')
                    Map map1 = [head_sha1: sha1, create_dt: nowAsTimestamp(), id: cubeId]
                    sql1.executeUpdate(map1, commitStmt)
                    dto.changed = false
                    dto.changeType = changeType
                    dto.id = convertToString(cubeId)
                    dto.sha1 = sha1
                    dto.headSha1 = sha1
                    infoRecs.add(dto)
                }
            })
        }
        createSysInfoCube(c, headAppId, username)
        return infoRecs
    }

    /**
     * Create sys.info if it doesn't exist.
     */
    private static void createSysInfoCube(Connection c, ApplicationID appId, String username)
    {
        List<NCubeInfoDto> records = search(c, appId, SYS_INFO, null,
                [(SEARCH_INCLUDE_CUBE_DATA): false,
                 (SEARCH_EXACT_MATCH_NAME): true,
                 (SEARCH_ACTIVE_RECORDS_ONLY):false,
                 (SEARCH_DELETED_RECORDS_ONLY):false,
                 (SEARCH_ALLOW_SYS_INFO):true
                ] as Map)

        if (!records.empty)
        {
            return
        }
        NCube sysInfo = new NCube(SYS_INFO)
        Axis attribute = new Axis(AXIS_ATTRIBUTE, AxisType.DISCRETE, AxisValueType.CISTRING, true)
        sysInfo.addAxis(attribute)
        sysInfo.applicationID = appId
        createCube(c, sysInfo, username)
    }

    /**
     * Rollback branch cube to the last time it matched the HEAD branch (SHA-1 == HEAD_SHA-1).  This entails
     * going through the revision history from highest revision toward lowest revision, and finding the
     * last time sha1 == headSha1.  When found, INSERT a new record that is a copy of that record, with
     * revision_number == max(revision_number) + 1.
     * If there are no matches, then the revision_number inserted is negative (deleted).  This is the case of
     * creating a new cube and never calling commit with it.
     */
    static int rollbackCubes(Connection c, ApplicationID appId, Object[] names, String username)
    {
        int count = 0
        boolean autoCommit = c.autoCommit
        PreparedStatement ins = null
        try
        {
            c.autoCommit = false
            ins = c.prepareStatement("""\
/* rollbackCubes */
INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
 sha1, head_sha1, create_dt, create_hid, ${CUBE_VALUE_BIN}, test_data_bin, notes_bin, changed)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

            Map map = appId as Map
            map.tenant = padTenant(c, appId.tenant)
            long txId = uniqueId
            Timestamp now = nowAsTimestamp()
            String note = createNote(username, now, "rolled back, txId: [${txId}]")
            byte[] noteBytes = getUTF8Bytes(note)

            String stmt = """\
/* rollbackCubes */
SELECT ${CUBE_VALUE_BIN}, test_data_bin, changed, sha1, head_sha1
FROM n_cube
WHERE ${buildNameCondition('n_cube_nm')} = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status
AND tenant_cd = :tenant AND branch_id = :branch AND revision_number = :rev"""

            names.each { Object cname ->
                String cubeName = (String) cname
                Long madMaxRev = getMaxRevision(c, appId, cubeName, 'rollbackCubes')
                if (madMaxRev == null)
                {
                    log.info("Attempt to rollback non-existing cube: ${cubeName}, app: ${appId}")
                }
                else
                {
                    long maxRev = madMaxRev
                    Long rollbackRev = findRollbackRevision(c, appId, cubeName)
                    boolean rollbackStatusActive = getRollbackRevisionStatus(c, appId, cubeName)
                    boolean mustDelete = rollbackRev == null
                    map.cube = buildName(cubeName)
                    map.rev = mustDelete ? maxRev : rollbackRev
                    Sql sql = getSql(c)
                    sql.eachRow(map, stmt, 0, 1, { ResultSet row ->
                        byte[] bytes = row.getBytes(CUBE_VALUE_BIN)
                        byte[] testData = row.getBytes(TEST_DATA_BIN)
                        String sha1 = row.getString('sha1')
                        String headSha1 = row.getString('head_sha1')
                        long nextRev = Math.abs(maxRev) + 1
                        long uniqueId = uniqueId

                        ins.setLong(1, uniqueId)
                        ins.setString(2, appId.tenant)
                        ins.setString(3, appId.app)
                        ins.setString(4, appId.version)
                        ins.setString(5, appId.status)
                        ins.setString(6, appId.branch)
                        ins.setString(7, cubeName)
                        ins.setLong(8, mustDelete || !rollbackStatusActive ? -nextRev : nextRev)
                        ins.setString(9, sha1)
                        ins.setString(10, headSha1)
                        ins.setTimestamp(11, now)
                        ins.setString(12, username)
                        ins.setBytes(13, bytes)
                        ins.setBytes(14, testData)
                        ins.setBytes(15, noteBytes)
                        ins.setInt(16, 0)
                        ins.addBatch()
                        count++
                        if (count % EXECUTE_BATCH_CONSTANT == 0)
                        {
                            ins.executeBatch()
                        }
                    })
                }
            }
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                ins.executeBatch()
            }
        }
        finally
        {
            c.autoCommit = autoCommit
            ins?.close()
        }
        return count
    }

    private static boolean getRollbackRevisionStatus(Connection c, ApplicationID appId, String cubeName)
    {
        Sql sql = getSql(c)
        Map map = appId as Map
        map.cube = buildName(cubeName)
        map.tenant = padTenant(c, appId.tenant)
        Long maxRev = null
        String stmt = """\
/* rollbackCubes.findRollbackRevisionStatus */
SELECT h.revision_number FROM
(SELECT revision_number, head_sha1, create_dt FROM n_cube
WHERE ${buildNameCondition('n_cube_nm')} = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status
AND tenant_cd = :tenant AND branch_id = :branch AND sha1 = head_sha1) b
JOIN n_cube h ON h.sha1 = b.head_sha1
WHERE h.app_cd = :app AND h.branch_id = 'HEAD' AND h.tenant_cd = :tenant AND h.create_dt <= b.create_dt
ORDER BY ABS(b.revision_number) DESC, ABS(h.revision_number) DESC"""

        sql.eachRow(map, stmt, 0, 1, { ResultSet row ->
            maxRev = row.getLong('revision_number')
        })
        return maxRev != null && maxRev >= 0
    }

    private static Long findRollbackRevision(Connection c, ApplicationID appId, String cubeName)
    {
        Sql sql = getSql(c)
        Map map = appId as Map
        map.cube = buildName(cubeName)
        map.tenant = padTenant(c, appId.tenant)
        Long maxRev = null

        sql.eachRow(map, """\
/* rollbackCubes.findRollbackRev */
SELECT revision_number FROM n_cube
WHERE ${buildNameCondition('n_cube_nm')} = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status
AND tenant_cd = :tenant AND branch_id = :branch AND revision_number >= 0 AND sha1 = head_sha1
ORDER BY revision_number desc""", 0, 1, { ResultSet row ->
            maxRev = row.getLong("revision_number")
        })
        return maxRev
    }

    /**
     * Fast forward branch cube to HEAD cube, because even though it's HEAD_SHA-1 value is out-of-date,
     * the cubes current SHA-1 is the same as the HEAD cube's SHA-1.  Therefore, we can 'scoot' up the
     * cube record's HEAD-SHA-1 value to the same as the HEAD Cube's SHA-1.
     * In addition, set the changed flag based on whether SHA-1 matches HEAD SHA-1.
     */
    static boolean updateBranchCubeHeadSha1(Connection c, Long cubeId, String branchSha1, String headSha1)
    {
        if (cubeId == null)
        {
            throw new IllegalArgumentException("Update branch cube's HEAD SHA-1, cube id cannot be empty")
        }

        if (isEmpty(branchSha1))
        {
            throw new IllegalArgumentException("Update branch cube's SHA-1 cannot be empty")
        }

        if (isEmpty(headSha1))
        {
            throw new IllegalArgumentException("Update branch cube's HEAD SHA-1, SHA-1 cannot be empty")
        }

        Map map = [sha1:headSha1, id: cubeId]
        int changed = equalsIgnoreCase(branchSha1, headSha1) ? 0 : 1
        Sql sql = getSql(c)
        int count = sql.executeUpdate(map, "/* updateBranchCubeHeadSha1 */ UPDATE n_cube set head_sha1 = :sha1, changed = ${changed} WHERE n_cube_id = :id")
        if (count == 0)
        {
            throw new IllegalArgumentException("error updating branch cube: ${cubeId}, to HEAD SHA-1: ${headSha1}, no record found.")
        }
        if (count != 1)
        {
            throw new IllegalStateException("error updating branch cube: ${cubeId}, to HEAD SHA-1: ${headSha1}, more than one record found: ${count}")
        }
        return true
    }

    static boolean mergeAcceptMine(Connection c, ApplicationID appId, String cubeName, String username)
    {
        ApplicationID headId = appId.asHead()
        Long headRevision = null
        String headSha1 = null

        Map<String, Object> options = [(SEARCH_EXACT_MATCH_NAME): true,
                                       (METHOD_NAME) : 'mergeAcceptMine'] as Map

        runSelectCubesStatement(c, headId, cubeName, options, 1, { ResultSet row ->
            headRevision = row.getLong('revision_number')
            headSha1 = row.getString('sha1')
        })

        if (headRevision == null)
        {
            throw new IllegalStateException("failed to update branch cube because HEAD cube does not exist: ${cubeName}, app: ${appId}")
        }

        Long newRevision = null
        String tipBranchSha1 = null
        byte[] myTestData = null
        byte[] myBytes = null
        boolean changed = false
        options[SEARCH_INCLUDE_CUBE_DATA] = true
        options[SEARCH_INCLUDE_TEST_DATA] = true
        options[METHOD_NAME] = 'mergeAcceptMine'

        runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row ->
            myBytes = row.getBytes(CUBE_VALUE_BIN)
            myTestData = row.getBytes(TEST_DATA_BIN)
            newRevision = row.getLong('revision_number')
            tipBranchSha1 = row.getString('sha1')
            changed = row.getBoolean(CHANGED)
        })

        if (newRevision == null)
        {
            throw new IllegalStateException("failed to update branch cube because branch cube does not exist: ${cubeName}, app: ${appId}")
        }

        String notes = 'merge: branch accepted over head'
        Long rev = Math.abs(newRevision as long) + 1L
        insertCube(c, appId, cubeName, newRevision < 0 ? -rev : rev, myBytes, myTestData, notes, changed, tipBranchSha1, headSha1, username, 'mergeAcceptMine')
        return true
    }

    static boolean mergeAcceptTheirs(Connection c, ApplicationID appId, String cubeName, String sourceBranch, String username)
    {
        ApplicationID sourceId = appId.asBranch(sourceBranch)
        byte[] sourceBytes = null
        Long sourceRevision = null
        byte[] sourceTestData = null
        String sourceSha1 = null
        String sourceHeadSha1 = null
        boolean sourceChanged = false

        Map<String, Object> options = [
                (SEARCH_INCLUDE_CUBE_DATA):true,
                (SEARCH_INCLUDE_TEST_DATA):true,
                (SEARCH_EXACT_MATCH_NAME):true,
                (METHOD_NAME) : 'mergeAcceptTheirs'] as Map

        runSelectCubesStatement(c, sourceId, cubeName, options, 1, { ResultSet row ->
            sourceBytes = row.getBytes(CUBE_VALUE_BIN)
            sourceTestData = row.getBytes(TEST_DATA_BIN)
            sourceRevision = row.getLong('revision_number')
            sourceSha1 = row.getString('sha1')
            sourceHeadSha1 = row.getString('head_sha1')
            sourceChanged = (boolean)row.getBoolean(CHANGED)
        })

        if (sourceRevision == null)
        {
            throw new IllegalStateException("Failed to overwrite cube in your branch, because ${cubeName} does not exist in ${sourceId}")
        }

        Long newRevision = null
        String targetHeadSha1 = null

        // Do not use cube_value_bin, test data, or notes to speed up search
        Map<String, Object> options1 = [(SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'mergeAcceptTheirs'] as Map
        runSelectCubesStatement(c, appId, cubeName, options1, 1, { ResultSet row ->
            newRevision = row.getLong('revision_number')
            targetHeadSha1 = row.getString('head_sha1')
        })

        String actualHeadSha1 = null

        // Do not use cube_value_bin, test data, or notes to speed up search
        Map<String, Object> options2 = [(SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'mergeAcceptTheirs'] as Map
        runSelectCubesStatement(c, appId.asHead(), cubeName, options2, 1, { ResultSet row ->
            actualHeadSha1 = row.getString('sha1')
        })

        String notes = "merge: ${sourceBranch} accepted over branch"
        long rev = newRevision == null ? 0L : Math.abs(newRevision as long) + 1L
        rev = sourceRevision < 0 ? -rev : rev

        String headSha1
        boolean changed = false
        if (sourceBranch == ApplicationID.HEAD)
        {
            headSha1 = sourceSha1
        }
        else if (equalsIgnoreCase(sourceSha1, actualHeadSha1))
        {
            headSha1 = actualHeadSha1
        }
        else
        {
            headSha1 = targetHeadSha1
            changed = true
        }

        if ((sourceRevision < 0 != newRevision < 0) && equalsIgnoreCase(sourceSha1, sourceHeadSha1))
        {
            changed = true
        }

        insertCube(c, appId, cubeName, rev, sourceBytes, sourceTestData, notes, changed, sourceSha1, headSha1, username, 'mergeAcceptTheirs')
        return true
    }

    /**
     * @param c Connection (JDBC) from ConnectionProvider
     * @param appId ApplicationID
     * @param namePattern String name pattern (using wildcards * and ?)
     * @param options map with possible keys:
     *                changedRecordsOnly - default false
     *                activeRecordsOnly - default false
     *                deletedRecordsOnly - default false
     *                includeCubeData - default false
     *                includeTestData - default false
     *                exactMatchName - default false
     * @param closure Closure to run for each record selected.
     */
    protected static void runSelectCubesStatement(Connection c, ApplicationID appId, String namePattern, Map options, int max = 0, Closure closure)
    {
        boolean includeCubeData = convertToBoolean(options[SEARCH_INCLUDE_CUBE_DATA])
        boolean includeTestData = convertToBoolean(options[SEARCH_INCLUDE_TEST_DATA])
        boolean onlyTestData = convertToBoolean(options[SEARCH_ONLY_TEST_DATA])
        boolean changedRecordsOnly = convertToBoolean(options[SEARCH_CHANGED_RECORDS_ONLY])
        boolean activeRecordsOnly = convertToBoolean(options[SEARCH_ACTIVE_RECORDS_ONLY])
        boolean deletedRecordsOnly = convertToBoolean(options[SEARCH_DELETED_RECORDS_ONLY])
        boolean exactMatchName = convertToBoolean(options[SEARCH_EXACT_MATCH_NAME])
        Date createDateStart = convertToTimestamp(options[SEARCH_CREATE_DATE_START])
        Date createDateEnd = convertToTimestamp(options[SEARCH_CREATE_DATE_END])
        String methodName = (String)options[METHOD_NAME]
        if (isEmpty(methodName))
        {
            methodName = 'methodNameNotSet'
        }

        if (activeRecordsOnly && deletedRecordsOnly)
        {
            throw new IllegalArgumentException("activeRecordsOnly and deletedRecordsOnly are mutually exclusive options and cannot both be 'true'.")
        }

        namePattern = convertPattern(buildName(namePattern))
        boolean hasNamePattern = hasContent(namePattern)
        String nameCondition1 = ''
        String nameCondition2 = ''
        Map map = appId as Map
        map.name = namePattern
        map.changed = changedRecordsOnly
        map.tenant = padTenant(c, appId.tenant)
        map.createDateStart = createDateStart
        map.createDateEnd = createDateEnd

        if (hasNamePattern)
        {
            nameCondition1 = ' AND ' + buildNameCondition('n_cube_nm') + (exactMatchName ? ' = :name' : ' LIKE :name')
            nameCondition2 = ' AND m.low_name ' + (exactMatchName ? '= :name' : 'LIKE :name')
        }

        String revisionCondition = activeRecordsOnly ? ' AND n.revision_number >= 0' : deletedRecordsOnly ? ' AND n.revision_number < 0' : ''
        String changedCondition = changedRecordsOnly ? ' AND n.changed = :changed' : ''
        String createDateStartCondition = createDateStart ? 'AND n.create_dt >= :createDateStart' : ''
        String createDateEndCondition = createDateEnd ? 'AND n.create_dt <= :createDateEnd' : ''
        String onlyTestDataCondition = onlyTestData ? 'AND n.test_data_bin IS NOT NULL' : ''
        String testCondition = includeTestData ? ', n.test_data_bin' : ''
        String cubeCondition = includeCubeData ? ', n.cube_value_bin' : ''

        Sql sql = getSql(c)

        String select = """\
/* ${methodName} */
SELECT n.n_cube_id, n.n_cube_nm, n.app_cd, n.notes_bin, n.version_no_cd, n.status_cd, n.create_dt, n.create_hid, n.revision_number, n.branch_id, n.changed, n.sha1, n.head_sha1 ${testCondition} ${cubeCondition}
FROM n_cube n,
( SELECT LOWER(n_cube_nm) as low_name, max(abs(revision_number)) AS max_rev
 FROM n_cube
 WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = :tenant AND branch_id = :branch
 ${nameCondition1}
 GROUP BY LOWER(n_cube_nm) ) m
WHERE m.low_name = LOWER(n.n_cube_nm) AND m.max_rev = abs(n.revision_number) AND n.app_cd = :app AND n.version_no_cd = :version AND n.status_cd = :status AND tenant_cd = :tenant AND n.branch_id = :branch
${revisionCondition} ${changedCondition} ${nameCondition2} ${createDateStartCondition} ${createDateEndCondition} ${onlyTestDataCondition}"""

        if (max >= 1)
        {   // Use pre-closure to fiddle with batch fetchSize and to monitor row count
            long count = 0
            sql.eachRow(map, select, 0, max, { ResultSet row ->
                if (count > max)
                {
                    throw new IllegalStateException("More results returned than expected, expecting only ${max}")
                }
                count++
                closure(row)
            })
        }
        else
        {   // Use pre-closure to fiddle with batch fetchSizes
            sql.eachRow(map, select, { ResultSet row ->
                closure(row)
            })
        }
    }

    private static String removePreviousNotesCopyMessage(byte[] notes)
    {
        if (notes)
        {
            String oldNotes = new String(notes, 'UTF-8')
            int copyMsgIdx = oldNotes.lastIndexOf('copied from')
            return copyMsgIdx > -1 ? oldNotes.substring(oldNotes.indexOf(' - ', copyMsgIdx) + 3) : oldNotes
        }
        else
        {
            return new String("".bytes, 'UTF-8')
        }
    }

    static int copyBranch(Connection c, ApplicationID srcAppId, ApplicationID targetAppId, String username)
    {
        if (doCubesExist(c, targetAppId, true, 'copyBranch'))
        {
            throw new IllegalArgumentException("Branch '${targetAppId.branch}' already exists, app: ${targetAppId}")
        }

        int headCount = srcAppId.head ? 0 : copyBranchInitialRevisions(c, srcAppId, targetAppId, username)

        Map<String, Object> options = [(SEARCH_INCLUDE_CUBE_DATA): true,
                                       (SEARCH_INCLUDE_TEST_DATA): true,
                                       (METHOD_NAME) : 'copyBranch'] as Map
        int count = 0
        boolean autoCommit = c.autoCommit
        PreparedStatement insert = null
        try
        {
            c.autoCommit = false
            insert = c.prepareStatement(
                    "/* copyBranch */ INSERT /*+append*/ INTO n_cube (n_cube_id, n_cube_nm, ${CUBE_VALUE_BIN}, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number, changed, sha1, head_sha1) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            runSelectCubesStatement(c, srcAppId, null, options, { ResultSet row ->
                byte[] notes = row.getBytes(NOTES_BIN)
                String oldNotes = removePreviousNotesCopyMessage(notes)
                insert.setLong(1, uniqueId)
                insert.setString(2, row.getString('n_cube_nm'))
                insert.setBytes(3, row.getBytes(CUBE_VALUE_BIN))
                insert.setTimestamp(4, nowAsTimestamp())
                insert.setString(5, username)
                insert.setString(6, targetAppId.version)
                insert.setString(7, ReleaseStatus.SNAPSHOT.name())
                insert.setString(8, targetAppId.app)
                insert.setBytes(9, row.getBytes(TEST_DATA_BIN))
                insert.setBytes(10, getUTF8Bytes("target ${targetAppId} copied from ${srcAppId} - ${oldNotes}"))
                insert.setString(11, targetAppId.tenant)
                insert.setString(12, targetAppId.branch)
                insert.setLong(13, row.getLong('revision_number'))
                insert.setBoolean(14, targetAppId.head ? false : (boolean)row.getBoolean(CHANGED))
                insert.setString(15, row.getString('sha1'))

                String headSha1 = null
                if (!targetAppId.head)
                {
                    headSha1 = row.getString(srcAppId.head ? 'sha1' : 'head_sha1')
                }
                insert.setString(16, headSha1)

                insert.addBatch()
                count++
                if (count % EXECUTE_BATCH_CONSTANT == 0)
                {
                    insert.executeBatch()
                }
            })
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                insert.executeBatch()
            }
            return count + headCount
        }
        finally
        {
            c.autoCommit = autoCommit
            insert?.close()
        }
    }

    private static int copyBranchInitialRevisions(Connection c, ApplicationID srcAppId, ApplicationID targetAppId, String username)
    {
        Map map = srcAppId as Map
        map.tenant = padTenant(c, srcAppId.tenant)

        Sql sql = getSql(c)
        String select = """SELECT n.n_cube_id, n.n_cube_nm, n.app_cd, n.notes_bin, n.cube_value_bin, n.test_data_bin,
                n.version_no_cd, n.status_cd, n.create_dt, n.create_hid, n.revision_number, n.branch_id, n.changed, n.sha1
        FROM (SELECT x.n_cube_id, x.n_cube_nm, x.app_cd, x.notes_bin, x.cube_value_bin, x.test_data_bin,
                x.version_no_cd, x.status_cd, x.create_dt, x.create_hid, x.revision_number, x.branch_id, x.changed, x.sha1
            FROM n_cube x
            JOIN (SELECT n_cube_nm, MAX(ABS(revision_number)) AS max_rev
                            FROM n_cube
                            WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND branch_id = 'HEAD' GROUP BY n_cube_nm) y
                    ON LOWER(x.n_cube_nm) = LOWER(y.n_cube_nm) AND ABS(x.revision_number) = y.max_rev
                            WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND branch_id = 'HEAD') n,
        (SELECT x.n_cube_nm, x.head_sha1, x.sha1
            FROM n_cube x
            JOIN (SELECT n_cube_nm, MAX(ABS(revision_number)) AS max_rev
                            FROM n_cube
                            WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND branch_id = :branch GROUP BY n_cube_nm) y
                    ON LOWER(x.n_cube_nm) = LOWER(y.n_cube_nm) AND ABS(x.revision_number) = y.max_rev
                            WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND branch_id = :branch) m
        WHERE LOWER(m.n_cube_nm) = LOWER(n.n_cube_nm) AND n.app_cd = :app AND n.version_no_cd = :version AND n.status_cd = :status
                AND n.branch_id = 'HEAD' AND n.sha1 = m.head_sha1 AND m.head_sha1 <> m.sha1"""

        int count = 0
        boolean autoCommit = c.autoCommit
        PreparedStatement insert = null
        try
        {
            c.autoCommit = false
            insert = c.prepareStatement(
                    "/* copyBranch */ INSERT /*+append*/ INTO n_cube (n_cube_id, n_cube_nm, ${CUBE_VALUE_BIN}, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number, changed, sha1, head_sha1) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            sql.eachRow(map, select, { ResultSet row ->
                byte[] notes = row.getBytes(NOTES_BIN)
                String oldNotes = removePreviousNotesCopyMessage(notes)
                String sha1 = row.getString('sha1')
                insert.setLong(1, uniqueId)
                insert.setString(2, row.getString('n_cube_nm'))
                insert.setBytes(3, row.getBytes(CUBE_VALUE_BIN))
                insert.setTimestamp(4, nowAsTimestamp())
                insert.setString(5, username)
                insert.setString(6, targetAppId.version)
                insert.setString(7, ReleaseStatus.SNAPSHOT.name())
                insert.setString(8, targetAppId.app)
                insert.setBytes(9, row.getBytes(TEST_DATA_BIN))
                insert.setBytes(10, getUTF8Bytes("target ${targetAppId} copied from ${srcAppId} - ${oldNotes}"))
                insert.setString(11, targetAppId.tenant)
                insert.setString(12, targetAppId.branch)
                insert.setLong(13, (row.getLong('revision_number') >= 0) ? 0 : -1)
                insert.setBoolean(14, targetAppId.head ? false : (boolean)row.getBoolean(CHANGED))
                insert.setString(15, sha1)
                insert.setString(16, targetAppId.head ? null : sha1)

                insert.addBatch()
                count++
                if (count % EXECUTE_BATCH_CONSTANT == 0)
                {
                    insert.executeBatch()
                }
            })
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                insert.executeBatch()
            }
            return count
        }
        finally
        {
            c.autoCommit = autoCommit
            insert?.close()
        }
    }

    static int copyBranchWithHistory(Connection c, ApplicationID srcAppId, ApplicationID targetAppId, String username)
    {
        if (doCubesExist(c, targetAppId, true, 'copyBranch'))
        {
            throw new IllegalStateException("Branch '${targetAppId.branch}' already exists, app: ${targetAppId}")
        }

        Map<String, Object> options = [(METHOD_NAME): 'copyBranch'] as Map
        int count = 0
        boolean autoCommit = c.autoCommit
        PreparedStatement insert = null
        try
        {
            c.autoCommit = false
            insert = c.prepareStatement(
                    "/* copyBranchWithHistory */ INSERT /*+append*/ INTO n_cube (n_cube_id, n_cube_nm, ${CUBE_VALUE_BIN}, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number, changed, sha1, head_sha1) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            runSelectAllCubesInBranch(c, srcAppId, options, { ResultSet row ->
                String sha1 = row.getString('sha1')
                byte[] notes = row.getBytes(NOTES_BIN)
                String oldNotes = removePreviousNotesCopyMessage(notes)
                insert.setLong(1, uniqueId)
                insert.setString(2, row.getString('n_cube_nm'))
                insert.setBytes(3, row.getBytes(CUBE_VALUE_BIN))
                insert.setTimestamp(4, row.getTimestamp('create_dt'))
                insert.setString(5, username)
                insert.setString(6, targetAppId.version)
                insert.setString(7, ReleaseStatus.SNAPSHOT.name())
                insert.setString(8, targetAppId.app)
                insert.setBytes(9, row.getBytes(TEST_DATA_BIN))
                insert.setBytes(10, getUTF8Bytes("target ${targetAppId} full copied from ${srcAppId} - ${oldNotes}"))
                insert.setString(11, targetAppId.tenant)
                insert.setString(12, targetAppId.branch)
                insert.setLong(13, row.getLong('revision_number'))
                insert.setBoolean(14, targetAppId.head ? false : (boolean)row.getBoolean(CHANGED))
                insert.setString(15, sha1)

                String headSha1 = null
                if (!targetAppId.head)
                {
                    headSha1 = row.getString(srcAppId.head ? 'sha1' : 'head_sha1')
                }
                insert.setString(16, headSha1)

                insert.addBatch()
                count++
                if (count % EXECUTE_BATCH_CONSTANT == 0)
                {
                    insert.executeBatch()
                }
            })
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {   // complete last batch
                insert.executeBatch()
            }
            return count
        }
        finally
        {
            c.autoCommit = autoCommit
            insert?.close()
        }
    }

    protected static void runSelectAllCubesInBranch(Connection c, ApplicationID appId, Map options, Closure closure)
    {
        String methodName = (String)options[METHOD_NAME]
        if (isEmpty(methodName))
        {
            methodName = 'methodNameNotSet'
        }

        Map map = appId as Map
        map.tenant = padTenant(c, appId.tenant)

        Sql sql = getSql(c)
        String select = """\
/* ${methodName}.runSelectAllCubesInBranch */
SELECT n_cube_nm, notes_bin, create_dt, create_hid, revision_number, changed, sha1, head_sha1, test_data_bin, ${CUBE_VALUE_BIN}
FROM n_cube
WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = :tenant AND branch_id = :branch"""

        sql.eachRow(map, select, { ResultSet row ->
            closure(row)
        })
    }

    static boolean deleteBranch(Connection c, ApplicationID appId)
    {
        Map map = appId as Map
        map.tenant = padTenant(c, appId.tenant)
        Sql sql = getSql(c)
        sql.execute(map, "/* deleteBranch */ DELETE FROM n_cube WHERE app_cd = :app AND version_no_cd = :version AND tenant_cd = :tenant AND branch_id = :branch")
        GroovyRowResult row = sql.firstRow(map, "/* deleteBranch */ SELECT count(1) FROM n_cube WHERE app_cd = :app AND version_no_cd != '0.0.0' AND status_cd = 'SNAPSHOT' AND tenant_cd = :tenant AND branch_id = :branch")
        if (!row[0])
        {
            sql.execute(map, "/* deleteBranch */ DELETE FROM n_cube WHERE app_cd = :app AND version_no_cd = '0.0.0' AND status_cd = 'SNAPSHOT' AND tenant_cd = :tenant AND branch_id = :branch")
        }
        return true
    }

    static boolean deleteApp(Connection c, ApplicationID appId)
    {
        Map<String, List<String>> versions = getVersions(c, appId.tenant, appId.app)
        if (!versions[ReleaseStatus.RELEASE.name()].empty)
        {
            throw new IllegalArgumentException("Only applications without a released version can be deleted, app: ${appId}")
        }
        Map map = [app: appId.app, tenant: padTenant(c, appId.tenant)]
        Sql sql = getSql(c)
        sql.execute(map, "/* deleteApp */ DELETE FROM n_cube WHERE app_cd = :app AND tenant_cd = :tenant AND status_cd = 'SNAPSHOT'")
        return true
    }

    static int moveBranch(Connection c, ApplicationID appId, String newSnapVer)
    {
        if (ApplicationID.HEAD == appId.branch)
        {
            throw new IllegalArgumentException('Cannot use moveBranch() API on HEAD branch')
        }

        // Move SNAPSHOT branch cubes from one version to another version.
        Map map = appId as Map
        map.newVer = newSnapVer
        map.tenant = padTenant(c, appId.tenant)
        Sql sql = getSql(c)
        return sql.executeUpdate(map, "/* moveBranch */ UPDATE n_cube SET version_no_cd = :newVer WHERE app_cd = :app AND version_no_cd = :version AND tenant_cd = :tenant AND branch_id = :branch")
    }

    static int releaseCubes(Connection c, ApplicationID appId)
    {
        // Step 1: Release cubes where branch == HEAD (change their status from SNAPSHOT to RELEASE)
        Sql sql = getSql(c)
        Map map = appId as Map
        map.tenant = padTenant(c, appId.tenant)
        return sql.executeUpdate(map, "/* releaseCubes */ UPDATE n_cube SET status_cd = 'RELEASE' WHERE app_cd = :app AND version_no_cd = :version AND status_cd = 'SNAPSHOT' AND tenant_cd = :tenant AND branch_id = 'HEAD'")
    }

    static int changeVersionValue(Connection c, ApplicationID appId, String newVersion)
    {
        ApplicationID newSnapshot = appId.createNewSnapshotId(newVersion)
        if (doCubesExist(c, newSnapshot, true, 'changeVersionValue'))
        {
            throw new IllegalStateException("Cannot change version value to ${newVersion} because cubes with this version already exists.  Choose a different version number, app: ${appId}")
        }

        Map map = appId as Map
        map.newVer = newVersion
        map.status = 'SNAPSHOT'
        map.tenant = padTenant(c, appId.tenant)
        Sql sql = getSql(c)
        int count = sql.executeUpdate(map, "/* changeVersionValue */ UPDATE n_cube SET version_no_cd = :newVer WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = :tenant AND branch_id = :branch")
        if (count < 1)
        {
            throw new IllegalArgumentException("No SNAPSHOT n-cubes found with version ${appId.version}, therefore no versions updated, app: ${appId}")
        }
        return count
    }

    static Map getAppTestData(Connection c, ApplicationID appId)
    {
        Map ret = [:]

        Map<String, Object> options = [(SEARCH_INCLUDE_CUBE_DATA): false,
                                       (SEARCH_INCLUDE_TEST_DATA): true,
                                       (SEARCH_ONLY_TEST_DATA): true,
                                       (SEARCH_ACTIVE_RECORDS_ONLY): true,
                                       (METHOD_NAME) : 'getAppTestData'] as Map
        runSelectCubesStatement(c, appId, null, options, { ResultSet row ->
            String cubeName = row.getString('n_cube_nm')
            if (!ret.containsKey(cubeName))
            {
                ret[cubeName] = new String(row.getBytes(TEST_DATA_BIN), 'UTF-8')
            }
        })

        return ret
    }

    static String getTestData(Connection c, Long cubeId)
    {
        Map map = [cubeId: cubeId]
        String select = "/* getTestData */ SELECT test_data_bin FROM n_cube WHERE n_cube_id = :cubeId"
        String msg = "Could not fetch test data for cube with id ${cubeId}"
        return fetchTestData(c, map, select, msg)
    }

    static String getTestData(Connection c, ApplicationID appId, String cubeName)
    {
        Map map = appId as Map
        map.cube = buildName(cubeName)
        map.tenant = padTenant(c, appId.tenant)

        String select = """\
/* getTestData */
SELECT test_data_bin FROM n_cube
WHERE ${buildNameCondition('n_cube_nm')} = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = :tenant AND branch_id = :branch
ORDER BY abs(revision_number) DESC"""

        String msg = "Could not fetch test data, cube: ${cubeName} does not exist in app: ${appId}"
        return fetchTestData(c, map, select, msg)
    }

    private static String fetchTestData(Connection c, Map map, String select, String msg)
    {
        Sql sql = getSql(c)
        byte[] testBytes = null
        boolean found = false

        sql.eachRow(select, map, 0, 1, { ResultSet row ->
            testBytes = row.getBytes(TEST_DATA_BIN)
            found = true
        })

        if (!found)
        {
            throw new IllegalArgumentException(msg)
        }
        return testBytes == null ? '' : new String(testBytes, "UTF-8")
    }

    static boolean updateNotes(Connection c, ApplicationID appId, String cubeName, String notes)
    {
        Long maxRev = getMaxRevision(c, appId, cubeName, 'updateNotes')
        if (maxRev == null)
        {
            throw new IllegalArgumentException("Cannot update notes, cube: ${cubeName} does not exist in app: ${appId}")
        }

        Map map = appId as Map
        map.notes = getUTF8Bytes(notes)
        map.status = ReleaseStatus.SNAPSHOT.name()
        map.rev = maxRev
        map.cube = buildName(cubeName)
        map.tenant = padTenant(c, appId.tenant)
        Sql sql = getSql(c)

        int rows = sql.executeUpdate(map, """\
/* updateNotes */
UPDATE n_cube SET notes_bin = :notes
WHERE app_cd = :app AND ${buildNameCondition('n_cube_nm')} = :cube AND version_no_cd = :version
AND status_cd = :status AND tenant_cd = :tenant AND branch_id = :branch AND revision_number = :rev""")
        return rows == 1
    }

    static List<String> getAppNames(Connection c, String tenant)
    {
        if (isEmpty(tenant))
        {
            throw new IllegalArgumentException("error calling getAppVersions(), tenant cannot be null or empty")
        }
        Map map = [tenant: padTenant(c, tenant), sysinfo: SYS_INFO]
        Sql sql = getSql(c)
        List<String> apps = []

        sql.eachRow("/* getAppNames */ SELECT DISTINCT app_cd FROM n_cube WHERE tenant_cd = :tenant AND n_cube_nm = :sysinfo", map, { ResultSet row ->
            apps.add(row.getString('app_cd'))
        })
        return apps
    }

    static Map<String, List<String>> getVersions(Connection c, String tenant, String app)
    {
        if (isEmpty(tenant) || isEmpty(app))
        {
            throw new IllegalArgumentException("error calling getAppVersions() tenant: ${tenant} or app: ${app} cannot be null or empty")
        }
        Sql sql = getSql(c)
        Map map = [tenant: padTenant(c, tenant), app:app, sysinfo: SYS_INFO]
        List<String> releaseVersions = []
        List<String> snapshotVersions = []
        Map<String, List<String>> versions = [:]

        sql.eachRow("/* getVersions */ SELECT DISTINCT version_no_cd, status_cd FROM n_cube WHERE tenant_cd = :tenant AND app_cd = :app AND n_cube_nm = :sysinfo", map, { ResultSet row ->
            String version = row.getString('version_no_cd')
            if (ReleaseStatus.RELEASE.name() == row.getString('status_cd'))
            {
                releaseVersions.add(version)
            }
            else
            {
                snapshotVersions.add(version)
            }
        })
        versions[ReleaseStatus.SNAPSHOT.name()] = snapshotVersions
        versions[ReleaseStatus.RELEASE.name()] = releaseVersions
        return versions
    }

    static Set<String> getBranches(Connection c, ApplicationID appId)
    {
        Map map = appId as Map
        map.tenant = padTenant(c, appId.tenant)
        map.sysinfo = SYS_INFO
        Sql sql = getSql(c)
        Set<String> branches = new TreeSet<>(String.CASE_INSENSITIVE_ORDER)

        sql.eachRow("/* getBranches.appVerStat */ SELECT DISTINCT branch_id FROM n_cube WHERE tenant_cd = :tenant AND app_cd = :app AND version_no_cd = :version AND status_cd = :status AND n_cube_nm = :sysinfo", map, { ResultSet row ->
            branches.add(row.getString('branch_id'))
        })
        return branches
    }

    /**
     * Check for existence of a cube with this appId.  You can ignoreStatus if you want to check for existence of
     * a SNAPSHOT or RELEASE cube.
     * @param ignoreStatus - If you want to ignore status (check for both SNAPSHOT and RELEASE cubes in existence) pass
     *                     in true.
     * @return true if any cubes exist for the given AppId, false otherwise.
     */
    static boolean doCubesExist(Connection c, ApplicationID appId, boolean ignoreStatus, String methodName)
    {
        Map map = appId as Map
        map.tenant = padTenant(c, appId.tenant)
        map.sysinfo = SYS_INFO
        Sql sql = getSql(c)
        String statement = "/* ${methodName}.doCubesExist */ SELECT 1 FROM n_cube WHERE app_cd = :app AND version_no_cd = :version AND tenant_cd = :tenant AND branch_id = :branch AND n_cube_nm = :sysinfo"

        if (!ignoreStatus)
        {
            statement += ' AND status_cd = :status'
        }

        statement += addLimitingClause(c)

        boolean result = false
        sql.eachRow(statement, map, 0, 1, { ResultSet row -> result = true })
        return result
    }

    static Long getMaxRevision(Connection c, ApplicationID appId, String cubeName, String methodName)
    {
        Map map = appId as Map
        map.cube = buildName(cubeName)
        map.tenant = padTenant(c, appId.tenant)
        Sql sql = getSql(c)
        Long rev = null
        String select
        select = """\
/* ${methodName}.maxRev */ SELECT revision_number FROM n_cube
WHERE ${buildNameCondition("n_cube_nm")} = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = :tenant AND branch_id = :branch
ORDER BY abs(revision_number) DESC ${addLimitingClause(c)}"""

        sql.eachRow(select, map, 0, 1, { ResultSet row ->
            rev = row.getLong('revision_number')
        })
        return rev
    }

    private static String addLimitingClause(Connection c)
    {
        if (isOracle(c))
        {
            return ' FETCH FIRST 1 ROW ONLY'
        }
        else if (isMySQL(c) || isHSQLDB(c))
        {
            return ' LIMIT 1'
        }
        return ''
    }

    protected static NCubeInfoDto getCubeInfoRecords(ApplicationID appId, Pattern searchPattern, List<NCubeInfoDto> list, Map options, ResultSet row, boolean keepCubeData = true)
    {
        boolean hasSearchPattern = searchPattern != null
        Set<String> includeFilter = options[SEARCH_FILTER_INCLUDE] as Set
        Set<String> excludeFilter = options[SEARCH_FILTER_EXCLUDE] as Set

        if (hasSearchPattern || includeFilter || excludeFilter)
        {   // Only read CUBE_VALUE_BIN if needed (searching content or filtering by cube_tags)
            byte[] bytes = uncompressBytes(row.getBytes(CUBE_VALUE_BIN))
            blankOutName(bytes, row.getString('n_cube_nm'))
            String cubeData = createUTF8String(bytes)
            bytes = null // Clear out early (memory friendly for giant NCubes)

            if (includeFilter || excludeFilter)
            {
                Matcher tagMatcher = cubeData =~ /"$CUBE_TAGS"\s*:\s*(?:"|\{.*?value":")?(?<tags>.*?)"/
                Set<String> cubeTags = tagMatcher ? getFilter(tagMatcher.group('tags')) : new HashSet<String>()

                Closure tagsMatchFilter = { Set<String> filter ->
                    Set<String> copyTags = new CaseInsensitiveSet<>(cubeTags)
                    copyTags.retainAll(filter)
                    return !copyTags.empty
                }

                if ((includeFilter && !tagsMatchFilter(includeFilter)) // search by include tag but doesn't match
                    || (excludeFilter && tagsMatchFilter(excludeFilter))) // search by exclude tag and matches
                {   // exclude cube from search
                    return null
                }
            }

            if (hasSearchPattern)
            {
                if (!searchPattern.matcher(cubeData).find())
                {   // Did not contains-match content pattern
                    // check if cube has reference axes before returning, as the value may exist on a referenced column
                    if (Regexes.refAppSearchPattern.matcher(cubeData).find())
                    {
                        boolean foundInRefAxColumn = false
                        NCube cube
                        try
                        {   // cube will fail to load if a reference axis is in an invalid state
                            cube = NCube.fromSimpleJson(cubeData)
                        }
                        catch (IllegalStateException e)
                        {   // log the error, but keep searching
                            log.error(e.message, e)
                            return null
                        }
                        for (Axis axis : cube.axes)
                        {
                            if (axis.reference)
                            {
                                for (Column column : axis.columnsWithoutDefault)
                                {
                                    if (searchPattern.matcher(column.value.toString()).find() || (column.columnName != null && searchPattern.matcher(column.columnName).find()))
                                    {
                                        foundInRefAxColumn = true
                                        break
                                    }
                                }
                                if (foundInRefAxColumn)
                                {
                                    break
                                }
                            }
                        }
                        if (!foundInRefAxColumn)
                        {
                            return null
                        }
                    }
                    else
                    {
                        return null
                    }
                }
            }
        }

        NCubeInfoDto dto = createDtoFromRow(row, options)
        dto.tenant = appId.tenant
        if (SYS_INFO != dto.name || options[SEARCH_ALLOW_SYS_INFO])
        {
            list.add(dto)
        }
        Closure closure = (Closure)options[SEARCH_CLOSURE]
        if (closure)
        {
            closure(dto, options[SEARCH_OUTPUT])
        }
        if (!keepCubeData)
        {   // Although possibly used for searching contents, clear cube data from DTO unless they specifically requested it.
            dto.bytes = null
            dto.testData = null
        }
        return dto
    }

    /**
     * Locate cube name within bytes, and set to hyphens in byte[].  This is used to
     * prevent the cube name from matching content searches.
     */
    private static void blankOutName(byte[] bytes, String cubeName)
    {
        String json
        if (bytes.length < 4096)
        {
            json = createUTF8String(bytes)
        }
        else
        {   // Make new String out of partial piece of original (don't want to duplicate giant JSON strings)
            json = new String(bytes, 0, 4096)
        }

        int start = json.indexOf("\"${cubeName}\"")
        if (start == -1)
        {
            return
        }

        int end = start + 1 + cubeName.length()
        for (int i = start + 1; i < end; i++)
        {
            bytes[i] = 45   // UTF-8 / ASCII hyphen
        }
    }

    private static NCubeInfoDto createDtoFromRow(ResultSet row, Map options)
    {
        NCubeInfoDto dto = new NCubeInfoDto()
        dto.id = row.getString('n_cube_id')
        dto.name = row.getString('n_cube_nm')
        dto.branch = row.getString('branch_id')
        byte[] notes = null
        try
        {
            notes = row.getBytes(NOTES_BIN)
        }
        catch (Exception ignored) { }
        dto.notes = new String(notes ?: "".bytes, 'UTF-8')
        dto.version = row.getString('version_no_cd')
        dto.status = row.getString('status_cd')
        dto.app = row.getString('app_cd')
        dto.createDate = new Date(row.getTimestamp('create_dt').time)
        dto.createHid = row.getString('create_hid')
        dto.revision = row.getString('revision_number')
        dto.changed = row.getBoolean(CHANGED)
        dto.sha1 = row.getString('sha1')
        dto.headSha1 = row.getString('head_sha1')
        if (options[SEARCH_INCLUDE_CUBE_DATA])
        {
            dto.bytes = options[SEARCH_CHECK_SHA1]==dto.sha1 ? (byte[])null : row.getBytes(CUBE_VALUE_BIN)
        }
        if (options[SEARCH_INCLUDE_TEST_DATA])
        {
            byte[] testBytes = row.getBytes(TEST_DATA_BIN)
            if (testBytes)
            {
                dto.testData = new String(testBytes, 'UTF-8')
            }
        }
        return dto
    }

    static void clearTestDatabase(Connection c)
    {
        if (isHSQLDB(c))
        {
            Sql sql = getSql(c)
            sql.execute('/* Clear HSQLDB */ DELETE FROM n_cube')
        }
    }

    protected static NCube buildCube(ApplicationID appId, ResultSet row, boolean includeTestData = false)
    {
        NCube ncube = NCube.createCubeFromStream(row.getBinaryStream(CUBE_VALUE_BIN))
        ncube.sha1 = row.getString('sha1')
        ncube.applicationID = appId
        if (includeTestData)
        {
            byte[] testBytes = row.getBytes(TEST_DATA_BIN)
            if (testBytes)
            {
                String s = new String(testBytes, "UTF-8")
                ncube.testData = NCubeTestReader.convert(s).toArray()
            }
        }
        return ncube
    }

    // ------------------------------------------ local non-JDBC helper methods ----------------------------------------

    protected static String createNote(String user, Date date, String notes)
    {
        return "${DATE_TIME_FORMAT.format(date)} [${user}] ${notes}"
    }

    private static String convertPattern(String pattern)
    {
        if (isEmpty(pattern) || '*' == pattern)
        {
            return null
        }
        else
        {
            pattern = pattern.replace('*', '%')
            pattern = pattern.replace('?', '_')
        }
        return pattern
    }

    private static String buildNameCondition(String name)
    {
        return "LOWER(${name})"
    }

    private static String buildName(String name)
    {
        return name?.toLowerCase()
    }

    private static Timestamp nowAsTimestamp()
    {
        return new Timestamp(System.currentTimeMillis())
    }

    private static String padTenant(Connection c, String tenant)
    {
        return isOracle(c) ? tenant.padRight(10, ' ') : tenant
    }

    static boolean isOracle(Connection c)
    {
        if (c == null)
        {
            return false
        }

        if (isOracle == null)
        {
            isOracle = new AtomicBoolean(Regexes.isOraclePattern.matcher(c.metaData.driverName).find())
            log.info("Oracle JDBC driver: ${isOracle.get()}")
        }
        return isOracle.get()
    }

    static boolean isHSQLDB(Connection c)
    {
        if (c == null)
        {
            return false
        }

        if (isHSQLDB == null)
        {
            isHSQLDB = new AtomicBoolean(Regexes.isHSQLDBPattern.matcher(c.metaData.driverName).find())
            log.info("HSQLDB JDBC driver: ${isHSQLDB.get()}")
        }

        return isHSQLDB.get()
    }

    static boolean isMySQL(Connection c)
    {
        if (c == null)
        {
            return false
        }

        if (isMySQL == null)
        {
            isMySQL = new AtomicBoolean(Regexes.isMySQLPattern.matcher(c.metaData.driverName).find())
            log.info("MySQL JDBC driver: ${isMySQL.get()}")
        }
        return isMySQL.get()
    }

    /**
     * Given the unknown way of specifying tags, create a Collection of tags from the input.  This API
     * handles String (Command and space delimited), a Collection or Strings, or a Map of Strings in
     * which case the keySet of the map is used.
     * @param filter String, Collection, or Map of String tags.  If it is a String, they are expected to be
     * comma and/or space delimited.
     * @return CaseInsensitiveSet<String> of tags
     */
    private static CaseInsensitiveSet<String> getFilter(def filter)
    {
        if (filter instanceof String)
        {
            return new CaseInsensitiveSet<String>(filter.tokenize(', '))
        }
        else if (filter instanceof Collection)
        {
            Collection items = filter as Collection
            Set<String> tags = new CaseInsensitiveSet<>()
            items.each { tag -> safeAdd(tag, tags) }
            return tags
        }
        else if (filter instanceof Map)
        {
            Map map = filter as Map
            Set<String> tags = new CaseInsensitiveSet<>()

            if (map.containsKey('type') && map.containsKey('value'))
            {
                CellInfo cellInfo = new CellInfo(map.type as String, map.value as String, false, false)
                def item = cellInfo.recreate()  // recreate to original Java value (String, Boolean, Double, etc.)
                safeAdd(item, tags)
            }
            else
            {   // Use keys
                map.keySet().each { key -> safeAdd(key, tags) }
            }
            return tags
        }
        else if (filter instanceof Object[])
        {
            Set<String> tags = new CaseInsensitiveSet<String>()
            Object[] filterTags = filter as Object[]
            filterTags.each { tag -> safeAdd(tag, tags) }
            return tags
        }
        else
        {
            return new CaseInsensitiveSet<String>()
        }
    }

    /**
     * Best possible add of tag to Set of tags, where passed in tag type is unknown.
     */
    private static void safeAdd(def tag, Set tags)
    {
        if (tag instanceof String)
        {
            tags.addAll((tag as String).tokenize(', '))
        }
        else if (tag instanceof Number)
        {
            tags.add(convertToString(tag))
        }
        else if (tag instanceof Date)
        {
            tags.add(convertToDate(tag))
        }
        else if (tag instanceof Boolean)
        {
            tags.add(tag.toString())
        }
    }

    static Sql getSql(Connection c)
    {
        Sql sql = new Sql(c)
        sql.withStatement { Statement stmt -> stmt.fetchSize = FETCH_SIZE }
        return sql
    }
}
