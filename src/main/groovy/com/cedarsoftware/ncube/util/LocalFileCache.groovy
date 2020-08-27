package com.cedarsoftware.ncube.util

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.SnapshotPolicy
import com.cedarsoftware.ncube.formatters.JsonFormatter
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Value
import org.springframework.cache.Cache
import org.springframework.cache.support.SimpleValueWrapper
import org.springframework.stereotype.Component

import javax.annotation.PostConstruct

import static com.cedarsoftware.ncube.SnapshotPolicy.OFFLINE
import static com.cedarsoftware.ncube.SnapshotPolicy.RELEASE_ONLY

/**
 * @author Greg Morefield (morefigs@yahoo.com)
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
@Component
class LocalFileCache
{
    @Value('${ncube.cache.dir:#{null}}') String cacheDir
    @Value('${ncube.cache.snapshotPolicy:RELEASE_ONLY}') SnapshotPolicy snapshotPolicy

    LocalFileCache() {
    }

    LocalFileCache(String cacheDir, SnapshotPolicy snapshotPolicy) {
        this.cacheDir = cacheDir
        this.snapshotPolicy = snapshotPolicy
    }

    @PostConstruct
    void init()
    {
        if (cacheDir) {
            log.info("NCUBE file cache configured to use dir: {}", cacheDir)
            log.info("NCUBE file cache snapshot policy set to: {}", snapshotPolicy.name())
        }
        else {
            log.info("NCUBE file cache disabled")
        }
    }

    /** Returns true if local file caching is enabled; otherwise, false */
    boolean isEnabled() {
        return cacheDir != null
    }

    /** Returns a String specifying the directory used to store local files */
    String getCacheDir() {
        return cacheDir
    }

    /** Allows the local cache directory to be specified */
    void setCacheDir(String cacheDir) {
        this.cacheDir = cacheDir
    }

    /** Returns the SnapshotPolicy being used by this file cache */
    SnapshotPolicy getSnapshotPolicy() {
        return snapshotPolicy
    }

    /** Sets the SnapShotPolicy to be used by this file cache */
    void setSnapshotPolicy(SnapshotPolicy snapshotPolicy) {
        this.snapshotPolicy = snapshotPolicy
    }

    /**
     * Checks for the requested cube in the local file cache
     * @param appId ApplicationID containing the cube
     * @param cubeName name of NCube
     * @return null, if cube not found; Cache.ValueWrapper with Boolean.FALSE, if cube does not exist; and
     *          Cache.ValueWrapper with NCube, if found
     */
    Cache.ValueWrapper get(ApplicationID appId, String cubeName)
    {
        boolean isSnapshot = appId.snapshot
        if (!cacheDir || (isSnapshot && RELEASE_ONLY==snapshotPolicy)) {
            return null
        }

        // try to load item from cache
        Cache.ValueWrapper valueWrapper = null
        File cacheFile = checkInLocalCache(appId, cubeName)

        boolean isFile = cacheFile.file
        if (isFile && cacheFile.length()>0) {
            try {
                NCube ncube = null
                cacheFile.withInputStream {stream ->
                    ncube = NCube.fromSimpleJson(stream)
                }
                if (!ncube) {
                    throw new IllegalStateException("Failed to build ncube: ${cubeName} from file stream")
                }
                ncube.applicationID = appId
                valueWrapper = new SimpleValueWrapper(ncube)
            }
            catch (Exception e) {
                log.warn("Unable to load ncube {} from {}", cubeName, cacheFile.path, e)
                if (OFFLINE==snapshotPolicy) {
                    throw new IllegalStateException("Failed to load cube: ${cubeName} from offline cache")
                }
            }
        }
        else if (isFile) {
            valueWrapper = new SimpleValueWrapper(Boolean.FALSE)
        }
        else if (OFFLINE==snapshotPolicy) {
            throw new IllegalStateException("Failed to find cube: ${cubeName} in offline cache")
        }
        else {
            // allow valueWrapper to remain null so that a normal load is performed
            log.debug("{} not found", cacheFile.name)
        }
        return valueWrapper
    }

    /**
     * Stores the NCube bytes, if it exists; otherwise, an empty file is persisted
     *
     * @param appId ApplicationID to use when storing the file
     * @param cubeName String containing name of cube
     * @param ncube An instance of NCube, if the cube exists; null, if cube does not exist
     */
    void put(ApplicationID appId, String cubeName, NCube ncube) {
        boolean isSnapshot = appId.snapshot
        if (isSnapshot && RELEASE_ONLY==snapshotPolicy) {
            return
        }

        // use sha1 to write snapshot cubes
        if (isSnapshot) {
            writeSha1ToFile(appId,cubeName,ncube ? ncube.sha1() : '')
            if (ncube) {
                writeCubeToFile(appId, cubeName, ncube.sha1(), ncube)
            }
        }
        else {
            // write cube without sha1 as 'latest'
            writeCubeToFile(appId, cubeName, '', ncube)
        }
    }

    /**
     * Write the specified NCube to a file based on AppId, cube name, and sha1. The file contents
     * will contain the json bytes for cubes that exist. For non-existent cubes, a zero byte file
     * is written.
     *
     * Release cubes are written without sha1 to improve lookup performance
     * Snapshot cubes are written with the sha1 in the file name
     */
    private void writeCubeToFile(ApplicationID appId, String cubeName, String sha1, NCube ncube)
    {
        File cacheFile = getFileForCachedCube(appId, cubeName, sha1)
        if (ensureDirectoryExists(cacheFile.parent)) {
            log.debug("Writing ${cacheFile.absolutePath}")

            try {
                if (ncube) {
                    cacheFile.withOutputStream { stream ->
                        new JsonFormatter(stream).formatCube(ncube,[:])
                    }
                }
                else {
                    cacheFile.bytes = ''.bytes
                }
            }
            catch (Exception e) {
                log.warn("Unable to write ncube {} to file {}", cubeName, cacheFile.path, e)
            }
        }
    }

    /**
     * Write a 'sha1' file that can be used to determine the latest snapshot file. The 'sha1'
     * file will contain the sha1 for cubes that exist. A zero-byte file will be written as
     * the 'sha1' file for cubes that don't exist
     */
    private void writeSha1ToFile(ApplicationID appId, String cubeName, String sha1)
    {
        File sha1File = getFileForCachedSha1(appId,cubeName)
        if (ensureDirectoryExists(sha1File.parent)) {
            log.debug("Writing ${sha1File.absolutePath}")

            try {
                sha1File.bytes = sha1 ? sha1.bytes : ''.bytes
            }
            catch (Exception e) {
                log.warn("Unable to write sha1 for ncube {} to file {}", cubeName, sha1File.path, e)
            }
        }
    }

    /**
     * Check in the file cache for the cube specified. The returned File will be to the json file
     * for release cubes and snapshot files that exist. For snapshot cubes that don't exist, the
     * File will point to the sha1 file.
     *
     * For release cubes that don't exist, the json file will be zero-byte.
     * For snapshot cubes that don't exist, the sha1 file will be zero-byte and no json file will exist.
     */
    private File checkInLocalCache(ApplicationID appId, String cubeName)
    {
        String sha1 = ''
        if (appId.snapshot) {
            File sha1File = getFileForCachedSha1(appId, cubeName)
            try {
                if (sha1File.file && sha1File.length()>0) {
                    sha1 = sha1File.text
                }
            }
            catch (Exception e) {
                log.warn("Unable to read sha1 for ncube {} from {}", cubeName, sha1File?.path, e)
                if (OFFLINE==snapshotPolicy) {
                    throw new IllegalStateException("Failed to load sha1 for cube: ${cubeName} from offline cache")
                }
            }

            if (!sha1) {
                return sha1File    // sha1 file is either missing, couldn't be read, or is empty
            }
        }

        return getFileForCachedCube(appId, cubeName, sha1)
    }

    /**
     * Helper method to build the File for cached json files
     */
    private File getFileForCachedCube(ApplicationID appId, String cubeName, String sha1)
    {
        String suffix = sha1 ? ".${sha1}" : ''
        return new File("${cacheDir}/${appId.cacheKey()}${cubeName.toLowerCase()}${suffix}.json")
    }

    /**
     * Helper method to build the File for cache sha1 files
     */
    private File getFileForCachedSha1(ApplicationID appId, String cubeName)
    {
        return new File("${cacheDir}/${appId.cacheKey()}${cubeName.toLowerCase()}.sha1")
    }

    /**
     * Validates directory existence
     * @param dirPath String path of directory to validate
     * @return true if directory exists; false, otherwise
     * @throws SecurityException if call to mkdirs encounters an issue
     */
    private static boolean ensureDirectoryExists(String dirPath) {
        if (!dirPath) {
            return false
        }

        File dir = new File(dirPath)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        boolean valid = dir.directory
        if (!valid)
        {
            log.warn("Failed to locate or create cache directory with path: ${dir.path}")
        }
        return valid
    }
}
