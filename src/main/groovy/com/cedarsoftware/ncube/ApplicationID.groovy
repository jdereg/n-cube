package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

import java.util.regex.Matcher
import java.util.regex.Pattern

import static com.cedarsoftware.util.StringUtilities.*

/**
 * This class binds together Tenant, App, version, status, and branch.  These fields together
 * completely identify the application that a given n-cube belongs to.
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
@CompileStatic
class ApplicationID
{
    public static final String SYS_BOOT_VERSION = '0.0.0'
    public static final String DEFAULT_TENANT = 'NONE'
    public static final String DEFAULT_APP = 'DEFAULT_APP'
    public static final String DEFAULT_VERSION = '999.99.9'
    public static final String DEFAULT_STATUS = ReleaseStatus.SNAPSHOT.name()
    public static final String HEAD = 'HEAD'
    public static final String TEST_BRANCH = 'TEST'

    public static final transient ApplicationID testAppId = new ApplicationID(DEFAULT_TENANT, DEFAULT_APP, DEFAULT_VERSION, DEFAULT_STATUS, TEST_BRANCH)
    public static final Pattern VERSION_REGEX = ~/[.]/

    private final String tenant
    private final String app
    private final String version
    private final String status
    private final String branch

    // For serialization support only
    private ApplicationID()
    {
        tenant = DEFAULT_TENANT
        app = DEFAULT_APP
        version = DEFAULT_VERSION
        status = ReleaseStatus.SNAPSHOT.name()
        branch = HEAD
    }

    ApplicationID(String tenant, String app, String version, String status, String branch)
    {
        this.tenant = tenant ? tenant.trim() : null
        this.app = app
        this.version = version
        this.status = status
        this.branch = branch
        validate()
    }

    String getTenant()
    {
        return tenant
    }

    String getApp()
    {
        return app
    }

    String getVersion()
    {
        return version
    }

    String getStatus()
    {
        return status
    }

    String getBranch()
    {
        return branch
    }

    String cacheKey(String name = '')
    {
        name = name ?: ''
        return toString().concat(name).toLowerCase()
    }

    /**
     * Convert "tenant/app/version/status/branch/" to ApplicationID
     */
    static ApplicationID convert(String appIdString)
    {
        if (isEmpty(appIdString))
        {
            throw new IllegalArgumentException('Empty or null string cannot be converted to an ApplicationID.')
        }
        try
        {
            List<String> pieces = appIdString.tokenize('/')
            if (pieces.size() != 5)
            {
                throw new IllegalArgumentException('Must have 5 components to the appId string.')
            }
            return new ApplicationID(pieces[0].trim(), pieces[1].trim(), pieces[2].trim(), pieces[3].trim().toUpperCase(), pieces[4].trim())
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Unable to convert: ${appIdString} to an ApplicationID. ApplicationID string must look like 'tenant/app/version/status/branch'. ${e.message}")
        }
    }

    boolean equals(Object o)
    {
        if (this.is(o))
        {
            return true
        }

        if (!(o instanceof ApplicationID))
        {
            return false
        }

        ApplicationID that = (ApplicationID) o
        return equalsNotIncludingBranch(that) && equalsIgnoreCase(branch, that.branch)
    }

    int hashCode()
    {
        int result = tenant.toLowerCase().hashCode()
        result = 31 * result + app.toLowerCase().hashCode()
        result = 31 * result + version.hashCode()
        result = 31 * result + status.toUpperCase().hashCode()
        result = 31 * result + branch.toLowerCase().hashCode()
        return result
    }

    String toString()
    {
        StringBuilder builder = new StringBuilder(tenant)
        builder.append('/')
        builder.append(app)
        builder.append('/')
        builder.append(version)
        builder.append('/')
        builder.append(status)
        builder.append('/')
        builder.append(branch)
        builder.append('/')
        return builder.toString()
    }

    boolean isSnapshot()
    {
        return ReleaseStatus.SNAPSHOT.name() == status
    }

    boolean isRelease()
    {
        return ReleaseStatus.RELEASE.name() == status
    }

    /**
     * Creates a new SNAPSHOT version of this application id.
     * @param ver new version.
     * @return a new ApplicationId that is a snapshot of the new version passed in.
     */
    ApplicationID createNewSnapshotId(String ver)
    {
        //  In the Change Version the status was always SNAPSHOT when creating a new version.
        //  That is why we hardcode this to snapshot here.
        return new ApplicationID(tenant, app, ver, ReleaseStatus.SNAPSHOT.name(), branch)
    }

    /**
     * Allow ApplicationID to be converted to Map as in <code>appId as Map</code>
     */
    Object asType(Class clazz)
    {

        if (Map.isAssignableFrom(clazz))
        {
            return [tenant: tenant, app: app, version: version, status: status, branch: branch]
        }
        else if (ApplicationID.isAssignableFrom(clazz))
        {
            return this
        }
        else
        {
            throw new IllegalArgumentException('Cannot convert ApplicationID to ' + clazz.name)
        }
    }

    /**
     * Creates a new ApplicationID with a status of release using all the same parameters of
     * this ApplicationID, but forcing the branch to HEAD (release versions can only exit
     * on HEAD).  If the current appliationID is already RELEASE just return this.
     * @return a new ApplicationId that is in RELEASE mode.
     */
    ApplicationID asRelease()
    {
        if (ReleaseStatus.RELEASE.name() == status && HEAD == branch)
        {
            return this
        }

        return new ApplicationID(tenant, app, version, ReleaseStatus.RELEASE.name(), HEAD)
    }

    /**
     * Creates a new ApplicationID with a status of SNAPSHOT using all the same parameters of
     * this ApplicationID.  If the current ApplicationID is already SNAPSHOT just return this.
     * @return a new ApplicationId that is in SNAPSHOT mode.
     */
    ApplicationID asSnapshot()
    {
        if (ReleaseStatus.SNAPSHOT.name() == status)
        {
            return this
        }

        return new ApplicationID(tenant, app, version, ReleaseStatus.SNAPSHOT.name(), branch)
    }

    /**
     * Creates a new ApplicationID with HEAD as the branch using all the same parameters of
     * this ApplicationID.
     * @return a new ApplicationId that is on the HEAD branch.
     */
    ApplicationID asHead()
    {
        if (HEAD == branch)
        {
            return this
        }
        return new ApplicationID(tenant, app, version, status, HEAD)
    }

    /**
     * Convert an ApplicationID from one branch to another
     * @param bran String destination branch name
     * @return new ApplicationID in terms of the passed in branch (bran)
     */
    ApplicationID asBranch(String bran)
    {
        return new ApplicationID(tenant, app, version, status, bran)
    }

    /**
     * Convert an Application from one version to another
     * @param ver String destination version
     * @return new ApplicationID in terms of the passed in version (ver)
     */
    ApplicationID asVersion(String ver)
    {
        if (version == ver)
        {
            return this
        }
        return new ApplicationID(tenant, app, ver, status, branch)
    }

    ApplicationID asBootVersion()
    {
        return new ApplicationID(tenant, app, SYS_BOOT_VERSION, DEFAULT_STATUS, branch)
    }

    /**
     * Ensure that all components of the ApplicationID are valid (with their specs in terms of allowable characters.)
     */
    void validate()
    {
        validateTenant(tenant)
        validateApp(app)
        validateVersion(version)
        validateStatus(status)
        validateBranch(branch)
    }

    static void validateTenant(String tenant)
    {
        if (hasContent(tenant))
        {
            Matcher m = Regexes.validTenantName.matcher(tenant)
            if (m.find() && tenant.length() <= 10)
            {
                return
            }
        }
        throw new IllegalArgumentException("Invalid tenant string: '" + tenant + "'. Tenant must contain only A-Z, a-z, or 0-9 and dash (-). From 1 to 10 characters max.")
    }

    static void validateApp(String appName)
    {
        if (isEmpty(appName))
        {
            throw new IllegalArgumentException("App cannot be null or empty")
        }
    }

    static void validateVersion(String version)
    {
        if (isEmpty(version))
        {
            throw new IllegalArgumentException("n-cube version cannot be null or empty")
        }

        Matcher m = Regexes.validVersion.matcher(version)
        if (m.matches())
        {
            return
        }
        throw new IllegalArgumentException("Invalid version: '" + version + "'. n-cube version must follow the form n.n.n where n is a number 0 or greater. The numbers stand for major.minor.revision")
    }

    static void validateStatus(String status)
    {
        if (status == null)
        {
            throw new IllegalArgumentException("status name cannot be null")
        }
        try
        {
            ReleaseStatus.valueOf(status)
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException('status must be RELEASE or SNAPSHOT')
        }
    }

    static void validateBranch(String branch)
    {
        if (isEmpty(branch))
        {
            throw new IllegalArgumentException("n-cube branch cannot be null or empty")
        }
        Matcher m = Regexes.validBranch.matcher(branch)
        if (m.matches() && branch.length() <= 80)
        {
            return
        }
        throw new IllegalArgumentException("Invalid branch: '" + branch + "'. n-cube branch must contain only A-Z, a-z, or 0-9 dash(-), underscore (_), and dot (.) From 1 to 80 characters.")
    }

    boolean isHead()
    {
        return HEAD.equalsIgnoreCase(branch)
    }

    void validateBranchIsNotHead()
    {
        if (head)
        {
            throw new IllegalArgumentException("Branch cannot be 'HEAD'")
        }
    }

    void validateStatusIsNotRelease()
    {
        if (release)
        {
            throw new IllegalArgumentException("Status cannot be 'RELEASE'")
        }
    }

    boolean equalsNotIncludingBranch(ApplicationID that)
    {
        return equalsIgnoreCase(tenant, that.tenant) &&
                equalsIgnoreCase(app, that.app) &&
                equalsIgnoreCase(status, that.status) &&
                equals(version, that.version)

    }

    /**
     * @return long version as a number that can be compared.  It is one million times major, plus one
     * thousand times minor, plus patch.
     */
    long getVersionValue()
    {
        return getVersionValue(version)
    }

    /**
     * @param String version in "major.minor.patch" format where all three components are numeric and each one
     * is less than 1,000.
     * @return long version as a number that can be compared.  It is one million times major, plus one
     * thousand times minor, plus patch.
     */
    static long getVersionValue(String version)
    {
        String[] pieces = VERSION_REGEX.split(version)
        if (pieces.length != 3)
        {
            return 0
        }
        int major = Integer.valueOf(pieces[0]) * 1000 * 1000
        int minor = Integer.valueOf(pieces[1]) * 1000
        int patch = Integer.valueOf(pieces[2].split('-')[0])
        return major + minor + patch
    }

    /**
     * Validate ApplicationID is not null and valid
     * @param appId ApplicationID to test
     */
    static void validateAppId(ApplicationID appId)
    {
        if (appId == null)
        {
            throw new IllegalArgumentException('ApplicationID cannot be null')
        }
        appId.validate()
    }
}
