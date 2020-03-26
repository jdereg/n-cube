package com.cedarsoftware.util

import com.cedarsoftware.servlet.JsonCommandServlet
import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.AuthCache
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.routing.HttpRoute
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.BasicAuthCache
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.StandardHttpRequestRetryHandler
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.springframework.beans.factory.annotation.Value
import org.springframework.util.FastByteArrayOutputStream

import javax.servlet.http.Cookie
import javax.servlet.http.HttpServletRequest
import java.util.zip.Deflater

import static com.cedarsoftware.ncube.NCubeConstants.LOG_ARG_LENGTH
import static com.cedarsoftware.util.io.MetaUtils.getLogMessage
import static org.apache.http.HttpHeaders.*
import static org.apache.http.entity.ContentType.APPLICATION_JSON

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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
class JsonHttpProxy implements CallableBean
{
    @Value("#{\${ncube.proxy.cookiesToInclude:'JSESSIONID'}.split(',')}")
    private List<String> cookiesToInclude

    private final CloseableHttpClient httpClient
    private CredentialsProvider credsProvider
    private AuthCache authCache
    private final HttpHost httpHost
    private final HttpHost proxyHost
    private final String context
    private final String username
    private final String password
    private final int numConnections
    private String accessTokenUri
    private String token
    private long expireTime = System.currentTimeMillis()

    JsonHttpProxy(HttpHost httpHost, String context, String username = null, String password = null, int numConnections = 100)
    {
        this.httpHost = httpHost
        proxyHost = null
        this.context = context
        this.username = username
        this.password = password
        this.numConnections = numConnections
        httpClient = createClient()
        createAuthCache()
        log.info('Started with basic authentication')
    }

    JsonHttpProxy(HttpHost httpHost, HttpHost proxyHost, String context, String username = null, String password = null, int numConnections = 100)
    {
        this.httpHost = httpHost
        this.proxyHost = proxyHost
        this.context = context
        this.username = username
        this.password = password
        this.numConnections = numConnections
        httpClient = createClient()
        createAuthCache()
        log.info('Started with basic authentication with proxy host')
    }

    JsonHttpProxy(HttpHost httpHost, String context, String accessTokenUri, String clientId, String clientSecret, int numConnections = 100)
    {
        this.httpHost = httpHost
        this.proxyHost = null
        this.context = context
        this.username = clientId
        this.password = clientSecret
        this.numConnections = numConnections
        this.accessTokenUri = accessTokenUri
        httpClient = createClient()
        log.info('Started with oauth2 authentication')
    }

    /**
     * Creates the client object with the proxy and cookie store for later use.
     *
     * @return A {@link CloseableHttpClient} 
     */
    protected CloseableHttpClient createClient()
    {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
        cm.maxTotal = numConnections // Max total connection
        cm.defaultMaxPerRoute = numConnections // Default max connection per route
        cm.setMaxPerRoute(new HttpRoute(httpHost), numConnections) // Max connections per route

        RequestConfig.Builder configBuilder = RequestConfig.custom()
        configBuilder.connectTimeout = 10 * 1000
        configBuilder.connectionRequestTimeout = 10 * 1000
        configBuilder.socketTimeout = 420 * 1000
        RequestConfig config = configBuilder.build()

        HttpClientBuilder builder = HttpClientBuilder.create()
        builder.defaultRequestConfig = config
        builder.connectionManager = cm
        builder.disableCookieManagement()
        builder.retryHandler = new StandardHttpRequestRetryHandler(5, true)

        if (proxyHost)
        {
            builder.proxy = proxyHost
        }

        CloseableHttpClient httpClient = builder.build()
        return httpClient
    }

    Object call(String bean, String methodName, List args)
    {
        Object[] params = args.toArray()
        FastByteArrayOutputStream stream = new FastByteArrayOutputStream(1024)
        JsonWriter writer = new JsonWriter(new AdjustableGZIPOutputStream(stream, Deflater.BEST_SPEED))
        writer.write(params)
        writer.flush()
        writer.close()

        if (log.debugEnabled)
        {
            log.debug("${bean}.${getLogMessage(methodName, params, LOG_ARG_LENGTH)}")
        }

        HttpClientContext clientContext = HttpClientContext.create()
        HttpPost request = new HttpPost("${httpHost.toURI()}/${context}/cmd/${bean}/${methodName}")
        if (accessTokenUri)
        {
            String token = requestToken()
            request.setHeader(AUTHORIZATION, "Bearer ${token}")
        }
        else if (username && password)
        {
            clientContext.credentialsProvider = credsProvider
            clientContext.authCache = authCache
        }
        else
        {
            assignCookieHeader(request)
        }
        request.setHeader(USER_AGENT, 'ncube')
        request.setHeader(ACCEPT, APPLICATION_JSON.mimeType)
        request.setHeader(ACCEPT_ENCODING, 'gzip, deflate')
        request.setHeader(CONTENT_TYPE, "application/json; charset=UTF-8")
        request.setHeader(CONTENT_ENCODING, 'gzip')
        request.entity = new ByteArrayEntity(stream.toByteArrayUnsafe(), 0, stream.size())

        HttpResponse response = httpClient.execute(request, clientContext)
        request.entity = null
        boolean parsedJsonOk = false
        JsonReader reader = null
        try
        {
            reader = new JsonReader(new BufferedInputStream(response.entity.content))
            Map envelope = reader.readObject() as Map
            parsedJsonOk = true
            
            if (envelope.exception != null)
            {
                throw (Exception)envelope.exception
            }
            if (!envelope.status)
            {
                String msg
                if (envelope.data instanceof String)
                {
                    msg = envelope.data
                }
                else if (envelope.data != null)
                {
                    msg = envelope.data.toString()
                }
                else
                {
                    msg = 'no extra info provided.'
                }
                throw new RuntimeException("REST call [${bean}.${methodName}] indicated failure on server: ${msg}")
            }
            return envelope.data
        }
        catch (ThreadDeath t)
        {
            throw t
        }
        catch (Throwable e)
        {
            if (!parsedJsonOk)
            {
                log.warn("Failed to process response (code: ${response.statusLine.statusCode}) from server with call: ${bean}.${getLogMessage(methodName, args.toArray(), LOG_ARG_LENGTH)}, headers: ${request.allHeaders}")
            }
            throw e
        }
        finally
        {
            if (reader != null)
            {
                IOUtilities.close(reader)
            }
        }
    }

    private void assignCookieHeader(HttpPost proxyRequest)
    {
        HttpServletRequest servletRequest = JsonCommandServlet.servletRequest.get()
        if (servletRequest instanceof HttpServletRequest)
        {
            Cookie[] cookies = servletRequest.cookies
            if (cookies == null)
            {
                return
            }
            StringJoiner joiner = new StringJoiner("; ")
            for (Cookie cookie: cookies)
            {
                if (cookiesToInclude.contains(cookie.name))
                {
                    joiner.add("${cookie.name}=${cookie.value}")
                }
            }
            if (joiner.length())
            {
                proxyRequest.setHeader('Cookie', joiner.toString())
            }
        }
    }

    private void createAuthCache()
    {
        byte[] rocketBytes = [0xf0, 0x9f, 0x9a, 0x80]
        String rocket = StringUtilities.createUTF8String(rocketBytes)
        log.info("NCUBE storage-server: ${rocket} ${httpHost.schemeName}://${httpHost.hostName}:${httpHost.port}/${context}")
        if (username && password)
        {
            credsProvider = new BasicCredentialsProvider()
            AuthScope authScope = new AuthScope(httpHost.hostName, httpHost.port)
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password)
            credsProvider.setCredentials(authScope, credentials)
            authCache = new BasicAuthCache()
            authCache.put(httpHost, new BasicScheme())
        }
    }

    private String requestToken()
    {
        long currentTime = System.currentTimeMillis()
        if (currentTime >= expireTime)
        {
            JsonReader reader = null
            try
            {
                HttpPost request = new HttpPost("${accessTokenUri}?grant_type=client_credentials")
                String authString = "${username}:${password}".bytes.encodeBase64().toString()
                request.setHeader(AUTHORIZATION, "Basic ${authString}")

                HttpResponse response = httpClient.execute(request)
                reader = new JsonReader(new BufferedInputStream(response.entity.content))
                Map envelope = reader.readObject() as Map

                token = envelope.access_token
                long expiresInSeconds = (long) envelope.expires_in
                long expiresInMillis = (expiresInSeconds - 10) * 1000
                expireTime = currentTime + expiresInMillis
                log.info('Updated the oauth2 token')
            }
            catch (ThreadDeath t)
            {
                throw t
            }
            catch (Throwable e)
            {
                log.error("Failed acquire access token from ${accessTokenUri}.")
                throw e
            }
            finally
            {
                if (reader != null)
                {
                    IOUtilities.close(reader)
                }
            }
        }
        return token
    }
}
