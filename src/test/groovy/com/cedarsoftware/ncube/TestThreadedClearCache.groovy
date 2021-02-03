package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Test

import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static com.cedarsoftware.util.ExceptionUtilities.getDeepestException
import static org.junit.Assert.assertEquals

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestThreadedClearCache extends NCubeCleanupBaseTest
{
    public static ApplicationID usedId = new ApplicationID(ApplicationID.DEFAULT_TENANT, "usedInvalidId", ApplicationID.DEFAULT_VERSION, ApplicationID.DEFAULT_STATUS, ApplicationID.TEST_BRANCH)

    @Test
    void testQuiet()
    {
    }
    
    @Test
    void testCubesWithThreadedClearCacheWithAppId()
    {
        createCubeFromResource(usedId, 'sys.classpath.2per.app.json')
        createCubeFromResource(usedId, 'math.controller.json')
        concurrencyTest()
    }

    private void concurrencyTest()
    {
        int numThreads = 12
        final CountDownLatch startLatch = new CountDownLatch(1)
        final CountDownLatch finishedLatch = new CountDownLatch(numThreads)
        final AtomicBoolean failed = new AtomicBoolean(false)
        ExecutorService executor = Executors.newFixedThreadPool(numThreads)
        Random random = new SecureRandom()
        int count = 0

        Thread t = new Thread(new Runnable() {
            void run()
            {
                while (true)
                {
                    Thread.sleep(random.nextInt(4))
                    ncubeRuntime.clearCache(usedId, ["MathController"])
                }
            }
        })
        t.setName("PullTheRugOut")
        t.setDaemon(true)
        t.start()

        for (int taskCount = 0; taskCount < numThreads; taskCount++)
        {
            executor.execute(new Runnable() {
                void run() {
                    startLatch.await()
                    try
                    {
                        // NOTE: Change the wave count to 100 to ensure that now "cell cleared while code executing" errors occur.
                        for (int wave = 0; wave < 1; wave++)
                        {
                            println "wave ${wave} ${Thread.currentThread().name}"

                            try
                            {
                                NCube cube = mutableClient.getCube(usedId, "MathController")

                                for (int i = 0; i < 100; i++)
                                {
                                    Thread.sleep(random.nextInt(3))
                                    for (int x=0; x < 20; x++)
                                    {
                                        def input = [:]
                                        input.env = "a"
                                        input.x = 5
                                        input.method = 'square'

                                        assertEquals(25, cube.getCell(input))

                                        input.method = 'factorial'
                                        assertEquals(120, cube.getCell(input))

                                        input.env = "b"
                                        input.x = 6
                                        input.method = 'square'
                                        assertEquals(6, cube.getCell(input))

                                        input.method = 'factorial'
                                        assertEquals(6, cube.getCell(input))
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace()
                                failed.set(true)
                                throw e
                            }
                        }
                    }
                    finally
                    {
                        finishedLatch.countDown()
                    }
                }
            })
        }
        startLatch.countDown()  // trigger all Runnables to start
        finishedLatch.await()   // wait for all Runnables to finish
        executor.shutdown()
        assert !failed.get()
    }
}
