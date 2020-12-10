package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import static com.cedarsoftware.util.ExceptionUtilities.getDeepestException

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License');
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
class TestNCubeConcurrency extends NCubeBaseTest
{
    @Test
    void testConcurrencyWithDifferentFiles()
    {
        final CountDownLatch startLatch = new CountDownLatch(1)
        int numTests = 4
        final CountDownLatch finishedLatch = new CountDownLatch(numTests)

        Runnable test1 = {
            startLatch.await()
            concurrencyTest('StringFromRemoteUrlBig')
            finishedLatch.countDown()
        } as Runnable

        Runnable test2 = {
            startLatch.await()
            concurrencyTest('StringFromLocalUrl')
            finishedLatch.countDown()
        } as Runnable

        Runnable test3 = {
            startLatch.await()
            concurrencyTest('BinaryFromRemoteUrl')
            finishedLatch.countDown()
        } as Runnable

        Runnable test4 = {
            startLatch.await()
            concurrencyTest('BinaryFromLocalUrl')
            finishedLatch.countDown()
        } as Runnable

        ExecutorService executor = Executors.newFixedThreadPool(numTests)
        executor.execute(test1)
        executor.execute(test2)
        executor.execute(test3)
        executor.execute(test4)

        startLatch.countDown()  // trigger all threads to begin
        finishedLatch.await()   // wait for all threads to finish
        executor.shutdown()
    }

    private static void concurrencyTest(final String site)
    {
        int numThreads = 8
        long timeToRun = 3000L
        final AtomicBoolean failed = new AtomicBoolean(false)
        NCube n1 = createRuntimeCubeFromResource(ApplicationID.testAppId, 'urlContent.json')

        final CountDownLatch startLatch = new CountDownLatch(1)
        final CountDownLatch finishedLatch = new CountDownLatch(numThreads)
        ExecutorService executor = Executors.newFixedThreadPool(numThreads)

        // Ensure that the URL fetching does not have issues with high contention
        Runnable runnable = {
            try
            {
                startLatch.await()
                long start = System.currentTimeMillis()
                while (System.currentTimeMillis() - start < timeToRun)
                {
                    for (int j = 0; j < 100; j++)
                    {
                        n1.getCell([sites:site] as Map)
                    }
                }
            }
            catch (Exception e)
            {
                Throwable t = getDeepestException(e)
                if (!(t.message?.contains('cleared while cell was executing')))
                {
                    failed.set(true)
                    throw e
                }
                else
                {
                    println 'benign - code cleared while cell was executing'
                }
            }
            finally
            {
                finishedLatch.countDown()
            }
        } as Runnable

        for (int i = 0; i < numThreads; i++)
        {
            executor.execute(runnable)
        }
        startLatch.countDown()
        finishedLatch.await()
        assert !failed.get()
    }

    @Test
    void testCacheFlag() throws IOException
    {
        NCube<Object> n1 = createRuntimeCubeFromResource(ApplicationID.testAppId, 'urlContent.json')
        def items = new IdentityHashMap()
        def set = new LinkedHashSet()

        def cell = n1.getCell([sites:'StringFromRemoteUrlBig'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'StringFromRemoteUrlBig'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 1
        assert set.size() == 1

        items.clear()
        set.clear()
        cell = n1.getCell([sites:'StringFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'StringFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 2        // Different at the Identity level, therefore IdentityHashSet creates another entry
        assert set.size() == 1          // Matches as .equals() therefore LinkedHashSet does not create another entry

        items.clear()
        set.clear()
        cell = n1.getCell([sites:'BinaryFromRemoteUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'BinaryFromRemoteUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 1
        assert set.size() == 1

        items.clear()
        set.clear()
        cell = n1.getCell([sites:'BinaryFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'BinaryFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 2
        assert set.size() == 2
    }
}
