package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.decision.DecisionTable
import groovy.transform.CompileStatic
import org.junit.Test

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
class TestDecisionTable extends NCubeBaseTest
{
    @Test
    void testGetDecision()
    {
        Map input = [profitCenter: '2967',
                     producerCode: '50',
                     date: new Date(),
                     symbol: 'CAP']
        NCube ncube = createRuntimeCubeFromResource(ApplicationID.testAppId, 'commission.json')
        DecisionTable decisionTable = new DecisionTable(ncube)

        for (int i=0; i < 1000; i++)
        {
            long start = System.nanoTime()
            decisionTable.getDecision(input)
            long end = System.nanoTime()
            println "took ${(end - start) / 1000000} ms"
        }
    }
}
