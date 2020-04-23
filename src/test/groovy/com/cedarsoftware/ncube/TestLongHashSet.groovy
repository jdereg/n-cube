package com.cedarsoftware.ncube

import com.cedarsoftware.util.LongHashSet
import groovy.transform.CompileStatic
import org.junit.Test

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestLongHashSet
{
    @Test
    void testLongHashset()
    {
        LongHashSet set1 = new LongHashSet()
        LongHashSet set2 = new LongHashSet()
        set1.add(1L)
        set1.add(20L)
        set1.add(300L)
        set1.add(4000L)
        set1.add(50000L)
        set1.add(600000L)
        set1.add(7000000L)
        set1.add(80000000L)
        set1.add(900000000L)

        set2.add(900000000L)
        set2.add(80000000L)
        set2.add(4000L)
        set2.add(7000000L)
        set2.add(600000L)
        set2.add(1L)
        set2.add(20L)
        set2.add(300L)
        set2.add(50000L)

        assert set1.hashCode() == set2.hashCode()
        assert set1 == set2
    }

    @Test
    void testRemoveFirst()
    {
        LongHashSet set = new LongHashSet()
        set.add(7L)
        set.add(8L)
        set.add(9L)
        assert !set.remove(10)
        set.remove(7)

        assert set.size() == 2
        assert set.contains(8)
        assert set.contains(9)

        set.remove(8)
        assert set.size() == 1
        assert set.contains(9)

        set.remove(9)
        assert set.size() == 0
        assert set.hashCode() == 0
    }

    @Test
    void testRemoveMiddle()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        set.remove(8)
        assert set.size() == 2
        assert set.contains(7)
        assert set.contains(9)
    }

    @Test
    void testRemoveLast()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        set.remove(9)
        assert set.size() == 2
        assert set.contains(7)
        assert set.contains(8)

        set.remove(8)
        assert set.size() == 1
        assert set.contains(7)

        set.remove(7)
        assert set.size() == 0
    }

    @Test
    void testToArray()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        Object[] nums = set.toArray()
        assert nums[0] == 7
        assert nums[1] == 8
        assert nums[2] == 9

        int[] array = set.toArray([] as int[]) as int[]
        assert array.length == 3
        assert array[0] == 7
        assert array[1] == 8
        assert array[2] == 9

        assert array instanceof int[]
    }

    @Test
    void testEqualsGarbage()
    {
        LongHashSet set = new LongHashSet()
        set.add(1)
        set.add(2)
        assert set != 'foo'
    }

    @Test
    void testEqualsNotSameSize()
    {
        LongHashSet set1 = new LongHashSet()
        set1.add(1)

        LongHashSet set2 = new LongHashSet()
        set2.add(1)
        set2.add(2)
        assert !set1.equals(set2)
        assert !set2.equals(set1)
    }

    @Test
    void testNotEqualsElements()
    {
        LongHashSet set1 = new LongHashSet()
        set1.add(7L)
        set1.add(8L)
        set1.add(9L)

        LongHashSet set2 = new LongHashSet()
        set2.add(7L)
        set2.add(8L)
        set2.add(10L)

        assert set1 != set2
    }

    @Test
    void testEqualsEmpty()
    {
        LongHashSet set1 = new LongHashSet()
        LongHashSet set2 = new LongHashSet()
        assert set1.equals(set2)
    }

    @Test
    void testClear()
    {
        LongHashSet set1 = new LongHashSet()
        set1.add(9L)
        set1.add(8L)
        set1.add(7L)
        assert set1.size() == 3
        assert set1.first() == 7L
        set1.clear()
        assert set1.empty
    }

    @Test
    void testEmptyStuff()
    {
        LongHashSet set1 = new LongHashSet()
        LongHashSet set2 = new LongHashSet()
        assert set1 == set2
        assert !set1.contains(9)
        Object[] array = set1.toArray()
        assert array.length == 0
        assert !set1.remove(5)
    }

    @Test
    void testAddRemoveAll()
    {
        LongHashSet set = new LongHashSet()
        set.addAll([7, 8, 9] as Set<Long>)
        assert set.size() == 3
        assert set.contains(7)
        assert set.contains(8)
        assert set.contains(9)

        set.removeAll([8, 9])
        assert set.contains(7)
        set.removeAll([7])
        assert set.empty
    }

    @Test
    void testRetainsContainsAll()
    {
        LongHashSet set1 = new LongHashSet([7, 8, 9] as Set)
        LongHashSet set2 = new LongHashSet([5, 6, 7] as Set)
        assert set1.retainAll(set2)
        assert set1.size() == 1
        assert set1.contains(7)
        assert set2.containsAll(set1)
        assert !set1.containsAll(set2)
    }

    @Test
    void testActsAsSetNotCollection()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(7)
        assert set.size() == 1
        assert set.contains(7)

        set = new LongHashSet([1, 2, 3, 3, 4, 5] as Set<Long>)
        assert set.size() == 5
        set.addAll([0, 7, 0, 7] as Set<Long>)
        assert set.size() == 7

        set = new LongHashSet()
        set.addAll([1, 2, 3, 3, 4, 5] as List<Long>)
        assert set.size() == 5
        set.addAll([0, 7, 0, 7] as Set<Long>)
        assert set.size() == 7
    }

    @Test
    void testOrderDoesNotMatter()
    {
        Set set1 = new LongHashSet()
        set1.add(1)
        set1.add(2)
        set1.add(3)
        Set set2 = new LongHashSet()
        set2.add(3)
        set2.add(2)
        set2.add(1)

        assert set1.hashCode() == set2.hashCode()
        assert set1 == set2
    }

    @Test
    void testFastConstructors()
    {
        Set set1 = new LongHashSet(16L)
        assert set1.size() == 1
        assert set1.contains(16)

        Set set2 = new LongHashSet(16L, 7L)
        assert set2.size() == 2
        assert set2.contains(16)
        assert set2.contains(7L)
        assert set2.first() == 7L
        assert set2.last() == 16L

        Set set3 = new LongHashSet(16L, 7L, -1L)
        assert set3.size() == 3
        assert set3.contains(16)
        assert set3.contains(7)
        assert set3.contains(-1)
        assert set3.first() == -1L
        assert set3.last() == 16L
    }
}
