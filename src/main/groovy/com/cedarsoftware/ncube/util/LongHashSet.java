package com.cedarsoftware.ncube.util;

import java.util.*;

/**
 * Special Set instance that hashes the Set&lt;Long&gt; column IDs with excellent dispersion,
 * while at the same time, using only a single primitive long (8 bytes) per entry.
 * This set is backed by a long[], so adding and removing items is O(n).
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License");
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
public class LongHashSet implements Set<Long>
{
    private long[] elems = null;

    public LongHashSet()
    { }

    public LongHashSet(Set col)
    {
        long[] items = new long[col.size()];
        int pos = 0;
        for (Object n : col)
        {
            items[pos++] = ((Number)n).longValue();
        }
        Arrays.sort(items);
        elems = items;
    }

    public int size()
    {
        return elems == null ? 0 : elems.length;
    }

    public boolean isEmpty()
    {
        return elems == null || elems.length == 0;
    }

    public boolean contains(Object item)
    {
        if (isEmpty() || item == null)
        {
            return false;
        }

        return Arrays.binarySearch(elems, ((Number)item).longValue()) >= 0;
    }

    public Iterator iterator()
    {
        Iterator it = new Iterator() {
            private int currentIndex = 0;

            public boolean hasNext()
            {
                if (elems == null)
                {
                    return false;
                }
                return currentIndex < elems.length;
            }

            public Long next()
            {
                return elems[currentIndex++];
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
        return it;
    }

    public Object[] toArray()
    {
        if (isEmpty())
        {
            return new Object[]{};
        }

        long[] local = elems;
        int len = local.length;
        Object[] array = new Object[len];

        for (int i=0; i < len; i++)
        {
            array[i] = local[i];
        }
        return array;
    }

    public boolean add(int n)
    {
        return add((long) n);
    }

    public boolean add(Long n)
    {
        if (elems == null)
        {
            elems = new long[1];
            elems[0] = n;
            return true;
        }
        else
        {
            if (contains(n))
            {   // Don't allow duplicates - this is a Set
                return false;
            }
            int origSize = size();
            long[] newElems = new long[origSize + 1];
            System.arraycopy(elems, 0, newElems, 0, origSize);
            newElems[origSize] = n;
            Arrays.sort(newElems);
            elems = newElems;
            return size() != origSize;
        }
    }

    public boolean remove(Object n)
    {
        if (isEmpty() || n == null)
        {
            return false;
        }

        long[] local = elems;
        int len = local.length;

        for (int i=0; i < len; i++)
        {
            if (local[i] == ((Number)n).longValue())
            {
                long[] newElems = new long[len - 1];
                System.arraycopy(local, i + 1, local, i, len - i - 1);
                System.arraycopy(local, 0, newElems, 0, len - 1);
                elems = newElems;
                return true;
            }
        }
        return false;
    }

    public boolean addAll(Collection col)
    {
        int origSize = size();
        Set newSet = new HashSet(col);

        for (Long n : this)
        {
            newSet.add(n);
        }

        long[] items = new long[newSet.size()];
        int j = 0;
        for (Object n : newSet)
        {
            items[j++] = ((Number)n).longValue();
        }
        Arrays.sort(items);
        elems = items;

        return size() != origSize;
    }

    public void clear()
    {
        elems = null;
    }

    public boolean removeAll(Collection col)
    {
        int origSize = size();
        for (Object n : col)
        {
            remove(n);
        }
        return size() != origSize;
    }

    public boolean retainAll(Collection col)
    {
        int origSize = size();
        Set<Long> keep = new LinkedHashSet<>();
        for (Object item : col)
        {
            long n = ((Number)item).longValue();
            if (Arrays.binarySearch(elems, n) >= 0)
            {
                keep.add(n);
            }
        }
        elems = new long[keep.size()];
        long[] local = elems;
        int idx = 0;
        for (Long n : keep)
        {
            local[idx++] = n;
        }
        return size() != origSize;
    }

    public boolean containsAll(Collection col)
    {
        for (Object n : col)
        {
            if (Arrays.binarySearch(elems, ((Number)n).longValue()) < 0)
            {
                return false;
            }
        }
        return true;
    }

    public Object[] toArray(Object[] a)
    {
        return toArray();
    }

    public boolean equals(Object other)
    {
        if (!(other instanceof Set))
        {
            return false;
        }

        if (!(other instanceof LongHashSet))
        {   // Use normal Set equals (let 'them' iterate this set, getting O(1) on each check against 'their' Set)
            return other.equals(this);
        }

        int len = size();
        LongHashSet that = (LongHashSet) other;
        if (that.size() != len)
        {
            return false;
        }

        // Compare all elements in O(n) because we have two LongHashSets, and they order their elements.
        long[] local = elems;
        for (int i=0; i < len; i++)
        {
            if (local[i] != that.elems[i])
            {
                return false;
            }
        }
        return true;
    }

    public int hashCode()
    {
        // This must be an order insensitive hash
        int h = 0;
        long[] local = elems;
        int len = size();
        for (int i=0; i < len; i++)
        {
            long value = local[i];
            h += (int)(value ^ (value >>> 32));
        }
        return h;
    }
}
