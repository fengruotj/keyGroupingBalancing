package com.basic.storm.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/10.
 */
public class ListTest {

    @Test
    public void testList(){
        List<Long> mlist=new ArrayList();
        mlist.add(56L);
        Long aLong = mlist.get(0);
        aLong=28L;
        System.out.println(mlist);
    }

    @Test
    public void testIterator(){
        List<Long> mlist=new ArrayList();
        mlist.add(56L);
        Iterator iterator=mlist.iterator();
        while (iterator.hasNext()){
            Long next = (Long) iterator.next();
            next=28L;
            iterator.remove();
        }
        System.out.println(mlist);
    }
}
