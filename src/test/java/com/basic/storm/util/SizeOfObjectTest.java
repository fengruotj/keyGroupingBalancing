package com.basic.storm.util;

import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;

/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/9.
 */
public class SizeOfObjectTest {

    private SynopsisHashMap <String,Integer> synopsisHashMap=new SynopsisHashMap<>();

    @Test
    public void sizeOfsynopsisHashMap() throws Exception {
        synopsisHashMap.put("key",1);
        synopsisHashMap.put("hha",2);
        System.out.println(ClassLayout.parseClass(synopsisHashMap.getClass()).toPrintable(synopsisHashMap));
    }

    @Test
    public void sizeOfBitSet() throws Exception {
        BitSet bitSet=new BitSet(16);
        System.out.println(ClassLayout.parseClass(BitSet.class).toPrintable(bitSet));
    }
}
