package com.basic.storm.util;

import org.junit.Test;

/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/8.
 */
public class TimeTest {

    public void tryWait(long INTERVAL){
        long start = System.nanoTime();
        System.out.println(start);
        long end=0;
        do{
            end = System.nanoTime();
        }while(start + INTERVAL >= end);
    }

    @Test
    public void Test(){
        tryWait(5000000000L);
    }
}
