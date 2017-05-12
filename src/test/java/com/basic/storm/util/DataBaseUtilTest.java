package com.basic.storm.util;

import org.junit.Test;

/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/9.
 */
public class DataBaseUtilTest {
    private DataBaseUtil dataBaseUtil=new DataBaseUtil();

    @Test
    public void getCount() throws Exception {
        int count = dataBaseUtil.getCount("select * from administrator");
        System.out.println(count);
    }

}
