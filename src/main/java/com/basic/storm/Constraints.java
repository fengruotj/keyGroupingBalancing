package com.basic.storm;

import com.basic.storm.util.PropertiesUtil;

/**
 * locate com.basic.storm
 * Created by 79875 on 2017/5/12.
 */
public class Constraints {
    public static int Threshold_r = Integer.valueOf(PropertiesUtil.getProperties("Threshold_r"));//
    public static int Threshold_l = Integer.valueOf(PropertiesUtil.getProperties("Threshold_l"));//
    public static double Threshold_p = Double.valueOf(PropertiesUtil.getProperties("Threshold_p"));//衰减概率
    //public static double Threshold_p = 0.01;//衰减概率
}
