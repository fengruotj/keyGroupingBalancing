package com.basic.storm.model;

import java.io.Serializable;

/**
 * locate com.basic.storm.main
 * Created by 79875 on 2017/5/9.
 */
public class KeyTimeModel implements Serializable{
    private long startTimeSystemTime;
    private long endTimeSystemTime;

    public KeyTimeModel(long startTimeSystemTime, long endTimeSystemTime) {
        this.startTimeSystemTime = startTimeSystemTime;
        this.endTimeSystemTime = endTimeSystemTime;
    }

    public KeyTimeModel() {
    }

    public long getStartTimeSystemTime() {
        return startTimeSystemTime;
    }

    public void setStartTimeSystemTime(long startTimeSystemTime) {
        this.startTimeSystemTime = startTimeSystemTime;
    }

    public long getEndTimeSystemTime() {
        return endTimeSystemTime;
    }

    public void setEndTimeSystemTime(long endTimeSystemTime) {
        this.endTimeSystemTime = endTimeSystemTime;
    }

    public long getdelayTime(){
        return endTimeSystemTime-startTimeSystemTime;
    }

    @Override
    public String toString() {
        return "KeyTimeModel{" +
                "startTimeSystemTime=" + startTimeSystemTime +
                ", endTimeSystemTime=" + endTimeSystemTime +
                '}';
    }
}
