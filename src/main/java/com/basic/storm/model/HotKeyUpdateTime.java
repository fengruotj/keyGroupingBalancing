package com.basic.storm.model;

import java.sql.Timestamp;

/**
 * locate com.basic.storm.model
 * Created by 79875 on 2017/5/13.
 */
public class HotKeyUpdateTime {
    private String hotkey;
    private long updateTime;
    private Timestamp timestamp;

    public HotKeyUpdateTime() {
    }

    public HotKeyUpdateTime(String hotkey, long updateTime, Timestamp timestamp) {
        this.hotkey = hotkey;
        this.updateTime = updateTime;
        this.timestamp = timestamp;
    }

    public String getHotkey() {
        return hotkey;
    }

    public void setHotkey(String hotkey) {
        this.hotkey = hotkey;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }
}
