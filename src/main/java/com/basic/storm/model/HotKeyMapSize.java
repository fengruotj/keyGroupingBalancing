package com.basic.storm.model;

/**
 * locate com.basic.storm.model
 * Created by 79875 on 2017/5/13.
 */
public class HotKeyMapSize {
    private int keysize;
    private int tablelength;

    public HotKeyMapSize(int keysize, int tablelength) {
        this.keysize = keysize;
        this.tablelength = tablelength;
    }

    public HotKeyMapSize() {
    }

    public int getKeysize() {
        return keysize;
    }

    public void setKeysize(int keysize) {
        this.keysize = keysize;
    }

    public int getTablelength() {
        return tablelength;
    }

    public void setTablelength(int tablelength) {
        this.tablelength = tablelength;
    }
}
