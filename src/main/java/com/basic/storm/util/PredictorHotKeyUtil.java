package com.basic.storm.util;

import com.basic.storm.inter.DumpRemoveHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import static com.basic.storm.Constraints.*;


/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/6.
 */
public class PredictorHotKeyUtil implements Serializable{

    private static volatile PredictorHotKeyUtil predictorHotKeyUtil = null;

    private SynopsisHashMap<String, BitSet> predictHotKeyMap = new SynopsisHashMap<String, BitSet>();

    /**
     * 线程池
     * 创建一个可缓存线程池，如果线程池长度超过处理需要，可灵活回收空闲线程，若无可回收，则新建线程。
     */
    //ExecutorService executor = Executors.newCachedThreadPool();
    private long dumpKeyCount=0L;
    private static final Log LOG = LogFactory.getLog(PredictorHotKeyUtil.class);

    private long totalDelayTime=0L;//统计中奖的Key的总实际延迟
    private long totalKeyCount=0L;  //统计中奖的Key的总个数

    public PredictorHotKeyUtil() {
    }

    public static PredictorHotKeyUtil getPredictorHotKeyUtilInstance(){
        if(null == predictorHotKeyUtil)
        {
            synchronized (PredictorHotKeyUtil.class)
            {
                predictorHotKeyUtil=new PredictorHotKeyUtil();
            }
        }
        return predictorHotKeyUtil;
    }

    /**
     *  投掷硬币直到硬币朝上 统计之前投硬币次数
     * @return
     */
    public int countCointUtilUp(){
        int rand = (int)(Math.random()*2);
        int count=0;
        while(rand == 0 && count < Threshold_r+Threshold_l-1)     //Max length set equal to max length+r;
        {
            rand = (int)(Math.random()*2);
            count++;
        }
        return count;
    }

    /**
     * 热词表中热词衰减
     */
    public void SynopsisHashMapDump(DumpRemoveHandler dumpRemoveHandler) {

//        SynopsisHashMapDumpTask synopsisHashMapDumpTask=new SynopsisHashMapDumpTask(predictHotKeyMap);
//        executor.execute(synopsisHashMapDumpTask);

        Iterator<Map.Entry<String, BitSet>> iterator = predictHotKeyMap.newEntryIterator();
        while (iterator.hasNext()){
            Map.Entry<String, BitSet> next = iterator.next();
            BitSet bitm = next.getValue();
            String key = next.getKey();
            if(key!=null){
                long[] lo = bitm.toLongArray();
                if(lo.length > 0){
                    for(int j=0;j<lo.length - 1;j++){
                        lo[j] = lo[j] >>> 1;
                        lo[j] = lo[j] | (lo[j+1] << 63);
                    }
                    lo[lo.length-1] = lo[lo.length-1] >>> 1;
                }
                bitm = BitSet.valueOf(lo);
                if (bitm.isEmpty()) {
                    iterator.remove();
                    dumpRemoveHandler.dumpRemove(key);
                }else
                    next.setValue(bitm);
            }
        }
    }

    /**
     * 预言HotKey
     * @param key
     * @param coninCount
     */
    public void PredictorHotKey(String key,int coninCount){
        int count=coninCount-Threshold_r;
        BitSet bitmap=null;
        if(predictHotKeyMap.get(key)!=null)
            bitmap = (BitSet) predictHotKeyMap.get(key);
        else
            bitmap=new BitSet(Threshold_l);

        bitmap.set(coninCount);
        predictHotKeyMap.put(key,bitmap);

        if(bitmap.cardinality() >= 2)
        {
            //写入数据库？
        }
    }

    /**
     * 简单计算预言热词
     * @param key
     */
    public void simpleComputPredictorHotKey(String key) {
        int count = countCointUtilUp();
        int dumpsize = (int) (1 / Threshold_p);

        if (count >= Threshold_r) {
            PredictorHotKey(key, count - Threshold_r);
            dumpKeyCount++;
            if (dumpKeyCount == dumpsize) {
                //dump
                SynopsisHashMapDump(new DumpRemoveHandler() {
                    @Override
                    public void dumpRemove(String key) {

                    }
                });
                dumpKeyCount = 0;
            }
        }
    }

    /**
     *  测试预言热词性能
     * @param key
     * @throws Exception
     */
    public void TestComputPredictorHotKey(String key) throws Exception {
        //KeyTimeModel keyTimeModel=new KeyTimeModel();
        //keyTimeModel.setStartTimeSystemTime(System.nanoTime());

        int count = countCointUtilUp();
        int dumpsize= (int) (1/Threshold_p);

        if(count>=Threshold_r){
           PredictorHotKey(key,count - Threshold_r);
            dumpKeyCount++;
            //totalKeyCount++;
            if(dumpKeyCount==dumpsize){
                //dump
                //long startTimeSystemTime= System.currentTimeMillis();
                SynopsisHashMapDump(new DumpRemoveHandler() {
                    @Override
                    public void dumpRemove(String key) {

                    }
                });
                //long endTimeSystemTime = System.currentTimeMillis();
                //long timelong = (endTimeSystemTime-startTimeSystemTime);
                //LOG.debug("dump totalTime:"+timelong+" ms");
                dumpKeyCount=0;
            }

            //keyTimeModel.setEndTimeSystemTime(System.nanoTime());
            //double delayTime=keyTimeModel.getdelayTime();
            //totalDelayTime+=delayTime;
            //屏蔽文件输出代码降低处理延迟
            //FileUtil.writeAppendTxtFile(new File("/root/TJ/keyTimeModel.txt"),String.valueOf(delayTime)+"\n");
        }
    }

    /**
     * 预测key是否是HotKey
     * @param key 被预测的key值
     * @return
     */
    public boolean isHotKey(String key){
       if(!predictHotKeyMap.containsKey(key))
           return false;
        if(predictHotKeyMap.get(key).cardinality() >= 2)
            return true;
        else
            return false;
    }

    public long getTotalDelayTime() {
        return totalDelayTime;
    }

    public void setTotalDelayTime(long totalDelayTime) {
        this.totalDelayTime = totalDelayTime;
    }

    public long getTotalKeyCount() {
        return totalKeyCount;
    }

    public void setTotalKeyCount(long totalKeyCount) {
        this.totalKeyCount = totalKeyCount;
    }

    public SynopsisHashMap<String, BitSet> getPredictHotKeyMap() {
        return predictHotKeyMap;
    }

    public void setPredictHotKeyMap(SynopsisHashMap<String, BitSet> predictHotKeyMap) {
        this.predictHotKeyMap = predictHotKeyMap;
    }
}
