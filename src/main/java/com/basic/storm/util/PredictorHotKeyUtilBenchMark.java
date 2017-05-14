package com.basic.storm.util;

import com.basic.storm.inter.DumpRemoveHandler;
import com.basic.storm.model.HotKeyUpdateTime;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static com.basic.storm.Constraints.*;

/**
 * locate com.basic.storm.util
 * Created by 79875 on 2017/5/6.
 */
public class PredictorHotKeyUtilBenchMark implements Serializable{
    //做benchMark需要测试性能
    private CountingBloomFilter cbf=new CountingBloomFilter(16,4,1);

    private static volatile PredictorHotKeyUtilBenchMark predictorHotKeyUtil = null;

    private SynopsisHashMap<String, BitSet> predictHotKeyMap = new SynopsisHashMap<String, BitSet>();

    private Random random=new Random();

    private Timer timer=new Timer();
    /**
     * 线程池
     * 创建一个可缓存线程池，如果线程池长度超过处理需要，可灵活回收空闲线程，若无可回收，则新建线程。
     */
    ExecutorService executor = Executors.newFixedThreadPool(20);

    private long dumpKeyCount=0L;
    private static final Log LOG = LogFactory.getLog(PredictorHotKeyUtilBenchMark.class);

    private long totalDelayTime=0L;//统计中奖的Key的总实际延迟
    private long totalKeyCount=0L;  //统计中奖的Key的总个数

    private Queue<HotKeyUpdateTime> keyUpdateTimes=new ArrayDeque<>();

    public PredictorHotKeyUtilBenchMark() {
    }

    public static PredictorHotKeyUtilBenchMark getPredictorHotKeyUtilInstance(){
        if(null == predictorHotKeyUtil)
        {
            synchronized (PredictorHotKeyUtilBenchMark.class)
            {
                predictorHotKeyUtil=new PredictorHotKeyUtilBenchMark();
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
     * 热词表中热词衰减所有
     */
    public void SynopsisHashMapAllDump(DumpRemoveHandler dumpRemoveHandler) {
        int dumpsize = (int) (1 / Threshold_p);
        dumpKeyCount++;
        if (dumpKeyCount == dumpsize) {
            //dump 衰减所有key
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
            dumpKeyCount = 0;
        }
    }

    /**
     * 热词表中热词随机衰减
     */
    public void SynopsisHashMapRandomDump(DumpRemoveHandler dumpRemoveHandler) {
        int size=predictHotKeyMap.size;
        long startTimeSystemTime=System.currentTimeMillis();
        Iterator<Map.Entry<String, BitSet>> iterator = predictHotKeyMap.newEntryIterator();
        while (iterator.hasNext()){
            Map.Entry<String, BitSet> next = iterator.next();
            if (random.nextDouble()> Threshold_p){
                continue;
            }
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
        long endTimeSystemTime=System.currentTimeMillis();
        long taotalTime=endTimeSystemTime-startTimeSystemTime;
    }

    /**
     * 预言HotKey
     * @param key
     * @param coinCount
     */
    public void PredictorHotKey(String key,int coinCount){
        int count=coinCount-Threshold_r;
        BitSet bitmap=null;
        if(predictHotKeyMap.get(key)!=null)
            bitmap = (BitSet) predictHotKeyMap.get(key);
        else
            bitmap=new BitSet(Threshold_l);

        bitmap.set(coinCount);
        predictHotKeyMap.put(key,bitmap);

        if(bitmap.cardinality() >= 2)
        {
            //写入数据库？
            Key hk = new Key(key.getBytes());
        }
    }


    private long startTimeSystemTime=0L;

    public void setStartTimeSystemTime(long startTimeSystemTime) {
        this.startTimeSystemTime = startTimeSystemTime;
    }

    /**
     * 测试预言HotKeyUpdateTime 预言主要类
     * @param key
     * @param coninCount
     */
    public void TestPredictorHotKeyUpdateTime(String key,int coninCount){
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
            Key hk = new Key(key.getBytes());
            if(!cbf.membershipTest(hk)){
                //如果cbf 中没有这个key 成员就 添加到cbf中
                cbf.add(hk);
                //测试写入文件多线程执行
                long nowTimeSystemTime=System.currentTimeMillis();
                keyUpdateTimes.add(new HotKeyUpdateTime(key,nowTimeSystemTime-startTimeSystemTime,new Timestamp(nowTimeSystemTime)));
            }
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
            SynopsisHashMapAllDump(new DumpRemoveHandler() {
                @Override
                public void dumpRemove(String key) {

                }
            });
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

            SynopsisHashMapAllDump(new DumpRemoveHandler() {
                @Override
                public void dumpRemove(String key) {

                }
            });

            //keyTimeModel.setEndTimeSystemTime(System.nanoTime());
            //double delayTime=keyTimeModel.getdelayTime();
            //totalDelayTime+=delayTime;
            //屏蔽文件输出代码降低处理延迟
            //FileUtil.writeAppendTxtFile(new File("/root/TJ/keyTimeModel.txt"),String.valueOf(delayTime)+"\n");
        }
    }

    /**
     *  测试预言热词 热词更新时间
     * @param key
     * @throws Exception
     */
    public void TestPredictorHotKeyUpdateTime(String key) throws Exception {

        int count = countCointUtilUp();
        int dumpsize= (int) (1/Threshold_p);

        if(count>=Threshold_r){
            TestPredictorHotKeyUpdateTime(key,count - Threshold_r);
//            SynopsisHashMapAllDump(new DumpRemoveHandler() {
//                @Override
//                public void dumpRemove(String key) {
//                    Key nohotkey = new Key(key.getBytes());
//                    if(cbf.membershipTest(nohotkey))
//                        cbf.delete(nohotkey);
//                }
//            });


            SynopsisHashMapRandomDump(new DumpRemoveHandler() {
                @Override
                public void dumpRemove(String key) {
                    Key nohotkey = new Key(key.getBytes());
                    if(cbf.membershipTest(nohotkey))
                        cbf.delete(nohotkey);
                }
            });

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
    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void outputKeyUpdateTimesQueue() throws Exception {
//        while (!keyUpdateTimes.isEmpty()){
//            HotKeyUpdateTimeTask task=new HotKeyUpdateTimeTask(keyUpdateTimes.poll());
//            executor.execute(task);
//        }
//        predictorHotKeyUtil.getExecutor().shutdown();
//        //判断是否所有的线程已经运行完
//        while (!predictorHotKeyUtil.getExecutor().isTerminated()) {
//            Thread.sleep(10);
//        }
        while (!keyUpdateTimes.isEmpty()){
            HotKeyUpdateTime poll = keyUpdateTimes.poll();
            FileUtil.writeAppendTxtFile(new File("D://HotKeyUpdateTime.txt"),poll.getHotkey()+"\t"+poll.getUpdateTime()+"\t"+poll.getTimestamp()+"\n");
        }
    }

    public class HotKeyUpdateTimeTask implements Runnable{
        private HotKeyUpdateTime hotKeyUpdateTime;
        private DataBaseUtil dataBaseUtil=DataBaseUtil.getDataBaseUtilInstance();

        public HotKeyUpdateTimeTask(HotKeyUpdateTime hotKeyUpdateTime) {
            this.hotKeyUpdateTime = hotKeyUpdateTime;
        }

        @Override
        public void run() {
            String sql="insert INTO t_hotkeyupdatetime(hotkey,updatetime,currentime) VALUES(?,?,?)";
            try {
                Connection connection=dataBaseUtil.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1,hotKeyUpdateTime.getHotkey());
                preparedStatement.setLong(2,hotKeyUpdateTime.getUpdateTime());
                preparedStatement.setTimestamp(3,hotKeyUpdateTime.getTimestamp());
                int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
                preparedStatement.closeOnCompletion();
            } catch (SQLException e) {
                System.out.println(hotKeyUpdateTime.getHotkey()+" "+hotKeyUpdateTime.getUpdateTime()+" "+hotKeyUpdateTime.getTimestamp());
                e.printStackTrace();
            }
        }
    }

}
