package com.basic.storm.benchmark;

import com.basic.storm.model.HotKeyMapSize;
import com.basic.storm.util.DataBaseUtil;
import com.basic.storm.util.PredictorHotKeyUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

/**
 * locate com.basic.storm.main
 * Created by 79875 on 2017/5/9.
 * 运行方法 java -cp keyGroupingBalancing-1.0-SNAPSHOT.jar com.basic.storm.benchmark.PredictorHotKeyBenchMark
 */
public class PredictorHotKeyBenchMark {
    private static final Log LOG = LogFactory.getLog(PredictorHotKeyBenchMark.class);
    private static PredictorHotKeyUtil predictorHotKeyUtil=PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();
    private static DataBaseUtil dataBaseUtil=DataBaseUtil.getDataBaseUtilInstance();
    private static Queue<HotKeyMapSize> hotKeyMapSizes=new ArrayDeque<>();

    public static void main(String[] args) throws Exception {
//        String inputFile="/user/root/flinkwordcount/input/resultTweets.txt";
//        FileSystem fs = HdfsOperationUtil.getFs();
//        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
//        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));
        FileInputStream dataInputStream=new FileInputStream("D:\\dataresult\\resultTweets.txt");
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));

        Timer timer=new Timer();

        long startTimeSystemTime= System.currentTimeMillis();
        long endTimeSystemTime = 0L;
        long tupleCount= 0L;
        //FileUtil.deleteFile("/root/TJ/keyTimeModel.txt");
        //FileUtil.createFile("/root/TJ/keyTimeModel.txt");//如果不存在文件名就新建一个文件

        //设置计时器每500ms计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                int size = predictorHotKeyUtil.getPredictHotKeyMap().size();
                int length = predictorHotKeyUtil.getPredictHotKeyMap().getLength();
                hotKeyMapSizes.add(new HotKeyMapSize(size,length));
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒

        String text=null;
        while ((text=bufferedReader.readLine())!=null){
            predictorHotKeyUtil.TestComputPredictorHotKey(text);
            tupleCount++;
        }
        endTimeSystemTime=System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        LOG.info("tupleCount: "+tupleCount+" avg: "+tupleCount/timelong);

        //double totalDelayTime = predictorHotKeyUtil.getTotalDelayTime();
        //long totalKeyCount=predictorHotKeyUtil.getTotalKeyCount();
        //LOG.info("TotalDelayTime: "+totalDelayTime+" avg: "+totalDelayTime/totalKeyCount);
        timer.cancel();//cancel Timer

        String sql="insert INTO t_predicthotkey(keysize,tablelength) VALUES(?,?)";
        Connection connection = dataBaseUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        while (!hotKeyMapSizes.isEmpty()){
            HotKeyMapSize poll = hotKeyMapSizes.poll();
            preparedStatement.setInt(1,poll.getKeysize());
            preparedStatement.setInt(2,poll.getTablelength());
            preparedStatement.executeUpdate();
        }
        preparedStatement.close();
        LOG.info("DataBaseService run over");
        System.exit(0);
    }
}
