package com.basic.storm.benchmark;

import com.basic.storm.util.PredictorHotKeyUtilBenchMark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;

/**
 * locate com.basic.storm.benchmark
 * Created by 79875 on 2017/5/12.
 * java -cp keyGroupingBalancing-1.0-SNAPSHOT.jar com.basic.storm.benchmark.PredictHotKeyUpdateTimeBenchMark
 */
public class PredictHotKeyUpdateTimeBenchMark {
    private static final Log LOG = LogFactory.getLog(PredictHotKeyUpdateTimeBenchMark.class);
    private static PredictorHotKeyUtilBenchMark predictorHotKeyUtil=PredictorHotKeyUtilBenchMark.getPredictorHotKeyUtilInstance();

    public static void main(String[] args) throws Exception {
//        String inputFile = "/user/root/flinkwordcount/input/resultTweets.txt";
//        FileSystem fs = HdfsOperationUtil.getFs();
//        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataInputStream));

        FileInputStream dataInputStream=new FileInputStream("D:\\dataresult\\resultTweets.txt");
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));

        long startTimeSystemTime = System.currentTimeMillis();
        predictorHotKeyUtil.setStartTimeSystemTime(startTimeSystemTime);
        long endTimeSystemTime = 0L;
        long tupleCount = 0L;

        String text = null;
        while ((text = bufferedReader.readLine()) != null) {
            predictorHotKeyUtil.TestPredictorHotKeyUpdateTime(text);
            tupleCount++;
        }
        endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:" + new Timestamp(startTimeSystemTime));
        LOG.info("endTime:" + new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime - startTimeSystemTime) / 1000;
        LOG.info("totalTime:" + timelong + " s" + "------or------" + timelong / 60 + " min");
        LOG.info("tupleCount: " + tupleCount + " avg: " + tupleCount / timelong);

        predictorHotKeyUtil.outputKeyUpdateTimesQueue();
        LOG.info("ExecutorService run over");
        System.exit(0);
    }
}
