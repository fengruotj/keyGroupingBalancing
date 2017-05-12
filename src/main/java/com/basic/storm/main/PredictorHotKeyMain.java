package com.basic.storm.main;

import com.basic.storm.util.PredictorHotKeyUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;

/**
 * locate com.basic.storm.main
 * Created by 79875 on 2017/5/9.
 * 运行方法 java -cp keyGroupingBalancing-1.0-SNAPSHOT.jar -Dcom.sun.management.jmxremote.port=8999 -Dcom.sun.managent.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false com.basic.storm.main.PredictorHotKeyMain
 */
public class PredictorHotKeyMain {
    private static final Log LOG = LogFactory.getLog(PredictorHotKeyMain.class);
    private static PredictorHotKeyUtil predictorHotKeyUtil=PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();

    public static void main(String[] args) throws IOException {
//        String inputFile="/user/root/flinkwordcount/input/resultTweets.txt";
//        FileSystem fs = HdfsOperationUtil.getFs();
//        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
//        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));

        FileInputStream dataInputStream=new FileInputStream("D:\\dataresult\\resultTweets.txt");
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));
        long startTimeSystemTime= System.currentTimeMillis();
        String text=null;
        //long keycount=0L;
        while ((text=bufferedReader.readLine())!=null){
            predictorHotKeyUtil.simpleComputPredictorHotKey(text);
            //keycount++;
        }
        //LOG.info("keycount: "+keycount);
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
