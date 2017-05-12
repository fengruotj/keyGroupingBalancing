package com.basic.storm.spout;

import com.basic.storm.util.HdfsOperationUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate com.basic.storm.spout
 * Created by 79875 on 2017/5/6.
 */
public class WordGeneratorSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;   //default
    private FileReader file = null;         //filereader
    private BufferedReader reader = null;   //bufferreader
    private FileWriter fileWriter;
    private ConcurrentHashMap<UUID, Values> pending;
    private FSDataInputStream fsDataInputStream;
    private static final String inputFile="/user/root/flinkwordcount/input/resultTweets.txt";
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this. pending = new ConcurrentHashMap<UUID, Values>();
        try {
            this.fsDataInputStream= HdfsOperationUtil.getFs().open(new Path(inputFile));
            this.reader=new BufferedReader(new InputStreamReader(fsDataInputStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        String word=null;

        try {
            word = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (word == null)
            return;
        UUID msgId = UUID.randomUUID();
        Values value = new Values(word);
        this.collector.emit(value,msgId);    //read a line, emit as a word
        this.pending.put(msgId,value);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }
    @Override
    public void fail(Object msgId) {
        this.collector.emit(pending.get(msgId),msgId);
    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
