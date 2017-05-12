package com.basic.storm.bolt;

import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * locate com.basic.storm.bolt
 * Created by 79875 on 2017/5/8.
 */
public class SplitterBolt extends BaseRichBolt {
    public static final String WORDGENERATOR_SPOUT_ID ="wordgenerator-spout";
    private OutputCollector collector;
    private CountingBloomFilter bf;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.bf = new CountingBloomFilter(16,4,1);
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals(WORDGENERATOR_SPOUT_ID)){
            String word = tuple.getStringByField("word");
            collector.emit("coin", new Values(word));
            Key ky = new Key(word.getBytes());
            if(bf.membershipTest(ky))
                collector.emit("hot", tuple, new Values(word));
            else
                collector.emit("nothot", tuple, new Values(word));

        }else {
            String key = tuple.getStringByField("word");
            Integer type = tuple.getIntegerByField("type");
            Key hk = new Key(key.getBytes());
            if(!bf.membershipTest(hk) && type.equals(1))
                bf.add(hk);
            if(bf.membershipTest(hk) && type.equals(0))
                bf.delete(hk);
        }
        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("coin", new Fields("word"));
        declarer.declareStream("hot", new Fields("word"));
        declarer.declareStream("nothot", new Fields("word"));
    }

}
