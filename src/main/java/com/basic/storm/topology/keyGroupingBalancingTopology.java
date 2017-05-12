package com.basic.storm.topology;

import com.basic.storm.bolt.*;
import com.basic.storm.spout.WordGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * locate com.basic.storm.topology
 * Created by 79875 on 2017/5/8.
 * 提交stormtopology任务 storm jar keyGroupingBalancing-1.0-SNAPSHOT.jar com.basic.storm.topology.keyGroupingBalancingTopology keyGroupingBalancing 8
        */
public class keyGroupingBalancingTopology {
    public static final String WORDGENERATOR_SPOUT_ID ="wordgenerator-spout";
    public static final String SPLITTER_BOLT_ID = "splitter-bolt";
    public static final String COIN_BOLT_ID= "coin-bolt";
    public static final String PREDICTOR_BOLT_ID= "predictor-bolt";
    public static final String AGGREGATOR_BOLT_ID= "aggregator-bolt";
    public static final String WORDCOUNTER_BOLT_ID ="wordcountter-spout";
    public static final String TOPOLOGY_NAME= "keyGroupingBalancing-topology";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        WordGeneratorSpout wordGeneratorSpout=new WordGeneratorSpout();
        SplitterBolt splitterBolt=new SplitterBolt();
        CoinBolt coinBolt=new CoinBolt();
        PredictorBolt predictorBolt=new PredictorBolt();
        WordCounterBolt wordCounterBolt=new WordCounterBolt();
        AggregatorBolt aggregatorBolt=new AggregatorBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);

        builder.setSpout(WORDGENERATOR_SPOUT_ID, wordGeneratorSpout, 1);
        builder.setBolt(SPLITTER_BOLT_ID, splitterBolt, 32).shuffleGrouping(WORDGENERATOR_SPOUT_ID).allGrouping(PREDICTOR_BOLT_ID);
        builder.setBolt(COIN_BOLT_ID, coinBolt, 32).shuffleGrouping(SPLITTER_BOLT_ID, "coin");
        builder.setBolt(PREDICTOR_BOLT_ID, predictorBolt,1).shuffleGrouping(COIN_BOLT_ID);
        builder.setBolt(WORDCOUNTER_BOLT_ID,wordCounterBolt, 32).fieldsGrouping(SPLITTER_BOLT_ID, "nothot", new Fields("word")).shuffleGrouping(SPLITTER_BOLT_ID, "hot");
        builder.setBolt(AGGREGATOR_BOLT_ID, aggregatorBolt, 1).fieldsGrouping(WORDCOUNTER_BOLT_ID, new Fields("word"));

        //Topology配置
        Config config=new Config();
        config.setNumWorkers(numworkers);//设置两个Worker进程 10
        if(args[0].equals("local")){
            LocalCluster localCluster=new LocalCluster();

            localCluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
            Utils.sleep(50*1000);//50s
            localCluster.killTopology(TOPOLOGY_NAME);
            localCluster.shutdown();
        }else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

    }
}
