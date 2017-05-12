package com.basic.storm.topology;

import com.basic.storm.bolt.WordCounterBolt;
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
 * 提交stormtopology任务 storm jar keyGroupingBalancing-1.0-SNAPSHOT.jar com.basic.storm.topology.keyGroupingTopology keyGrouping 8
 */
public class keyGroupingTopology {
    public static final String WORDGENERATOR_SPOUT_ID ="wordgenerator-spout";
    public static final String WORDCOUNTER_BOLT_ID ="wordcountter-spout";
    public static final String TOPOLOGY_NAME= "keyGrouping-topology";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        WordGeneratorSpout wordGeneratorSpout=new WordGeneratorSpout();
        WordCounterBolt wordCounterBolt=new WordCounterBolt();

        TopologyBuilder builder=new TopologyBuilder();
        Integer numworkers=Integer.valueOf(args[1]);
        builder.setSpout(WORDGENERATOR_SPOUT_ID, wordGeneratorSpout, 1);
        builder.setBolt(WORDCOUNTER_BOLT_ID,wordCounterBolt, 32).fieldsGrouping(WORDGENERATOR_SPOUT_ID, new Fields("word"));

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
