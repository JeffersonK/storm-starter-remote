package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.MergeObjects;
import storm.starter.bolt.RankObjects;
import storm.starter.bolt.RollingCountObjects;

public class RollingTopWords {
    
    public static void main(String[] args) throws Exception {
        
        final int TOP_N = 3;
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(1, new TestWordSpout(), 5);
        
        builder.setBolt(2, new RollingCountObjects(60, 10), 4)
                 .fieldsGrouping(1, new Fields("word"));
        builder.setBolt(3, new RankObjects(TOP_N), 4)
                 .fieldsGrouping(2, new Fields("obj"));
        builder.setBolt(4, new MergeObjects(TOP_N))
                 .globalGrouping(3);
        
        
        Config conf = new Config();
        conf.setNumWorkers(10);
        conf.setMaxSpoutPending(5000);
        StormSubmitter ss = new StormSubmitter();
        try {
			ss.submitTopology("rolling-demo", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}                
        
    }    
}
