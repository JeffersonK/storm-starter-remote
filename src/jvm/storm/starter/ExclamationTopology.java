package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;


public class ExclamationTopology {
    
    public static class ExclamationBolt implements IRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        @Override
        public void cleanup() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


    }
    
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(1, new TestWordSpout(), 10);        
        builder.setBolt(2, new ExclamationBolt(), 3)
                .shuffleGrouping(1);
        builder.setBolt(3, new ExclamationBolt(), 2)
                .shuffleGrouping(2);

        Config conf = new Config();
        conf.setNumWorkers(10);
        conf.setMaxSpoutPending(5000);
        StormSubmitter ss = new StormSubmitter();
        try {
			ss.submitTopology("exclamation", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}                
    }
}
