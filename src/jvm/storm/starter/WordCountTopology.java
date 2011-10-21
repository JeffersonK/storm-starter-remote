package storm.starter;

import storm.starter.spout.RandomSentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;


public class WordCountTopology {
    public static class SplitSentence extends ShellBolt implements IRichBolt {
        
        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }  
    
    public static class WordCount implements IBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void prepare(Map conf, TopologyContext context) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if(count==null) count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void cleanup() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(1, new RandomSentenceSpout(), 5);
        
        builder.setBolt(2, new SplitSentence(), 8)
                 .shuffleGrouping(1);
        builder.setBolt(3, new WordCount(), 12)
                 .fieldsGrouping(2, new Fields("word"));
        
                
        Config conf = new Config();
        conf.setNumWorkers(10);
        conf.setMaxSpoutPending(5000);
        StormSubmitter ss = new StormSubmitter();
        ss.submitTopology("word-count", conf, builder.createTopology());
        
    }
}
