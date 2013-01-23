package storm.cookbook.tfidf;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TermTopology {

	public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));                
            }
        }
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc) {
        //TODO: lookup the track terms for the spout or get them from the command line
        TwitterTrackSpout twitterSpout = new TwitterTrackSpout(10000, new String[]{"google"}, 100);
        TridentTopology topology = new TridentTopology();    
        /*TridentState termCounts =
                topology.newStream("tweetSpout", twitterSpout)
                  .parallelismHint(16)
                  .each(new Fields("status","links"), new FetchDocuments(), new Fields("documents"))
                  .each(new Fields("document"), new Tokenize(), new Fields("words"))
                  .each(new Fields("words"), new StopFilter(), new Fields("terms"))
                  .each(new Fields("terms"), new StemFunction, new Fields("stems"))
                  .aggregate(new Fields("stem","document"), new TFAggregate, new Fields("tf"))
                  .aggregate(new Fields("stems","document"), new DFAggregate, new Fields("df"))
                  .aggregate(new Fields("document"), new DAggregate, new Fields("d"))
                  .aggregate(new Fields("d","df"), new IDFAggregate,new Fields("idf"))
                  .persistentAggregate(new MemoryMapState.Factory(), new Fields("td","idf"), 
                  						new TFIDFAggregate, new Fields("tfidf"))         
                  .parallelismHint(16);*/
                
        /*topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(termCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
                ;*/
        return topology.build();
    }
    
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if(args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for(int i=0; i<100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));        
        }
    }

}
