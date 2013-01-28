package storm.cookbook.tfidf;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import trident.cassandra.CassandraState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class TermTopology {
	
	private static String[] searchTerms = new String[]{"AAPL"};
	private static String[] mimeTypes = new String[]{"application/pdf","text/html", "text/plain"};
    
    public static StormTopology buildTopology(StateFactory stateFactory) {
        //TODO: lookup the track terms for the spout or get them from the command line
        TwitterTrackSpout twitterSpout = new TwitterTrackSpout(10000, searchTerms, 100);
        TridentTopology topology = new TridentTopology();    
        GroupedStream tf =
                topology.newStream("tweetSpout", twitterSpout)
                  .parallelismHint(16)
                  .each(new Fields(TfidfTopologyFields.TWEET_ID, TfidfTopologyFields.TWEET_TEXT,TfidfTopologyFields.TWEET_URLS), 
                		  new DocumentFetchFunction(mimeTypes), new Fields(TfidfTopologyFields.DOCUMENT,
                				  TfidfTopologyFields.DOCUMENT_ID))
                  .each(new Fields(TfidfTopologyFields.DOCUMENT), new DocumentTokenizer(), 
                		  new Fields(TfidfTopologyFields.DIRTY_TERM))
                  .each(new Fields(TfidfTopologyFields.DIRTY_TERM), new TermFilter(), 
                		  new Fields(TfidfTopologyFields.TERM))
				  .groupBy(new Fields(TfidfTopologyFields.DOCUMENT_ID,TfidfTopologyFields.TERM));
        		  //add the aggregate here
        
        GroupedStream df =
                topology.newStream("tweetSpout", twitterSpout)
                  .parallelismHint(16)
                  .each(new Fields(TfidfTopologyFields.TWEET_ID, TfidfTopologyFields.TWEET_TEXT,TfidfTopologyFields.TWEET_URLS), 
                		  new DocumentFetchFunction(mimeTypes), new Fields(TfidfTopologyFields.DOCUMENT,
                				  TfidfTopologyFields.DOCUMENT_ID))
                  .each(new Fields(TfidfTopologyFields.DOCUMENT), new DocumentTokenizer(), 
                		  new Fields(TfidfTopologyFields.DIRTY_TERM))
                  .each(new Fields(TfidfTopologyFields.DIRTY_TERM), new TermFilter(), 
                		  new Fields(TfidfTopologyFields.TERM))
				  .groupBy(new Fields(TfidfTopologyFields.DOCUMENT_ID,TfidfTopologyFields.TERM));
				  //add the aggregate here
        
        GroupedStream d =
                topology.newStream("tweetSpout", twitterSpout)
                  .parallelismHint(16)
                  .each(new Fields(TfidfTopologyFields.TWEET_ID, TfidfTopologyFields.TWEET_TEXT,TfidfTopologyFields.TWEET_URLS), 
                		  new DocumentFetchFunction(mimeTypes), new Fields(TfidfTopologyFields.DOCUMENT,
                				  TfidfTopologyFields.DOCUMENT_ID))
                  .each(new Fields(TfidfTopologyFields.DOCUMENT), new DocumentTokenizer(), 
                		  new Fields(TfidfTopologyFields.DIRTY_TERM))
                  .each(new Fields(TfidfTopologyFields.DIRTY_TERM), new TermFilter(), 
                		  new Fields(TfidfTopologyFields.TERM))
				  .groupBy(new Fields(TfidfTopologyFields.DOCUMENT_ID,TfidfTopologyFields.TERM));
        			//add aggregate here
        
        Stream idf = topology.merge(new Fields(), df.toStream(), d.toStream());
        
        TridentState tfidf = topology.merge(new Fields(), tf.toStream(),idf)
                  .persistentAggregate(stateFactory,
                                     new Count(), new Fields("termFrequency"));
                  /*.aggregate(new Fields("stems","document"), new DFAggregate, new Fields("df"))
                  .aggregate(new Fields("document"), new DAggregate, new Fields("d"))
                  .aggregate(new Fields("d","df"), new IDFAggregate,new Fields("idf"))
                  .persistentAggregate(new MemoryMapState.Factory(), new Fields("td","idf"), 
                  						new TFIDFAggregate, new Fields("tfidf"))         
                  .parallelismHint(16);*/
        
        /*topology.newDRPCStream("tf", drpc)
                .stateQuery(termCounts, new Fields(), 
                		new MapGet(), new Fields(TfidfTopologyFields.DOCUMENT_ID, TfidfTopologyFields.TERM));*/
        return topology.build();
    }
    
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        
        if(args.length==0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            CassandraState.Options options = new CassandraState.Options();
            options.keyspace = "trident_test";
            options.columnFamily = "tfid";
            options.rowKey = "tf";
            StateFactory cassandraStateFactory = CassandraState.nonTransactional("localhost", options);
            cluster.submitTopology("tfidf", conf, buildTopology(cassandraStateFactory));
            Thread.sleep(60000);
        } else {
        	//TODO: initial the state appropriately here
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));        
        }
    }

}
