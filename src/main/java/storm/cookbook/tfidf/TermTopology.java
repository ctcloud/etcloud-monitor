package storm.cookbook.tfidf;

import java.io.Serializable;
import java.util.Arrays;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import trident.cassandra.CassandraState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TermTopology {

	private static String[] searchTerms = new String[] { "AAPL", "Mac",
			"iPhone", "iStore", "Apple" };
	private static String[] mimeTypes = new String[] { "application/pdf",
			"text/html", "text/plain" };

	private static StateFactory getStateFactory(String rowKey) {
		CassandraState.Options options = new CassandraState.Options();
		options.keyspace = "trident_test";
		options.columnFamily = "tfid";
		options.rowKey = rowKey;
		return CassandraState.nonTransactional("localhost", options);
	}

	public static StormTopology getTwitterTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new TwitterSpout(searchTerms, 1000),
				1);

		builder.setBolt("publishBolt", new PublishURLBolt(), 2)
				.shuffleGrouping("twitterSpout");

		return builder.createTopology();
	}

	@SuppressWarnings("unchecked")
	public static StormTopology buildTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();
		
		FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1, 
				new Values("http://t.co/hP5PM6fm"), 
				new Values("http://t.co/xSFteG23"));
		testSpout.setCycle(true);

		Stream documentStream = topology
				.newStream("tweetSpout", testSpout)
				.parallelismHint(20)
				.each(new Fields("url"),
						new DocumentFetchFunction(mimeTypes),
						new Fields("document", "documentId", "source"));

		Stream termStream = documentStream
				.parallelismHint(20)
				.each(new Fields("document"), new DocumentTokenizer(),
						new Fields("dirtyTerm"))
				.each(new Fields("dirtyTerm"), new TermFilter(),
						new Fields("term"))
				.project(new Fields("term","documentId","source"));

		TridentState dfState = termStream.groupBy(new Fields("term"))
				.persistentAggregate(getStateFactory("df"), new Count(),
						new Fields("df"));
		
		TridentState dState = termStream.groupBy(new Fields("source"))
				.persistentAggregate(getStateFactory("d"), new Count(), new Fields("d"));
		
		topology.newDRPCStream("dQuery",drpc)
							.each(new Fields("args"), new Split(), new Fields("source"))
							.stateQuery(dState, new Fields("source"), new MapGet(),
									new Fields("d_term", "currentD"));

		topology.newDRPCStream("dfQuery",drpc)
							.each(new Fields("args"), new Split(), new Fields("term"))
							.stateQuery(dfState, new Fields("term"),
									new MapGet(), new Fields("currentDf"));
		
		Stream tfidfStream = termStream.groupBy(new Fields("documentId", "term"))
				.aggregate(new Count(), new Fields("tf"))
				.each(new Fields("term","documentId", "tf"),
						new TfidfExpression(), new Fields("tfidf"));
		
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);

		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			//cluster.submitTopology("twitter", conf, getTwitterTopology());
			cluster.submitTopology("tfidf", conf, buildTopology(drpc));
			Thread.sleep(60000);
		} else {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
		}
	}

}
