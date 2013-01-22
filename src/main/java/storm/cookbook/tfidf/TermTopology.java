package storm.cookbook.tfidf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TermTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;

	public TermTopology() {
		
	}

	public TopologyBuilder getBuilder() {
		return builder;
	}

	public LocalCluster getLocalCluster() {
		return cluster;
	}

	public Config getConf() {
		return conf;
	}

	public void runLocal(int runTime) {
		conf.setDebug(true);
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}

	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public void runCluster(String name, String redisHost, String cassandraHost)
			throws AlreadyAliveException, InvalidTopologyException {
		conf.setNumWorkers(20);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {

		TermTopology topology = new TermTopology();

		if (args != null && args.length > 1) {
			topology.runCluster(args[0], args[1], args[2]);
		} else {
			if (args != null && args.length == 1)
				System.out
						.println("Running in local mode, redis ip missing for cluster run");
			topology.runLocal(10000);
		}

	}

}
