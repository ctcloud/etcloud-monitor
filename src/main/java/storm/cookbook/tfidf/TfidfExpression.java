package storm.cookbook.tfidf;

import java.util.Map;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;

public class TfidfExpression extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(TfidfExpression.class);
	private static final long serialVersionUID = 1L;
	private DRPCClient client = null;
	
	public void prepare(Map conf, TridentOperationContext context){
		//todo: add this to the config mappings
		if("true".equals(conf.get("LOCAL")))
				client = new DRPCClient("localhost", 3772);
	}
	
	private String execute(String function, String args) throws TException, DRPCExecutionException{
		if(client != null)
			return client.execute(function, args);
		//the test values are just there for testing purposes
		if(function.equals("d"))
			return "100";
		return "50";
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			String result = execute("dQuery", "twitter");
			double d = Double.parseDouble(result);
			result = execute("dfQuery", tuple.getStringByField("term"));
			double df = Double.parseDouble(result);
			double tf = (double) tuple.getLongByField("tf");
			double tfidf = tf * Math.log(d / (1.0 + df));
			LOG.debug("Emitting new TFIDF(term,Document): ("
					+ tuple.getStringByField("term") + ","
					+ tuple.getStringByField("documentId") + ") = " + tfidf);
			collector.emit(new Values(tfidf));
		} catch (Exception e) {
			LOG.error(e.getStackTrace().toString());
		} 
		
	}

}
