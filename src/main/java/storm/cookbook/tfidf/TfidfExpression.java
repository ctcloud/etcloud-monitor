package storm.cookbook.tfidf;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TfidfExpression extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(DocumentTokenizer.class); 
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//get of the tuple tf, d and df
		
		//calculate based on the formula
		
		//emit the term and associated result.
		
	}


}
