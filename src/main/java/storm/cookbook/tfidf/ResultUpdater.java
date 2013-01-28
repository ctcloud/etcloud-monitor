package storm.cookbook.tfidf;

import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.StateUpdater;

public class ResultUpdater implements StateUpdater {

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateState(State state, List tuples, TridentCollector collector) {
		// TODO Auto-generated method stub

	}

}
