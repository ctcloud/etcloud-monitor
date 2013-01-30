package storm.cookbook.tfidf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.ValueUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;

public class ResultUpdater extends BaseStateUpdater<MapState> {
	
	public static class OverwriteUpdater implements ValueUpdater<Object>{

		private Object newValue;
		public OverwriteUpdater(Object newValue){
			this.newValue = newValue;
		}
		
		@Override
		public Object update(Object stored) {
			return newValue;
		}
		
	}
	
	ProjectionFactory _groupFactory;
	Fields _groupFields;

	@Override
	public void updateState(MapState state, List<TridentTuple> tuples,
			TridentCollector collector) {
        List<ValueUpdater> updaters = new ArrayList<ValueUpdater>(tuples.size());
        for(TridentTuple t: tuples) {
        	updaters.add(new OverwriteUpdater(t.getValueByField("tfidf_value")));
        }
		List results = state.multiUpdate(tuples, updaters);
		
		for(int i=0; i<tuples.size(); i++) {
			collector.emit(new Values(tuples.get(i).getValueByField(TfidfTopologyFields.TERM), results.get(i)));
		}
	}
	
	@Override
    public void prepare(Map conf, TridentOperationContext context) {
        _groupFactory = context.makeProjectionFactory(_groupFields);
    }


}
