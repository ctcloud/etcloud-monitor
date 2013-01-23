package storm.cookbook.tfidf;

import java.net.URL;

import au.id.jericho.lib.html.Source;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.URLEntity;

public class DocumentFetchFunction extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		URLEntity[] urls = (URLEntity[]) tuple.getValueByField(TfidfTopologyFields.TWEET_URLS);
		for(int i = 0; i < urls.length; i++){
			//TODO: use another lib here
			/*Source source = new Source(new URL(urls[i].getExpandedURL()));
			String renderedText = source.getRenderer().toString();
			collector.emit(new Values(renderedText));*/
		}
		//The tweet text is always a document itself
		collector.emit(new Values(tuple.getStringByField(TfidfTopologyFields.TWEET_TEXT)));

	}

}
