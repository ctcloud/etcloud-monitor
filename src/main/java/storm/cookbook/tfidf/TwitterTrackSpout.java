package storm.cookbook.tfidf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterTrackSpout implements IBatchSpout {
	Logger LOG = LoggerFactory.getLogger(TwitterTrackSpout.class); 
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream twitterStream;
    long maxQueueDepth;
    String[] trackTerms;
    long batchSize;
    
    public TwitterTrackSpout(long maxQueueDepth, String[] trackTerms, long batchSize) {
    	this.maxQueueDepth = maxQueueDepth;
    	this.trackTerms = trackTerms;
    	this.batchSize = batchSize;
    }
    
    public void open(Map conf, TopologyContext context) {
        queue = new LinkedBlockingQueue<Status>(1000);
        StatusListener listener = new StatusListener() {
        	
            @Override
            public void onStatus(Status status) {
            	if(queue.size() < maxQueueDepth){
            		LOG.info("TWEET Received: " + status);
            		queue.offer(status);
            	} else {
            		LOG.error("Queue is now full, the following message is dropped: "+status);
            	}
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception e) {
            }

			@Override
			public synchronized void onStallWarning(StallWarning arg0) {
				LOG.error("Stall warning received!");
			}
            
        };
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        FilterQuery filter = new FilterQuery();
        filter.count(0);
        filter.track(trackTerms);
        twitterStream.filter(filter);
    }
    
    public void emitBatch(long batchId, TridentCollector collector) {
    	//TODO: do this better, maybe use disruptor
    	boolean controlFlag = true;
    	int count = 0;
    	while(controlFlag && (count < batchSize)){
    		Status ret = queue.poll();
            if(ret==null) {
                Utils.sleep(50);
                controlFlag = false;
            } else {
            	collector.emit(new Values(Long.toString(ret.getId()), ret.getText(),ret.getURLEntities(),ret.getHashtagEntities()));
            }
            count++;
    	}
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    

    public void fail(long batchId) {
    }

	@Override
	public void ack(long batchId) {
		//TODO: consider only removing from queue once processed, maybe use kafka instead?
		
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(TfidfTopologyFields.TWEET_ID,
				TfidfTopologyFields.TWEET_TEXT,
				TfidfTopologyFields.TWEET_URLS,
				TfidfTopologyFields.TWEET_HAS_TAGS);
	}
    
}
