package storm.cookbook.tfidf;

import backtype.storm.Config;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterTrackSpout extends BaseRichSpout {
	Logger LOG = LoggerFactory.getLogger(TwitterTrackSpout.class); 
    SpoutOutputCollector collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream twitterStream;
    long maxQueueDepth;
    String[] trackTerms;
    
    public TwitterTrackSpout(long maxQueueDepth, String[] trackTerms) {
    	this.maxQueueDepth = maxQueueDepth;
    	this.trackTerms = trackTerms;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;
        StatusListener listener = new StatusListener() {
        	
            @Override
            public void onStatus(Status status) {
            	if(queue.size() < maxQueueDepth){
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

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(ret));
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

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
    
}
