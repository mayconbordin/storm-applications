package org.storm.applications.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Spout which gets tweets from Twitter using OAuth Credentials.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class TwitterStreamSpout extends BaseRichSpout {
    private static final Logger LOG = Logger.getLogger(TwitterStreamSpout.class);
    private static final long serialVersionUID = -1590819539847344427L;

    private SpoutOutputCollector outputCollector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    private FilterQuery filterQuery;

    public TwitterStreamSpout() {
    }
    
    public TwitterStreamSpout(FilterQuery filterQuery) {
        this.filterQuery = filterQuery;
    }
    
    @Override
    public final void open(final Map conf, final TopologyContext context,
             final SpoutOutputCollector collector) {
        this.queue = new LinkedBlockingQueue<Status>(1000);
        this.outputCollector = collector;

        final StatusListener statusListener = new StatusListener() {
            @Override
            public void onStatus(final Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(final StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(final int i) {
            }

            @Override
            public void onScrubGeo(final long l, final long l1) {
            }

            @Override
            public void onStallWarning(final StallWarning stallWarning) {
            }

            @Override
            public void onException(final Exception e) {
            }
        };

        //Twitter stream authentication setup
        final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setIncludeEntitiesEnabled(true);

        configurationBuilder.setOAuthAccessToken((String) conf.get("OAUTH_ACCESS_TOKEN"));
        configurationBuilder.setOAuthAccessTokenSecret((String) conf.get("OAUTH_ACCESS_TOKEN_SECRET"));
        configurationBuilder.setOAuthConsumerKey((String) conf.get("OAUTH_CONSUMER_KEY"));
        configurationBuilder.setOAuthConsumerSecret((String) conf.get("OAUTH_CONSUMER_SECRET"));

        this.twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
        this.twitterStream.addListener(statusListener);

        if (filterQuery != null) {
            this.twitterStream.filter(filterQuery);
        } else {
            this.twitterStream.sample();
        }
    }

    @Override
    public final void nextTuple() {
        final Status status = queue.poll();
        if (null == status) {
            //If _queue is empty sleep the spout thread so it doesn't consume resources.
            Utils.sleep(500);
        } else {
            //Emit the complete tweet to the Bolt.
            this.outputCollector.emit(new Values(status));
        }
    }

    @Override
    public final void close() {
        this.twitterStream.cleanUp();
        this.twitterStream.shutdown();
    }

    @Override
    public final void ack(final Object id) {
    }

    @Override
    public final void fail(final Object id) {
    }

    @Override
    public final void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        //For emitting the complete tweet to the Bolt.
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}