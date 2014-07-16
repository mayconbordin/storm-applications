package storm.applications.spout;


import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import storm.applications.constants.BaseConstants.BaseConf;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * Spout which gets tweets from Twitter using OAuth Credentials.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class TwitterStreamingSpout extends AbstractSpout {
    private static final Logger LOG = Logger.getLogger(TwitterStreamingSpout.class);
     private static final JSONParser jsonParser = new JSONParser();
     
    private LinkedBlockingQueue<JSONObject> queue;
    private TwitterStream twitterStream;
    private FilterQuery filterQuery;
    
    @Override
    public void initialize() {
        this.queue = new LinkedBlockingQueue<>(1000);

        StatusListener statusListener = new TwitterStatusListener(queue);

        // Twitter stream authentication setup
        ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
        cfgBuilder.setIncludeEntitiesEnabled(true);
        cfgBuilder.setJSONStoreEnabled(true);
        
        cfgBuilder.setOAuthAccessToken(config.getString(getConfigKey(BaseConf.TWITTER_ACCESS_TOKEN)));
        cfgBuilder.setOAuthAccessTokenSecret(config.getString(getConfigKey(BaseConf.TWITTER_ACCESS_TOKEN_SECRET)));
        cfgBuilder.setOAuthConsumerKey(config.getString(getConfigKey(BaseConf.TWITTER_CONSUMER_KEY)));
        cfgBuilder.setOAuthConsumerSecret(config.getString(getConfigKey(BaseConf.TWITTER_CONSUMER_SECRET)));

        twitterStream = new TwitterStreamFactory(cfgBuilder.build()).getInstance();
        twitterStream.addListener(statusListener);

        if (filterQuery != null) {
            twitterStream.filter(filterQuery);
        } else {
            twitterStream.sample();
        }
    }

    @Override
    public void nextTuple() {
        JSONObject status = queue.poll();
        
        if (null == status) {
            //If _queue is empty sleep the spout thread so it doesn't consume resources.
            Utils.sleep(500);
        } else {
            //Emit the complete tweet to the Bolt.
            collector.emit(new Values(status));
        }
    }

    @Override
    public void close() {
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    public void setFilterQuery(FilterQuery filterQuery) {
        this.filterQuery = filterQuery;
    }
    
    private static class TwitterStatusListener implements StatusListener {
        private LinkedBlockingQueue<JSONObject> queue;

        public TwitterStatusListener(LinkedBlockingQueue<JSONObject> queue) {
            this.queue = queue;
        }
        
        @Override
        public void onStatus(final Status status) {
            try {
                String jsonStr = DataObjectFactory.getRawJSON(status);
                queue.offer((JSONObject) jsonParser.parse(jsonStr));
            } catch (ParseException ex) {
                LOG.error("Error parsing JSON encoded tweet", ex);
            }
        }

        @Override
        public void onDeletionNotice(final StatusDeletionNotice sdn) { }

        @Override
        public void onTrackLimitationNotice(final int i) { }

        @Override
        public void onScrubGeo(final long l, final long l1) { }

        @Override
        public void onStallWarning(final StallWarning stallWarning) { }

        @Override
        public void onException(final Exception e) { }
    }
}