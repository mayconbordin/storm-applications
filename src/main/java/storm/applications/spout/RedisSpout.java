package storm.applications.spout;

import backtype.storm.utils.Utils;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.parser.Parser;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;
import storm.applications.util.StreamValues;

/**
 * Adapted from https://github.com/sorenmacbeth/storm-redis-pubsub
 */
public class RedisSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSpout.class);
    
    private LinkedBlockingQueue<String> queue;
    private JedisPool pool;
    private Parser parser;
    
    private String hostKey      = BaseConf.REDIS_HOST;
    private String portKey      = BaseConf.REDIS_PORT;
    private String patternKey   = BaseConf.REDIS_PATTERN;
    private String queueSizeKey = BaseConf.REDIS_QUEUE_SIZE;
    private String parserKey    = BaseConf.SPOUT_PARSER;
        
    @Override
    protected void initialize() {
        String parserClass = ConfigUtility.getString(config, parserKey);
        String host        = ConfigUtility.getString(config, hostKey);
        String pattern     = ConfigUtility.getString(config, patternKey);
        int port           = ConfigUtility.getInt(config, portKey);
        int queueSize      = ConfigUtility.getInt(config, queueSizeKey);
        
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        queue = new LinkedBlockingQueue<>(queueSize);
        pool  = new JedisPool(new JedisPoolConfig(), host, port);

        ListenerThread listener = new ListenerThread(queue, pool, pattern);
        listener.start();
    }

    @Override
    public void nextTuple() {
        String message = queue.poll();
        
        if (message == null) {
            Utils.sleep(50);
        } else {
            StreamValues values = parser.parse(message);
        
            if (values != null) {
                collector.emit(values.getStreamId(), values);
            }
        }
    }
    
    private class ListenerThread extends Thread {
        private LinkedBlockingQueue<String> queue;
        private JedisPool pool;
        private String pattern;

        public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
            this.queue = queue;
            this.pool = pool;
            this.pattern = pattern;
        }

        @Override
        public void run() {
            Jedis jedis = pool.getResource();
            
            try {
                jedis.psubscribe(new JedisListener(queue), pattern);
            } finally {
                pool.returnResource(jedis);
            }
        }
    }
    
    private class JedisListener extends JedisPubSub {
        private LinkedBlockingQueue<String> queue;

        public JedisListener(LinkedBlockingQueue<String> queue) {
            this.queue = queue;
        }
        
        @Override
        public void onMessage(String channel, String message) {
            queue.offer(message);
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
            queue.offer(message);
        }

        @Override
        public void onPSubscribe(String channel, int subscribedChannels) { }

        @Override
        public void onPUnsubscribe(String channel, int subscribedChannels) { }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) { }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) { }
    }

    public void setHostKey(String hostKey) {
        this.hostKey = hostKey;
    }

    public void setPortKey(String portKey) {
        this.portKey = portKey;
    }

    public void setPatternKey(String patternKey) {
        this.patternKey = patternKey;
    }

    public void setQueueSizeKey(String queueSizeKey) {
        this.queueSizeKey = queueSizeKey;
    }

    public void setParserKey(String parserKey) {
        this.parserKey = parserKey;
    }
}
