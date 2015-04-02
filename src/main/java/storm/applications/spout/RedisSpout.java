package storm.applications.spout;

import backtype.storm.utils.Utils;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.parser.Parser;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.stream.StreamValues;

/**
 * Adapted from https://github.com/sorenmacbeth/storm-redis-pubsub
 */
public class RedisSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSpout.class);
    
    private LinkedBlockingQueue<String> queue;
    private JedisPool pool;
    private Parser parser;
   
    @Override
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConf.SPOUT_PARSER));
        String host        = config.getString(getConfigKey(BaseConf.REDIS_HOST));
        String pattern     = config.getString(getConfigKey(BaseConf.REDIS_PATTERN));
        int port           = config.getInt(getConfigKey(BaseConf.REDIS_PORT));
        int queueSize      = config.getInt(getConfigKey(BaseConf.REDIS_QUEUE_SIZE));
        
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
            List<StreamValues> tuples = parser.parse(message);
        
            if (tuples != null) {
                for (StreamValues values : tuples)
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
}
