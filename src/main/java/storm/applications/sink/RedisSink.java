package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import static storm.applications.constants.ReinforcementLearnerConstants.*;
import storm.applications.util.ConfigUtility;

public class RedisSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    
    private Jedis jedis;
    private String queue;
    
    private String queueKey = BaseConf.REDIS_SINK_QUEUE;

    @Override
    public void initialize() {
        super.initialize();
        
        queue = ConfigUtility.getString(config, queueKey);
        
        String redisHost = ConfigUtility.getString(config, BaseConf.REDIS_HOST);
        int redisPort    = ConfigUtility.getInt(config, BaseConf.REDIS_PORT);
        
        jedis = new Jedis(redisHost, redisPort);
    }

    @Override
    public void execute(Tuple input) {
        String content = formatter.format(input);
        jedis.lpush(queue, content);
    }

    public void setQueueKey(String queueKey) {
        this.queueKey = queueKey;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}