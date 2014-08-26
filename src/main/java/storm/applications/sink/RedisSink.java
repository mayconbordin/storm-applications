package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import static storm.applications.constants.ReinforcementLearnerConstants.*;

public class RedisSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    
    private Jedis jedis;
    private String queue;
    
    @Override
    public void initialize() {
        super.initialize();
        
        queue = config.getString(getConfigKey(BaseConf.REDIS_SINK_QUEUE));
        
        String redisHost = config.getString(getConfigKey(BaseConf.REDIS_HOST));
        int redisPort    = config.getInt(getConfigKey(BaseConf.REDIS_PORT));
        
        jedis = new Jedis(redisHost, redisPort);
    }

    @Override
    public void execute(Tuple input) {
        String content = formatter.format(input);
        jedis.lpush(queue, content);
        collector.ack(input);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}