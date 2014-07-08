package org.storm.applications.sink;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import static org.storm.applications.ReinforcementLearnerConstants.*;
import org.storm.applications.sink.BaseSink;
import org.storm.applications.util.ConfigUtility;

public class RedisActionSink extends BaseSink {
    private Jedis jedis;
    private String queue;
    
    public RedisActionSink(String queue) {
        this.queue = queue;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        
        // action output queue		
        String redisHost = ConfigUtility.getString(stormConf, "redis.server.host");
        int redisPort = ConfigUtility.getInt(stormConf,"redis.server.port");
        jedis = new Jedis(redisHost, redisPort);
    }

    public void execute(Tuple input) {
        String eventID = input.getStringByField(EVENT_ID);
        String[] actions = (String[]) input.getValueByField(ACTIONS);
        
        String actionList = actions.length > 1 ? StringUtils.join(actions) : actions[0] ;
        jedis.lpush(queue, eventID + "," + actionList);
    }
}