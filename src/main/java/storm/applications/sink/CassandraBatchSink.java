package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import com.hmsonline.storm.cassandra.bolt.AbstractBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.constants.BaseConstants.BaseConst;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CassandraBatchSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBatchSink.class);
    protected static final String configKey = BaseConst.CASSANDRA_CONFIG_KEY;
    
    protected AbstractBatchingBolt bolt;
    
    @Override
    public void initialize() {
        String host           = config.getString(getConfigKey(BaseConf.CASSANDRA_SINK_CF));
        String keyspace       = config.getString(getConfigKey(BaseConf.CASSANDRA_KEYSPACE));
        String columnFamily   = config.getString(getConfigKey(BaseConf.CASSANDRA_SINK_CF));
        String rowKeyField    = config.getString(getConfigKey(BaseConf.CASSANDRA_SINK_ROW_KEY_FIELD));
        String ackStrategyStr = config.getString(getConfigKey(BaseConf.CASSANDRA_SINK_ACK_STRATEGY));
        
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(BaseConf.CASSANDRA_HOST, host);
        clientConfig.put(BaseConf.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {keyspace}));
        config.put(configKey, clientConfig);
        
        bolt = createBolt(keyspace, columnFamily, rowKeyField);

        AckStrategy ackStrategy = BaseConst.CASSANDRA_ACK_STRATEGIES.get(ackStrategyStr);
        if (ackStrategy == null) {
            ackStrategy = AckStrategy.ACK_IGNORE;
        }
        
        bolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
        bolt.prepare(config, context, collector);
    }
    
    protected AbstractBatchingBolt createBolt(String keyspace, String columnFamily, String rowKeyField) {
        TupleMapper tupleMapper = new DefaultTupleMapper(keyspace, columnFamily, rowKeyField);
        return new CassandraBatchingBolt(configKey, tupleMapper);
    }
    
    @Override
    public void execute(Tuple input) {
        bolt.execute(input);
    }

    @Override
    public void cleanup() {
        super.cleanup();
        bolt.cleanup();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
