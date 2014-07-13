package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import com.hmsonline.storm.cassandra.bolt.AbstractBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.constants.BaseConstants.BaseConst;
import storm.applications.constants.LogProcessingConstants;
import storm.applications.util.ConfigUtility;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CassandraBatchSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraBatchSink.class);
    protected static final String configKey = BaseConst.CASSANDRA_CONFIG_KEY;
    
    protected String columnFamilyKey = BaseConf.CASSANDRA_SINK_CF;
    protected String rowKeyFieldKey  = BaseConf.CASSANDRA_SINK_ROW_KEY_FIELD;
    protected String ackStrategyKey  = BaseConf.CASSANDRA_SINK_ACK_STRATEGY;
    
    protected AbstractBatchingBolt bolt;
    
    @Override
    public void initialize() {
        String host           = ConfigUtility.getString(config, BaseConf.CASSANDRA_HOST);
        String keyspace       = ConfigUtility.getString(config, BaseConf.CASSANDRA_KEYSPACE);
        String columnFamily   = ConfigUtility.getString(config, columnFamilyKey);
        String rowKeyField    = ConfigUtility.getString(config, rowKeyFieldKey);
        String ackStrategyStr = ConfigUtility.getString(config, ackStrategyKey);
        
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

    public void setColumnFamilyKey(String columnFamilyKey) {
        this.columnFamilyKey = columnFamilyKey;
    }

    public void setRowKeyFieldKey(String rowKeyFieldKey) {
        this.rowKeyFieldKey = rowKeyFieldKey;
    }

    public void setAckStrategyKey(String ackStrategyKey) {
        this.ackStrategyKey = ackStrategyKey;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
