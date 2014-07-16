package storm.applications.sink;

import com.hmsonline.storm.cassandra.bolt.AbstractBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CassandraCountBatchSink extends CassandraBatchSink {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCountBatchSink.class);

    @Override
    protected AbstractBatchingBolt createBolt(String keyspace, String columnFamily, String rowKeyField) {
        String incField = config.getString(getConfigKey(BaseConf.CASSANDRA_SINK_INC_FIELD));
        TupleCounterMapper tupleMapper = new DefaultTupleCounterMapper(keyspace, columnFamily, rowKeyField, incField);
        
        return new CassandraCounterBatchingBolt(configKey, tupleMapper);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
