package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import com.hmsonline.storm.cassandra.bolt.AbstractBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.TupleCounterMapper;
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
public class CassandraCountBatchSink extends CassandraBatchSink {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCountBatchSink.class);

    private String incFieldKey = BaseConf.CASSANDRA_SINK_INC_FIELD;

    public void setIncFieldKey(String incFieldKey) {
        this.incFieldKey = incFieldKey;
    }

    @Override
    protected AbstractBatchingBolt createBolt(String keyspace, String columnFamily, String rowKeyField) {
        String incField = ConfigUtility.getString(config, incFieldKey);
        TupleCounterMapper tupleMapper = new DefaultTupleCounterMapper(keyspace, columnFamily, rowKeyField, incField);
        return new CassandraCounterBatchingBolt(configKey, tupleMapper);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
