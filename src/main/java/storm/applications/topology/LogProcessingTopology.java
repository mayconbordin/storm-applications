
package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.applications.bolt.GeoStatsBolt;
import storm.applications.bolt.GeographyBolt;
import storm.applications.bolt.LogEventParserBolt;
import storm.applications.bolt.LogParserBolt;
import storm.applications.bolt.PrinterBolt;
import storm.applications.bolt.StatusCountBolt;
import storm.applications.bolt.VolumeCountBolt;
import storm.applications.constants.LogProcessingConstants.Component;
import storm.applications.constants.LogProcessingConstants.Conf;
import storm.applications.constants.LogProcessingConstants.Field;
import storm.applications.topology.AbstractTopology;
import storm.applications.util.ConfigUtility;
import storm.applications.util.ip.HttpIPResolver;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessingTopology extends AbstractTopology {
    private SpoutConfig kafkaConfig;
    private CassandraCounterBatchingBolt logPersistenceBolt;
    private CassandraBatchingBolt statusPersistenceBolt;
    private CassandraBatchingBolt countryStatsPersistenceBolt;
    
    public LogProcessingTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void prepare() {
        String kafkaZookeeper = ConfigUtility.getString(config, Conf.KAFKA_HOSTS);
        String kafkaTopic     = ConfigUtility.getString(config, Conf.KAFKA_TOPIC);
        String kafkaPath      = ConfigUtility.getString(config, Conf.KAFKA_ZOOKEEPER_PATH);
        String kafkaId        = ConfigUtility.getString(config, Conf.KAFKA_CONSUMER_ID);
        
        String cassandraKeyspace = ConfigUtility.getString(config, Conf.CASSANDRA_KEYSPACE);
        String cassandraHost     = ConfigUtility.getString(config, Conf.CASSANDRA_HOST);
        String countColumnFamily = ConfigUtility.getString(config, Conf.CASSANDRA_COUNT_CF_NAME);
        String statusColumnFamily = ConfigUtility.getString(config, Conf.CASSANDRA_STATUS_CF_NAME);
        String countryColumnFamily = ConfigUtility.getString(config, Conf.CASSANDRA_COUNTRY_CF_NAME);
        
        BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
        kafkaConfig = new SpoutConfig(brokerHosts, kafkaTopic, kafkaPath, kafkaId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        String configKey = "cassandra-config";
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(Conf.CASSANDRA_HOST, cassandraHost);
        clientConfig.put(Conf.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {cassandraKeyspace}));
        config.put(configKey, clientConfig);
        
        
        // Create a bolt that writes to the "CASSANDRA_COUNT_CF_NAME" column family and uses the Tuple field
        // "LOG_TIMESTAMP" as the row key and "LOG_INCREMENT" as the increment value for atomic counter
        logPersistenceBolt = new CassandraCounterBatchingBolt(
                configKey, new DefaultTupleCounterMapper(cassandraKeyspace, 
                        countColumnFamily, "timestamp", "IncrementAmount"));
        logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
        
        //cassandra batching bolt to persist the status codes
        statusPersistenceBolt = new CassandraBatchingBolt(
            configKey, new DefaultTupleMapper(cassandraKeyspace, statusColumnFamily, "statusCode"));
        statusPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
        
        // casssandra bathing bolt to persist country counts
        countryStatsPersistenceBolt = new CassandraBatchingBolt(
            configKey, new DefaultTupleMapper(cassandraKeyspace, countryColumnFamily, "country"));
        countryStatsPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        builder.setSpout(Component.SPOUT, new KafkaSpout(kafkaConfig), 2);
        
        builder.setBolt(Component.PARSER, new LogParserBolt(), 2)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.VOLUME_COUNT, new VolumeCountBolt(), 2)
               .shuffleGrouping(Component.PARSER);
        
        builder.setBolt(Component.COUNT_PERSIST, logPersistenceBolt, 2)
               .shuffleGrouping(Component.VOLUME_COUNT);
        
        builder.setBolt(Component.IP_STAT_PARSER, new LogEventParserBolt(), 2)
               .shuffleGrouping(Component.PARSER);
        
        builder.setBolt(Component.STAT_COUNT, new StatusCountBolt(), 3)
               .fieldsGrouping(Component.IP_STAT_PARSER, new Fields(Field.LOG_STATUS_CODE));
        
        builder.setBolt(Component.STAT_COUNT_PERSIST, statusPersistenceBolt, 3)
               .shuffleGrouping(Component.STAT_COUNT);
        
        builder.setBolt(Component.GEO_FINDER, new GeographyBolt(new HttpIPResolver()), 3)
               .shuffleGrouping(Component.IP_STAT_PARSER);
        
        builder.setBolt(Component.COUNTRY_STATS, new GeoStatsBolt(), 3)
                .fieldsGrouping(Component.GEO_FINDER, new Fields(Field.COUNTRY));
        
        builder.setBolt(Component.COUNTRY_STATS_PERSIST, countryStatsPersistenceBolt, 3)
               .shuffleGrouping(Component.COUNTRY_STATS);
        
        builder.setBolt(Component.PRINTER, new PrinterBolt(), 1)
               .shuffleGrouping(Component.COUNTRY_STATS);
        
        return builder.createTopology();
    }
}
