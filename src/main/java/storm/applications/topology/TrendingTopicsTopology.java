package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import storm.applications.bolt.IntermediateRankingsBolt;
import storm.applications.bolt.RollingCountBolt;
import storm.applications.bolt.TotalRankingsBolt;
import storm.applications.constants.TrendingTopicsConstants.Component;
import storm.applications.constants.TrendingTopicsConstants.Conf;
import storm.applications.constants.TrendingTopicsConstants.Field;
import storm.applications.constants.WordCountConstants;
import storm.applications.util.ConfigUtility;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class TrendingTopicsTopology extends AbstractTopology {
    private String kafkaTopicTweets;
    private String kafkaZookeeperPath;
    private String kafkaConsumerId;
    private String cassandraKeyspace;
    private String wordCountCf;
    private String wordCountKey;
    private BrokerHosts brokerHosts;
    private int spoutThreads;
    private int counterThreads;
    private int iRankerThreads;
    private int tRankerThreads;
    private int sinkThreads;
    private int topN = 10;
    
    public TrendingTopicsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void prepare() {
        String kafkaHost = ConfigUtility.getString(config, Conf.KAFKA_HOST);
        brokerHosts = new ZkHosts(kafkaHost);
        
        kafkaTopicTweets  = ConfigUtility.getString(config, Conf.KAFKA_TOPIC_TWEETS);
        kafkaZookeeperPath   = ConfigUtility.getString(config, Conf.KAFKA_ZOOKEEPER_PATH);
        kafkaConsumerId      = ConfigUtility.getString(config, Conf.KAFKA_COMSUMER_ID);
        
        spoutThreads         = ConfigUtility.getInt(config, Conf.SPOUT_THREADS);
        counterThreads       = ConfigUtility.getInt(config, Conf.COUNTER_THREADS);
        iRankerThreads       = ConfigUtility.getInt(config, Conf.IRANKER_THREADS);
        tRankerThreads       = ConfigUtility.getInt(config, Conf.IRANKER_THREADS);
        sinkThreads          = ConfigUtility.getInt(config, Conf.SINK_THREADS);
        
        cassandraKeyspace = ConfigUtility.getString(config, WordCountConstants.Conf.CASSANDRA_KEYSPACE);
        wordCountCf = ConfigUtility.getString(config, WordCountConstants.Conf.CASSANDRA_WORDCOUNT_CF);
        wordCountKey = ConfigUtility.getString(config, WordCountConstants.Conf.CASSANDRA_WORDCOUNT_KEY);
        
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, ConfigUtility.getString(config, Conf.CASSANDRA_HOST));
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] { cassandraKeyspace }));
        config.put(Conf.CASSANDRA_CONFIG, clientConfig);
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        // Spout
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, kafkaTopicTweets, 
                kafkaZookeeperPath, kafkaConsumerId);
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        // Sink
        CassandraBatchingBolt<String, String, String> cassandraSink = new CassandraBatchingBolt<String, String, String>(WordCountConstants.Conf.CASSANDRA_CONFIG,
                new DefaultTupleMapper(cassandraKeyspace, wordCountCf, wordCountKey));
        cassandraSink.setAckStrategy(AckStrategy.ACK_ON_WRITE);
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.COUNTER, new RollingCountBolt(), counterThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.WORD));
        
        builder.setBolt(Component.INTERMEDIATE_RANKER, new IntermediateRankingsBolt(topN), iRankerThreads)
               .fieldsGrouping(Component.COUNTER, new Fields(Field.OBJ));
        
        builder.setBolt(Component.TOTAL_RANKER, new TotalRankingsBolt(topN), tRankerThreads)
               .globalGrouping(Component.INTERMEDIATE_RANKER);
        
        builder.setBolt(Component.SINK, cassandraSink, sinkThreads)
               .shuffleGrouping(Component.TOTAL_RANKER);
        
        return builder.createTopology();
    }
    
}
