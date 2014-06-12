package org.storm.applications;

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
import org.storm.applications.WordCountConstants.Component;
import org.storm.applications.WordCountConstants.Conf;
import org.storm.applications.WordCountConstants.Field;
import org.storm.applications.bolt.SplitSentenceBolt;
import org.storm.applications.bolt.WordCountBolt;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ConfigUtility;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class WordCountTopology extends AbstractTopology {
    private String kafkaTopicSentences;
    private String kafkaZookeeperPath;
    private String kafkaConsumerId;
    private String cassandraKeyspace;
    private String wordCountCf;
    private String wordCountKey;
    private BrokerHosts brokerHosts;
    private int spoutThreads;
    private int splitSentenceThreads;
    private int wordCountThreads;
    private int sinkThreads;

    public WordCountTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void prepare() {
        String kafkaHost = ConfigUtility.getString(config, Conf.KAFKA_HOST);
        brokerHosts = new ZkHosts(kafkaHost);
        
        kafkaTopicSentences  = ConfigUtility.getString(config, Conf.KAFKA_TOPIC_SENTENCES);
        kafkaZookeeperPath   = ConfigUtility.getString(config, Conf.KAFKA_ZOOKEEPER_PATH);
        kafkaConsumerId      = ConfigUtility.getString(config, Conf.KAFKA_COMSUMER_ID);
        
        spoutThreads         = ConfigUtility.getInt(config, Conf.SPOUT_THREADS);
        splitSentenceThreads = ConfigUtility.getInt(config, Conf.SPLIT_SENTENCE_THREADS);
        wordCountThreads     = ConfigUtility.getInt(config, Conf.WORD_COUNT_THREADS);
        sinkThreads          = ConfigUtility.getInt(config, Conf.SINK_THREADS);
        
        cassandraKeyspace = ConfigUtility.getString(config, Conf.CASSANDRA_KEYSPACE);
        wordCountCf = ConfigUtility.getString(config, Conf.CASSANDRA_WORDCOUNT_CF);
        wordCountKey = ConfigUtility.getString(config, Conf.CASSANDRA_WORDCOUNT_KEY);
        
        Map<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, ConfigUtility.getString(config, Conf.CASSANDRA_HOST));
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] { cassandraKeyspace }));
        config.put(Conf.CASSANDRA_CONFIG, clientConfig);
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        // Spout
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, kafkaTopicSentences, 
                kafkaZookeeperPath, kafkaConsumerId);
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        // Sink
        CassandraBatchingBolt<String, String, String> cassandraSink = new CassandraBatchingBolt<String, String, String>(Conf.CASSANDRA_CONFIG,
                new DefaultTupleMapper(cassandraKeyspace, wordCountCf, wordCountKey));
        cassandraSink.setAckStrategy(AckStrategy.ACK_ON_WRITE);
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.SPLIT_SENTENCE, new SplitSentenceBolt(), splitSentenceThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.WORD_COUNT, new WordCountBolt(), wordCountThreads)
               .fieldsGrouping(Component.SPLIT_SENTENCE, new Fields(Field.WORD));
        
        builder.setBolt(Component.SINK, cassandraSink, sinkThreads)
               .shuffleGrouping(Component.WORD_COUNT);
        
        return builder.createTopology();
    }
    
}
