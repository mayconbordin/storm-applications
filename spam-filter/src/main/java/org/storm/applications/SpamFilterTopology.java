package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.storm.applications.SpamFilterConstants.Component;
import org.storm.applications.SpamFilterConstants.Conf;
import org.storm.applications.SpamFilterConstants.Field;
import org.storm.applications.SpamFilterConstants.Stream;
import org.storm.applications.bolt.BayesRuleBolt;
import org.storm.applications.bolt.ParserBolt;
import org.storm.applications.bolt.WordProbabilityBolt;
import org.storm.applications.bolt.TokenizerBolt;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ConfigUtility;
import storm.kafka.*;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SpamFilterTopology extends AbstractTopology {
    private String kafkaTopicTraining;
    private String kafkaTopicAnalysis;
    private String kafkaZookeeperPath;
    private String kafkaConsumerId;
    private BrokerHosts brokerHosts;
    private int trainingSpoutThreads;
    private int analysisSpoutThreads;
    private int parserThreads;
    private int tokenizerThreads;
    private int wordProbThreads;
    private int bayesRuleThreads;
    
    public SpamFilterTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void prepare() {
        String kafkaHost = ConfigUtility.getString(config, Conf.KAFKA_HOST);
        brokerHosts = new ZkHosts(kafkaHost);
        
        kafkaTopicTraining = ConfigUtility.getString(config, Conf.KAFKA_TOPIC_TRAINING);
        kafkaTopicAnalysis = ConfigUtility.getString(config, Conf.KAFKA_TOPIC_ANALYSIS);
        kafkaZookeeperPath = ConfigUtility.getString(config, Conf.KAFKA_ZOOKEEPER_PATH);
        kafkaConsumerId    = ConfigUtility.getString(config, Conf.KAFKA_COMSUMER_ID);
        
        trainingSpoutThreads = ConfigUtility.getInt(config, Conf.TRAINING_SPOUT_THREADS);
        analysisSpoutThreads = ConfigUtility.getInt(config, Conf.ANALYSIS_SPOUT_THREADS);
        parserThreads        = ConfigUtility.getInt(config, Conf.PARSER_THREADS);
        tokenizerThreads     = ConfigUtility.getInt(config, Conf.TOKENIZER_THREADS);
        wordProbThreads      = ConfigUtility.getInt(config, Conf.WORD_PROB_THREADS);
        bayesRuleThreads     = ConfigUtility.getInt(config, Conf.BAYES_RULE_THREADS);
    }
    
    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        // Training Spout
        SpoutConfig trainingConf = new SpoutConfig(brokerHosts, kafkaTopicTraining, 
                kafkaZookeeperPath, kafkaConsumerId);
        KafkaSpout trainingSpout = new KafkaSpout(trainingConf);
        trainingConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        // Analysis Spout
        SpoutConfig analysisConf = new SpoutConfig(brokerHosts, kafkaTopicAnalysis, 
                kafkaZookeeperPath, kafkaConsumerId);
        KafkaSpout analysisSpout = new KafkaSpout(analysisConf);
        analysisConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        builder.setSpout(Component.TRAINING_SPOUT, trainingSpout, trainingSpoutThreads);
        builder.setSpout(Component.ANALYSIS_SPOUT, analysisSpout, analysisSpoutThreads);
        
        builder.setBolt(Component.PARSER, new ParserBolt(), parserThreads)
               .shuffleGrouping(Component.TRAINING_SPOUT)
               .shuffleGrouping(Component.ANALYSIS_SPOUT);
        
        builder.setBolt(Component.TOKENIZER, new TokenizerBolt(), tokenizerThreads)
               .shuffleGrouping(Component.PARSER, Stream.TRAINING)
               .shuffleGrouping(Component.PARSER, Stream.ANALYSIS);
        
        builder.setBolt(Component.WORD_PROBABILITY, new WordProbabilityBolt(), wordProbThreads)
               .fieldsGrouping(Component.TOKENIZER, Stream.TRAINING, new Fields(Field.WORD))
               .fieldsGrouping(Component.TOKENIZER, Stream.ANALYSIS, new Fields(Field.WORD))
               .allGrouping(Component.TOKENIZER, Stream.TRAINING_SUM);
        
        builder.setBolt(Component.BAYES_RULE, new BayesRuleBolt(), bayesRuleThreads)
               .fieldsGrouping(Component.WORD_PROBABILITY, Stream.ANALYSIS, new Fields(Field.ID));
        
        return builder.createTopology();
    }
}
