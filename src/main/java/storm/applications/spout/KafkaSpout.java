package storm.applications.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.parser.Parser;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;
import storm.applications.util.StreamValues;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * This is an encapsulation of the storm-kafka-0.8-plus spout implementation.
 * It only emits tuples to the default stream, also only the first value of the parser
 * is going to be emitted, this is a limitation of the implementation, most applications
 * won't be affected.
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class KafkaSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    
    private BrokerHosts brokerHosts;
    private storm.kafka.KafkaSpout spout;
    
    private String hostKey       = BaseConf.KAFKA_HOST;
    private String pathKey       = BaseConf.KAFKA_ZOOKEEPER_PATH;
    private String consumerIdKey = BaseConf.KAFKA_COMSUMER_ID;
    private String topicKey      = BaseConf.KAFKA_SPOUT_TOPIC;
    private String parserKey     = BaseConf.SPOUT_PARSER;
    
    @Override
    protected void initialize() {
        String parserClass = ConfigUtility.getString(config, parserKey);
        String host        = ConfigUtility.getString(config, hostKey);
        String topic       = ConfigUtility.getString(config, topicKey);
        String consumerId  = ConfigUtility.getString(config, consumerIdKey);
        String path        = ConfigUtility.getString(config, pathKey);
        
        Parser parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        Fields defaultFields = fields.get(Utils.DEFAULT_STREAM_ID);
        if (defaultFields == null) {
            throw new RuntimeException("A KafkaSpout must have a default stream");
        }
        
        brokerHosts = new ZkHosts(host);
        
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, path, consumerId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new ParserScheme(parser, defaultFields));
        
        spout = new storm.kafka.KafkaSpout(spoutConfig);
        spout.open(config, context, collector);
    }

    @Override
    public void nextTuple() {
        spout.nextTuple();
    }

    @Override
    public void fail(Object msgId) {
        spout.fail(msgId);
    }

    @Override
    public void ack(Object msgId) {
        spout.ack(msgId);
    }

    @Override
    public void deactivate() {
        spout.deactivate();
    }

    @Override
    public void close() {
        spout.close();
    }
    
    private class ParserScheme extends StringScheme {
        private final Parser parser;
        private final Fields fields;

        public ParserScheme(Parser parser, Fields fields) {
            this.parser = parser;
            this.fields = fields;
        }
        
        @Override
        public List<Object> deserialize(byte[] bytes) {
            String value = deserializeString(bytes);
            
            List<StreamValues> tuples = parser.parse(value);

            if (tuples != null && tuples.size() > 0) {
                return tuples.get(0);
            }
            
            return null;
        }
        
        @Override
        public Fields getOutputFields() {
            return fields;
        }
    }

    public void setHostKey(String hostKey) {
        this.hostKey = hostKey;
    }

    public void setPathKey(String pathKey) {
        this.pathKey = pathKey;
    }

    public void setConsumerIdKey(String consumerIdKey) {
        this.consumerIdKey = consumerIdKey;
    }

    public void setTopicKey(String topicKey) {
        this.topicKey = topicKey;
    }

    public void setParserKey(String parserKey) {
        this.parserKey = parserKey;
    }
}
