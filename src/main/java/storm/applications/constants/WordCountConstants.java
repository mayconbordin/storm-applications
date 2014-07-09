package storm.applications.constants;

public class WordCountConstants {
    public static final class Field {
        public static final String WORD = "word";
        public static final String COUNT = "count";
    }
    
    public static final class Conf {
        public static final String KAFKA_HOST = "kafka.zookeeper.host";
        public static final String KAFKA_TOPIC_SENTENCES = "kafka.topic.sentences";
        public static final String KAFKA_ZOOKEEPER_PATH = "kafka.zookeeper.path";
        public static final String KAFKA_COMSUMER_ID = "kafka.consumer.id";
        public static final String SPOUT_THREADS = "wc.spout.threads";
        public static final String SPLIT_SENTENCE_THREADS = "wc.splitsentence.threads";
        public static final String WORD_COUNT_THREADS = "wc.wordcount.threads";
        public static final String SINK_THREADS = "wc.sink.threads";
        
        public static final String CASSANDRA_CONFIG = "cassandra-config";
        public static final String CASSANDRA_HOST = "cassandra.host";
        public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
        public static final String CASSANDRA_WORDCOUNT_CF = "wc.cassandra.cf";
        public static final String CASSANDRA_WORDCOUNT_KEY = "wc.cassandra.key";
    }
    
    public static final class Component {
        public static final String SPOUT = "spout";
        public static final String SPLIT_SENTENCE = "splitSentence";
        public static final String WORD_COUNT = "wordCount";
        public static final String SINK = "sink";
    }
}
