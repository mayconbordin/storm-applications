package storm.applications.constants;

public interface TrendingTopicsConstants {
    interface Component {
        String SPOUT = "spout";
        String COUNTER = "counter";
        String INTERMEDIATE_RANKER = "intermediateRanker";
        String TOTAL_RANKER = "totalRanker";
        String SINK = "sink";
    }
    
    public static final class Conf {
        public static final String KAFKA_HOST = "kafka.zookeeper.host";
        public static final String KAFKA_TOPIC_TWEETS = "kafka.topic.tweets";
        public static final String KAFKA_ZOOKEEPER_PATH = "kafka.zookeeper.path";
        public static final String KAFKA_COMSUMER_ID = "kafka.consumer.id";
        
        public static final String SPOUT_THREADS = "tt.spout.threads";
        public static final String COUNTER_THREADS = "tt.counter.threads";
        public static final String IRANKER_THREADS = "tt.iranker.threads";
        public static final String TRANKER_THREADS = "tt.tranker.threads";
        public static final String SINK_THREADS = "tt.sink.threads";
        
        public static final String CASSANDRA_CONFIG = "cassandra-config";
        public static final String CASSANDRA_HOST = "cassandra.host";
        public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
        public static final String CASSANDRA_WORDCOUNT_CF = "wc.cassandra.cf";
        public static final String CASSANDRA_WORDCOUNT_KEY = "wc.cassandra.key";
    }
    
    interface Field {
        String WORD = "word";
        String OBJ  = "obj";
    }
}
