package storm.applications.constants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public final class SpamFilterConstants {
    public static final class Conf {
        public static final String KAFKA_HOST = "kafka.zookeeper.host";
        public static final String KAFKA_TOPIC_TRAINING = "kafka.topic.training";
        public static final String KAFKA_TOPIC_ANALYSIS = "kafka.topic.analysis";
        public static final String KAFKA_ZOOKEEPER_PATH = "kafka.zookeeper.path";
        public static final String KAFKA_COMSUMER_ID = "kafka.consumer.id";
        public static final String TRAINING_SPOUT_THREADS = "sf.training.spout.threads";
        public static final String ANALYSIS_SPOUT_THREADS = "sf.analysis.spout.threads";
        public static final String PARSER_THREADS = "sf.parser.threads";
        public static final String TOKENIZER_THREADS = "sf.tokenizer.threads";
        public static final String WORD_PROB_THREADS = "sf.wordprob.threads";
        public static final String BAYES_RULE_THREADS = "sf.bayesrule.threads";
    }
    
    public static final class Field {
        public static final String ID = "id";
        public static final String TYPE = "type";
        public static final String MESSAGE = "message";
        public static final String IS_SPAM = "isSpam";
        public static final String WORD = "word";
        public static final String NUM_WORDS = "numWords";
        public static final String COUNT = "count";
        public static final String SPAM_TOTAL = "spamTotal";
        public static final String HAM_TOTAL = "hamTotal";
        public static final String SPAM_PROB = "spamProb";
    }
    
    public static final class Stream {
        public static final String TRAINING = "trainingStream";
        public static final String TRAINING_END = "trainingEndStream";
        public static final String TRAINING_SUM = "trainingSumStream";
        public static final String ANALYSIS = "analysisStream";
    }
    
    public static final class Component {
        public static final String TRAINING_SPOUT = "trainingSpout";
        public static final String ANALYSIS_SPOUT = "analysisSpout";
        public static final String PARSER = "parserBolt";
        public static final String TOKENIZER = "tokenizerBolt";
        public static final String WORD_PROBABILITY = "wordProbabilityBolt";
        public static final String BAYES_RULE = "bayesRuleBolt";
    }
}
