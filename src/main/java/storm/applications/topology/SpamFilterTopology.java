package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.BayesRuleBolt;
import storm.applications.bolt.TokenizerBolt;
import storm.applications.bolt.WordProbabilityBolt;
import static storm.applications.constants.SpamFilterConstants.*;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SpamFilterTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpamFilterTopology.class);
    
    private AbstractSpout trainingSpout;
    private AbstractSpout analysisSpout;
    private BaseSink sink;
    
    private int trainingSpoutThreads;
    private int analysisSpoutThreads;
    private int tokenizerThreads;
    private int wordProbThreads;
    private int bayesRuleThreads;
    private int sinkThreads;
    
    public SpamFilterTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        trainingSpout = loadSpout("training");
        analysisSpout = loadSpout("analysis");
        sink = loadSink();
        
        trainingSpoutThreads = config.getInt(getConfigKey(Conf.SPOUT_THREADS, "training"), 1);
        analysisSpoutThreads = config.getInt(getConfigKey(Conf.SPOUT_THREADS, "analysis"), 1);
        tokenizerThreads     = config.getInt(Conf.TOKENIZER_THREADS, 1);
        wordProbThreads      = config.getInt(Conf.WORD_PROB_THREADS, 1);
        bayesRuleThreads     = config.getInt(Conf.BAYES_RULE_THREADS, 1);
        sinkThreads          = config.getInt(getConfigKey(Conf.SINK_THREADS), 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        trainingSpout.setFields(new Fields(Field.ID, Field.MESSAGE, Field.IS_SPAM));
        analysisSpout.setFields(new Fields(Field.ID, Field.MESSAGE));
        
        builder.setSpout(Component.TRAINING_SPOUT, trainingSpout, trainingSpoutThreads);
        builder.setSpout(Component.ANALYSIS_SPOUT, analysisSpout, analysisSpoutThreads);

        builder.setBolt(Component.TOKENIZER, new TokenizerBolt(), tokenizerThreads)
               .shuffleGrouping(Component.TRAINING_SPOUT)
               .shuffleGrouping(Component.ANALYSIS_SPOUT);
        
        builder.setBolt(Component.WORD_PROBABILITY, new WordProbabilityBolt(), wordProbThreads)
               .fieldsGrouping(Component.TOKENIZER, Stream.TRAINING, new Fields(Field.WORD))
               .fieldsGrouping(Component.TOKENIZER, Stream.ANALYSIS, new Fields(Field.WORD))
               .allGrouping(Component.TOKENIZER, Stream.TRAINING_SUM);
        
        builder.setBolt(Component.BAYES_RULE, new BayesRuleBolt(), bayesRuleThreads)
               .fieldsGrouping(Component.WORD_PROBABILITY, new Fields(Field.ID));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.BAYES_RULE);
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
