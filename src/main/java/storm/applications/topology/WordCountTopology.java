package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.SplitSentenceBolt;
import storm.applications.bolt.WordCountBolt;
import static storm.applications.constants.WordCountConstants.*;

public class WordCountTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    private int splitSentenceThreads;
    private int wordCountThreads;

    public WordCountTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        splitSentenceThreads = config.getInt(Conf.SPLITTER_THREADS, 1);
        wordCountThreads     = config.getInt(Conf.COUNTER_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.TEXT));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.COUNTER, new WordCountBolt(), wordCountThreads)
               .fieldsGrouping(Component.SPLITTER, new Fields(Field.WORD));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.COUNTER);
        
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
