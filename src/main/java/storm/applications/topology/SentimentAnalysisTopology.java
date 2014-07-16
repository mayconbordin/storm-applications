package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.CalculateSentimentBolt;
import static storm.applications.constants.SentimentAnalysisConstants.*;

/**
 * Orchestrates the elements and forms a Topology to find the most happiest state
 * by analyzing and processing Tweets.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class SentimentAnalysisTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisTopology.class);
    
    private int classifierThreads;
    
    public SentimentAnalysisTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        classifierThreads = config.getInt(Conf.CLASSIFIER_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.TWEET));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.CLASSIFIER, new CalculateSentimentBolt(), classifierThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.CLASSIFIER);
        
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
