package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.GlobalMedianCalculatorBolt;
import storm.applications.bolt.HouseLoadPredictorBolt;
import storm.applications.bolt.OutlierDetectionBolt;
import storm.applications.bolt.PlugLoadPredictorBolt;
import storm.applications.bolt.PlugMedianCalculatorBolt;
import storm.applications.bolt.SmartGridSlidingWindowBolt;
import static storm.applications.constants.SmartGridConstants.*;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;

/**
 *
 * @author mayconbordin
 */
public class SmartGridTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SmartGridTopology.class);
    
    private AbstractSpout spout;
    private BaseSink outlierSink;
    private BaseSink predictionSink;
    
    private int spoutThreads;
    private int outlierSinkThreads;
    private int predictionSinkThreads;
    private int slidingWindowThreads;
    private int globalMedianThreads;
    private int plugMedianThreads;
    private int outlierDetectorThreads;
    private int houseLoadThreads;
    private int plugLoadThreads;
    
    private int houseLoadFrequency;
    private int plugLoadFrequency;

    public SmartGridTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        spout          = loadSpout();
        outlierSink    = loadSink("outlier");
        predictionSink = loadSink("prediction");
        
        spoutThreads           = config.getInt(getConfigKey(Conf.SPOUT_THREADS), 1);
        outlierSinkThreads     = config.getInt(getConfigKey(Conf.SINK_THREADS, "outlier"), 1);
        predictionSinkThreads  = config.getInt(getConfigKey(Conf.SINK_THREADS, "prediction"), 1);
        
        slidingWindowThreads   = config.getInt(Conf.SLIDING_WINDOW_THREADS, 1);
        globalMedianThreads    = config.getInt(Conf.SLIDING_WINDOW_THREADS, 1);
        plugMedianThreads      = config.getInt(Conf.SLIDING_WINDOW_THREADS, 1);
        outlierDetectorThreads = config.getInt(Conf.SLIDING_WINDOW_THREADS, 1);
        houseLoadThreads       = config.getInt(Conf.SLIDING_WINDOW_THREADS, 1);
        plugLoadThreads        = config.getInt(Conf.SLIDING_WINDOW_THREADS, 1);
        
        houseLoadFrequency     = config.getInt(Conf.HOUSE_LOAD_FREQUENCY, 15);
        plugLoadFrequency      = config.getInt(Conf.PLUG_LOAD_FREQUENCY, 15);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.ID, Field.TIMESTAMP, Field.VALUE, Field.PROPERTY,
                                   Field.PLUG_ID, Field.HOUSEHOLD_ID, Field.HOUSE_ID));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.SLIDING_WINDOW, new SmartGridSlidingWindowBolt(), slidingWindowThreads)
               .globalGrouping(Component.SPOUT);
        
        // Outlier detection
        builder.setBolt(Component.GLOBAL_MEDIAN, new GlobalMedianCalculatorBolt(), globalMedianThreads)
               .globalGrouping(Component.SLIDING_WINDOW);
        
        builder.setBolt(Component.PLUG_MEDIAN, new PlugMedianCalculatorBolt(), plugMedianThreads)
               .fieldsGrouping(Component.SLIDING_WINDOW, new Fields(Field.HOUSE_ID, Field.HOUSEHOLD_ID, Field.PLUG_ID));
        
        builder.setBolt(Component.OUTLIER_DETECTOR, new OutlierDetectionBolt(), outlierDetectorThreads)
               .allGrouping(Component.GLOBAL_MEDIAN)
               .fieldsGrouping(Component.PLUG_MEDIAN, new Fields(Field.PLUG_SPECIFIC_KEY));
        
        // Load prediction
        builder.setBolt(Component.HOUSE_LOAD, new HouseLoadPredictorBolt(houseLoadFrequency), houseLoadThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.HOUSE_ID));
        
        builder.setBolt(Component.PLUG_LOAD, new PlugLoadPredictorBolt(plugLoadFrequency), plugLoadThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.HOUSE_ID));
        
        // Sinks
        builder.setBolt(Component.OUTLIER_SINK, outlierSink, outlierSinkThreads)
               .shuffleGrouping(Component.OUTLIER_DETECTOR);
        
        builder.setBolt(Component.PREDICTION_SINK, predictionSink, predictionSinkThreads)
               .fieldsGrouping(Component.HOUSE_LOAD, new Fields(Field.HOUSE_ID))
               .fieldsGrouping(Component.PLUG_LOAD, new Fields(Field.HOUSE_ID));
        
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
