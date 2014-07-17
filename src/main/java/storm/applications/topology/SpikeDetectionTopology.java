package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.MovingAverageBolt;
import storm.applications.bolt.SpikeDetectionBolt;
import static storm.applications.constants.SpikeDetectionConstants.*;
/**
 * Detects spikes in values emitted from sensors.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionTopology.class);
    
    private int movingAverageThreads;
    private int spikeDetectorThreads;

    public SpikeDetectionTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        movingAverageThreads = config.getInt(Conf.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInt(Conf.SPIKE_DETECTOR_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.DEVICE_ID, Field.TIMESTAMP, Field.VALUE));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt(), movingAverageThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.DEVICE_ID));
        
        builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt(), spikeDetectorThreads)
               .shuffleGrouping(Component.MOVING_AVERAGE);
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.SPIKE_DETECTOR);
        
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
