package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import storm.applications.bolt.MovingAverageBolt;
import storm.applications.bolt.SpikeDetectionBolt;
import storm.applications.spout.SensorRandomSpout;
/**
 * Detects spikes in values emitted from sensors.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionTopology extends AbstractTopology {

    public SpikeDetectionTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public StormTopology buildTopology() {
        builder.setSpout("string", new SensorRandomSpout(), 2);
        
        builder.setBolt("movingAverage", new MovingAverageBolt(10), 2)
               .shuffleGrouping("string");
        
        builder.setBolt("spikes", new SpikeDetectionBolt(0.10f), 2)
               .shuffleGrouping("movingAverage");
        
        return builder.createTopology();
    }

    @Override
    public void prepare() {
    }
}
