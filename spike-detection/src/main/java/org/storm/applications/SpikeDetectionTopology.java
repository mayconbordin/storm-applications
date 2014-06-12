package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import org.storm.applications.bolt.MovingAverageBolt;
import org.storm.applications.bolt.SpikeDetectionBolt;
import org.storm.applications.spout.SensorRandomSpout;
import org.storm.applications.topology.AbstractTopology;
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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
