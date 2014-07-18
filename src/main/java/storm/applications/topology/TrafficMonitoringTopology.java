package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.MapMatchingBolt;
import storm.applications.bolt.SpeedCalculatorBolt;
import static storm.applications.constants.TrafficMonitoringConstants.*;

/**
 *
 * https://github.com/whughchen/RealTimeTraffic
 * @author Chen Guanghua <whughchen@gmail.com>
 */
public class TrafficMonitoringTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoringTopology.class);
    
    private int mapMatcherThreads;
    private int speedCalcThreads;

    public TrafficMonitoringTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        mapMatcherThreads = config.getInt(Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads  = config.getInt(Conf.SPEED_CALCULATOR_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.MAP_MATCHER, new MapMatchingBolt(), mapMatcherThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.SPEED_CALCULATOR, new SpeedCalculatorBolt(), speedCalcThreads)
               .fieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.SPEED_CALCULATOR);
        
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
