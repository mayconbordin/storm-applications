package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

/**
 *
 * @author mayconbordin
 */
public class LinearRoadTopology extends AbstractTopology {

    public LinearRoadTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void prepare() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StormTopology buildTopology() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
