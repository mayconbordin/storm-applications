package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public abstract class AbstractTopology {
    protected TopologyBuilder builder;
    protected String topologyName;
    protected Config config;

    public AbstractTopology(String topologyName, Config config) {
        this.topologyName = topologyName;
        this.config = config;
        
        initialize();
    }
    
    private void initialize() {
        this.builder = new TopologyBuilder();
        prepare();
    }

    public String getTopologyName() {
        return topologyName;
    }

    public abstract void prepare();
    public abstract StormTopology buildTopology();
}