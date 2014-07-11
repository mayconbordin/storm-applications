package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;

public abstract class AbstractTopology {
    protected TopologyBuilder builder;
    protected String topologyName;
    protected Config config;

    public AbstractTopology(String topologyName, Config config) {
        this.topologyName = topologyName;
        this.config = config;
        this.builder = new TopologyBuilder();
    }

    public String getTopologyName() {
        return topologyName;
    }

    public abstract void initialize();
    public abstract StormTopology buildTopology();
    public abstract Logger getLogger();
}