package org.storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopology.class);
    
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