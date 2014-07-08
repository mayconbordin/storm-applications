package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.storm.applications.bolt.IntermediateRankingsBolt;
import org.storm.applications.bolt.RollingCountBolt;
import org.storm.applications.bolt.TotalRankingsBolt;
import org.storm.applications.topology.AbstractTopology;

public class TrendingTopicsTopology extends AbstractTopology {

    public TrendingTopicsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void prepare() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        builder.setSpout(Component.SPOUT, kafkaSpout);
        
        builder.setBolt(Component.COUNTER, new RollingCountBolt(windowLengthInSeconds, emitFrequencyInSeconds), counterThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.WORD));
        
        builder.setBolt(Component.INTERMEDIATE_RANKER, new IntermediateRankingsBolt(topN), intermediateRankerThreads)
               .fieldsGrouping(Component.COUNTER, new Fields(Field.OBJ));
        
        builder.setBolt(Component.TOTAL_RANKER, new TotalRankingsBolt(topN), totalRankerThreads)
               .globalGrouping(Component.INTERMEDIATE_RANKER);
        
        builder.setBolt(Component.SINK, cassandraSink, sinkThreads)
               .shuffleGrouping(Component.TOTAL_RANKER);
        
        return builder.createTopology();
    }
    
}
