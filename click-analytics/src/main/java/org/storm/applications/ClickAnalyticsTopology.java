package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static org.storm.applications.ClickAnalyticsConstants.*;
import org.storm.applications.bolt.GeoStatsBolt;
import org.storm.applications.bolt.GeographyBolt;
import org.storm.applications.bolt.RepeatVisitBolt;
import org.storm.applications.bolt.VisitStatsBolt;
import org.storm.applications.spout.RedisClickSpout;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ip.HttpIPResolver;

public class ClickAnalyticsTopology extends AbstractTopology {

    public ClickAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void prepare() {
        
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        builder.setSpout(CLICK_SPOUT, new RedisClickSpout(), 10);

        // First layer of bolts
        builder.setBolt(REPEATS_BOLT, new RepeatVisitBolt(), 10)
               .fieldsGrouping(CLICK_SPOUT, new Fields(URL_FIELD, CLIENT_KEY_FIELD));
        
        builder.setBolt(GEOGRAPHY_BOLT, new GeographyBolt(new HttpIPResolver()), 10)
               .shuffleGrouping(CLICK_SPOUT);

        // second layer of bolts, commutative in nature
        builder.setBolt(TOTAL_STATS, new VisitStatsBolt(), 1)
               .globalGrouping(REPEATS_BOLT);
        
        builder.setBolt(GEO_STATS, new GeoStatsBolt(), 10)
               .fieldsGrouping(GEOGRAPHY_BOLT, new Fields(COUNTRY_FIELD));

        return builder.createTopology();
    }
}
