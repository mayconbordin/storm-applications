package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.storm.applications.bolt.AggregatorCtrBolt;
import org.storm.applications.bolt.RollingCtrBolt;
import org.storm.applications.sink.FileSink;
import org.storm.applications.spout.AdEventSpout;
import org.storm.applications.topology.AbstractTopology;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopology extends AbstractTopology {
    public static final String AD_EVENT_SPOUT = "AdEventSpout";
    public static final String CTR_BOLT = "CtrBolt";
    public static final String CTR_AGGREGATOR_BOLT = "AggregatorCtrBolt";
    public static final String FILE_SINK = "FileSink";

    public AdsAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    public void prepare() {
        
    }

    @Override
    public StormTopology buildTopology() {
        builder.setSpout(AD_EVENT_SPOUT, new AdEventSpout("/home/mayconbordin/Projects/datasets/ctr/track2/sample.txt"));
        
        /*
        builder.setBolt(CTR_BOLT, new CtrBolt())
               .fieldsGrouping(AD_EVENT_SPOUT, AdEventSpout.CLICK_STREAM, new Fields("queryId", "adId"))
               .fieldsGrouping(AD_EVENT_SPOUT, AdEventSpout.IMPRESSION_STREAM, new Fields("queryId", "adId"));
        */
        
        builder.setBolt(CTR_BOLT, new RollingCtrBolt(300, 60))
               .fieldsGrouping(AD_EVENT_SPOUT, AdEventSpout.CLICK_STREAM, new Fields("queryId", "adId"))
               .fieldsGrouping(AD_EVENT_SPOUT, AdEventSpout.IMPRESSION_STREAM, new Fields("queryId", "adId"));
        
        //builder.setBolt(CTR_AGGREGATOR_BOLT, new AggregatorCtrBolt(600, 120))
        //       .fieldsGrouping(CTR_BOLT, new Fields("queryId", "adId"));
        
        builder.setBolt(FILE_SINK, new FileSink("/home/mayconbordin/tmp/ctr.txt"), 1)
               .shuffleGrouping(CTR_BOLT);
        
        
        // rolling ctr per minute - emit ctr, clicks and impressions
        // you can't do average of ctr, you need to sum clicks and impressions
        // another rolling by the hour or day
        
        return builder.createTopology();
    }
    
}
