package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.applications.bolt.AggregatorCtrBolt;
import storm.applications.bolt.RollingCtrBolt;
import storm.applications.constants.AdsAnalyticsConstants;
import storm.applications.constants.AdsAnalyticsConstants.Component;
import storm.applications.constants.AdsAnalyticsConstants.Conf;
import storm.applications.constants.AdsAnalyticsConstants.Stream;
import storm.applications.sink.FileSink;
import storm.applications.spout.AdEventSpout;
import storm.applications.util.ConfigUtility;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopology extends AbstractTopology {
    private String sinkFile;
    private String spoutFile;

    public AdsAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void prepare() {
        sinkFile = ConfigUtility.getString(config, Conf.SINK_FILE);
        spoutFile = ConfigUtility.getString(config, Conf.SPOUT_FILE);
    }

    @Override
    public StormTopology buildTopology() {
        builder.setSpout(Component.SPOUT, new AdEventSpout(spoutFile));
        
        /*
        builder.setBolt(CTR_BOLT, new CtrBolt())
               .fieldsGrouping(AD_EVENT_SPOUT, AdEventSpout.CLICK_STREAM, new Fields("queryId", "adId"))
               .fieldsGrouping(AD_EVENT_SPOUT, AdEventSpout.IMPRESSION_STREAM, new Fields("queryId", "adId"));
        */
        
        builder.setBolt(Component.CTR_BOLT, new RollingCtrBolt(300, 60))
               .fieldsGrouping(Component.SPOUT, Stream.CLICKS, new Fields("queryId", "adId"))
               .fieldsGrouping(Component.SPOUT, Stream.IMPRESSIONS, new Fields("queryId", "adId"));
        
        //builder.setBolt(CTR_AGGREGATOR_BOLT, new AggregatorCtrBolt(600, 120))
        //       .fieldsGrouping(CTR_BOLT, new Fields("queryId", "adId"));
        
        builder.setBolt(Component.SINK, new FileSink(sinkFile), 1)
               .shuffleGrouping(Component.CTR_BOLT);
        
        
        // rolling ctr per minute - emit ctr, clicks and impressions
        // you can't do average of ctr, you need to sum clicks and impressions
        // another rolling by the hour or day
        
        return builder.createTopology();
    }
    
}
