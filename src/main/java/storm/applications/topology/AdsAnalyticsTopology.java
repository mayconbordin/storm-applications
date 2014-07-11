package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.RollingCtrBolt;
import storm.applications.constants.AdsAnalyticsConstants.Component;
import storm.applications.constants.AdsAnalyticsConstants.Conf;
import storm.applications.constants.AdsAnalyticsConstants.Field;
import storm.applications.constants.AdsAnalyticsConstants.Stream;
import storm.applications.util.ConfigUtility;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AdsAnalyticsTopology.class);
    
    private int ctrThreads;

    public AdsAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        ctrThreads = ConfigUtility.getInt(config, Conf.CTR_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.QUERY_ID, Field.AD_ID, Field.EVENT));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.CTR, new RollingCtrBolt(), ctrThreads)
               .fieldsGrouping(Component.SPOUT, Stream.CLICKS, new Fields(Field.QUERY_ID, Field.AD_ID))
               .fieldsGrouping(Component.SPOUT, Stream.IMPRESSIONS, new Fields(Field.QUERY_ID, Field.AD_ID));

        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.CTR);

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
