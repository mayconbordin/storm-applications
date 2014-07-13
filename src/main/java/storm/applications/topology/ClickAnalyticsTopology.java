package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.ClickAnalyticsConstants.*;
import storm.applications.bolt.GeoStatsBolt;
import storm.applications.bolt.GeographyBolt;
import storm.applications.bolt.RepeatVisitBolt;
import storm.applications.bolt.VisitStatsBolt;
import storm.applications.util.ConfigUtility;

public class ClickAnalyticsTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalyticsTopology.class);
    
    private int repeatsThreads;
    private int geographyThreads;
    private int totalStatsThreads;
    private int geoStatsThreads;
    
    public ClickAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        repeatsThreads    = ConfigUtility.getInt(config, Conf.REPEATS_THREADS, 1);
        geographyThreads  = ConfigUtility.getInt(config, Conf.GEOGRAPHY_THREADS, 1);
        totalStatsThreads = ConfigUtility.getInt(config, Conf.TOTAL_STATS_THREADS, 1);
        geoStatsThreads   = ConfigUtility.getInt(config, Conf.GEO_STATS_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        // First layer of bolts
        builder.setBolt(Component.REPEATS, new RepeatVisitBolt(), repeatsThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.URL, Field.CLIENT_KEY));
        
        builder.setBolt(Component.GEOGRAPHY, new GeographyBolt(), geographyThreads)
               .shuffleGrouping(Component.SPOUT);

        // second layer of bolts, commutative in nature
        builder.setBolt(Component.TOTAL_STATS, new VisitStatsBolt(), totalStatsThreads)
               .globalGrouping(Component.REPEATS);
        
        builder.setBolt(Component.GEO_STATS, new GeoStatsBolt(), geoStatsThreads)
               .fieldsGrouping(Component.GEOGRAPHY, new Fields(Field.COUNTRY));

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
