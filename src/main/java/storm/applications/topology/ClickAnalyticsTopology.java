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
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;

public class ClickAnalyticsTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalyticsTopology.class);
    
    private int repeatsThreads;
    private int geographyThreads;
    private int totalStatsThreads;
    private int geoStatsThreads;
    private int spoutThreads;
    private int visitSinkThreads;
    private int locationSinkThreads;
    
    private AbstractSpout spout;
    private BaseSink visitSink;
    private BaseSink locationSink;
    
    public ClickAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        repeatsThreads       = config.getInt(Conf.REPEATS_THREADS, 1);
        geographyThreads     = config.getInt(Conf.GEOGRAPHY_THREADS, 1);
        totalStatsThreads    = config.getInt(Conf.TOTAL_STATS_THREADS, 1);
        geoStatsThreads      = config.getInt(Conf.GEO_STATS_THREADS, 1);
        spoutThreads         = config.getInt(BaseConf.SPOUT_THREADS, 1);
        visitSinkThreads     = config.getInt(getConfigKey(BaseConf.SINK_THREADS, "visit"), 1);
        locationSinkThreads  = config.getInt(getConfigKey(BaseConf.SINK_THREADS, "location"), 1);
        
        spout        = loadSpout();
        visitSink    = loadSink("visit");
        locationSink = loadSink("location");
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.IP, Field.URL, Field.CLIENT_KEY));
        
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
        
        // sinks
        builder.setBolt(Component.SINK_VISIT, visitSink, visitSinkThreads)
               .shuffleGrouping(Component.TOTAL_STATS);
        
        builder.setBolt(Component.SINK_LOCATION, locationSink, locationSinkThreads)
               .fieldsGrouping(Component.GEO_STATS, new Fields(Field.COUNTRY));

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
