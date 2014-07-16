
package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.GeoStatsBolt;
import storm.applications.bolt.GeographyBolt;
import storm.applications.bolt.StatusCountBolt;
import storm.applications.bolt.VolumeCountBolt;
import static storm.applications.constants.LogProcessingConstants.*;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessingTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LogProcessingTopology.class);
    
    private AbstractSpout spout;
    private BaseSink countSink;
    private BaseSink statusSink;
    private BaseSink countrySink;
    
    private int spoutThreads;
    private int countSinkThreads;
    private int statusSinkThreads;
    private int countrySinkThreads;
    private int volumeCountThreads;
    private int statusCountThreads;
    private int geoFinderThreads;
    private int geoStatsThreads;
    
    public LogProcessingTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        spout       = loadSpout();
        countSink   = loadSink("count");
        statusSink  = loadSink("status");
        countrySink = loadSink("country");
        
        spoutThreads       = config.getInt(getConfigKey(Conf.SPOUT_THREADS), 1);
        countSinkThreads   = config.getInt(getConfigKey(Conf.SINK_THREADS, "count"), 1);
        statusSinkThreads  = config.getInt(getConfigKey(Conf.SINK_THREADS, "status"), 1);
        countrySinkThreads = config.getInt(getConfigKey(Conf.SINK_THREADS, "country"), 1);
        
        volumeCountThreads = config.getInt(Conf.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInt(Conf.STATUS_COUNTER_THREADS, 1);
        geoFinderThreads   = config.getInt(Conf.GEO_FINDER_THREADS, 1);
        geoStatsThreads    = config.getInt(Conf.GEO_STATS_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.IP, Field.TIMESTAMP, Field.TIMESTAMP_MINUTES,
                Field.REQUEST, Field.RESPONSE, Field.BYTE_SIZE));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.VOLUME_COUNTER, new VolumeCountBolt(), volumeCountThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.TIMESTAMP_MINUTES));
        
        builder.setBolt(Component.STATUS_COUNTER, new StatusCountBolt(), statusCountThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.RESPONSE));
        
        builder.setBolt(Component.GEO_FINDER, new GeographyBolt(), geoFinderThreads)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.GEO_STATS, new GeoStatsBolt(), geoStatsThreads)
                .fieldsGrouping(Component.GEO_FINDER, new Fields(Field.COUNTRY));
        
        
        builder.setBolt(Component.VOLUME_SINK, countSink, countSinkThreads)
               .shuffleGrouping(Component.VOLUME_COUNTER);

        builder.setBolt(Component.STATUS_SINK, statusSink, statusSinkThreads)
               .shuffleGrouping(Component.STATUS_COUNTER);
        
        builder.setBolt(Component.GEO_SINK, countrySink, countrySinkThreads)
               .shuffleGrouping(Component.GEO_STATS);

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
