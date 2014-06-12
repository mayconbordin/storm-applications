
package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.storm.applications.LogProcessingConstants.Component;
import org.storm.applications.LogProcessingConstants.Field;
import org.storm.applications.bolt.GeoBolt;
import org.storm.applications.bolt.GeoStatsBolt;
import org.storm.applications.bolt.LogEventParserBolt;
import org.storm.applications.bolt.ParseBolt;
import org.storm.applications.bolt.PrinterBolt;
import org.storm.applications.bolt.StatusCountBolt;
import org.storm.applications.bolt.VolumeCountBolt;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ip.HttpIPResolver;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessingTopology extends AbstractTopology {

    public LogProcessingTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void prepare() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        /*
        builder.setSpout(Component.SPOUT, kafkaSpout, 2);
        
        builder.setBolt(Component.PARSER, new ParseBolt(), 2)
               .shuffleGrouping(Component.SPOUT);
        
        builder.setBolt(Component.VOLUME_COUNT, new VolumeCountBolt(), 2)
               .shuffleGrouping(Component.PARSER);
        
        builder.setBolt(Component.COUNT_PERSIST, logPersistenceBolt, 2)
               .shuffleGrouping(Component.VOLUME_COUNT);
        
        builder.setBolt(Component.IP_STAT_PARSER, new LogEventParserBolt(), 2)
               .shuffleGrouping(Component.PARSER);
        
        builder.setBolt(Component.STAT_COUNT, new StatusCountBolt(), 3)
               .fieldsGrouping(Component.IP_STAT_PARSER, new Fields(Field.LOG_STATUS_CODE));
        
        builder.setBolt(Component.STAT_COUNT_PERSIST, statusPersistenceBolt, 3)
               .shuffleGrouping(Component.STAT_COUNT);
        
        builder.setBolt(Component.GEO_FINDER, new GeoBolt(new HttpIPResolver()), 3)
               .shuffleGrouping(Component.IP_STAT_PARSER);
        
        builder.setBolt(Component.COUNTRY_STATS, new GeoStatsBolt(), 3)
                .fieldsGrouping(Component.GEO_FINDER, new Fields(Field.COUNTRY));
        
        builder.setBolt(Component.COUNTRY_STATS_PERSIST, countryStatsPersistenceBolt, 3)
               .shuffleGrouping(Component.COUNTRY_STATS);
        
        builder.setBolt(Component.PRINTER, new PrinterBolt(), 1)
               .shuffleGrouping(Component.COUNTRY_STATS);
        */
        return builder.createTopology();
    }
}
