package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.BargainIndexConstants.*;
import storm.applications.bolt.BargainIndexBolt;
import storm.applications.bolt.VwapBolt;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BargainIndexTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BargainIndexBolt.class);
    
    private int vwapThreads;
    private int bargainIndexThreads;
    
    public BargainIndexTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        vwapThreads         = config.getInt(Conf.VWAP_THREADS, 1);
        bargainIndexThreads = config.getInt(Conf.BARGAIN_INDEX_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(Stream.TRADES, new Fields(Field.STOCK, Field.PRICE, Field.VOLUME, Field.DATE, Field.INTERVAL));
        spout.setFields(Stream.QUOTES, new Fields(Field.STOCK, Field.PRICE, Field.VOLUME, Field.DATE, Field.INTERVAL));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.VWAP , new VwapBolt(), vwapThreads)
               .fieldsGrouping(Component.SPOUT, Stream.TRADES, new Fields(Field.STOCK));
        
        builder.setBolt(Component.BARGAIN_INDEX, new BargainIndexBolt(), bargainIndexThreads)
               .fieldsGrouping(Component.VWAP, new Fields(Field.STOCK))
               .fieldsGrouping(Component.SPOUT, Stream.QUOTES, new Fields(Field.STOCK));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .fieldsGrouping(Component.BARGAIN_INDEX, new Fields(Field.STOCK));
        
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
