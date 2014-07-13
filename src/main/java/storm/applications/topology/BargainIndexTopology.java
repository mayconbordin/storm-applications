package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.BargainIndexConstants.*;
import storm.applications.bolt.BargainIndexBolt;
import storm.applications.bolt.VwapBolt;
import storm.applications.util.ConfigUtility;

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
        vwapThreads         = ConfigUtility.getInt(config, Conf.VWAP_THREADS, 1);
        bargainIndexThreads = ConfigUtility.getInt(config, Conf.BARGAIN_INDEX_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        builder.setSpout(Component.TRADES_QUOTES, spout, spoutThreads);

        builder.setBolt(Component.VWAP , new VwapBolt(), vwapThreads)
               .fieldsGrouping(Component.TRADES_QUOTES, Stream.TRADES, new Fields(Field.STOCK));
        
        builder.setBolt(Component.BARGAIN_INDEX, new BargainIndexBolt(), bargainIndexThreads)
               .fieldsGrouping(Component.VWAP, new Fields(Field.STOCK))
               .fieldsGrouping(Component.TRADES_QUOTES, Stream.QUOTES, new Fields(Field.STOCK));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .fieldsGrouping(Component.BARGAIN_INDEX, new Fields(Field.STOCK));
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
