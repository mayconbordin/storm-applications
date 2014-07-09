package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static storm.applications.constants.BargainIndexConstants.*;
import storm.applications.bolt.BargainIndexBolt;
import storm.applications.bolt.VwapBolt;
import storm.applications.model.finance.GoogleQuoteFetcher;
import storm.applications.sink.FileSink;
import storm.applications.spout.TradeQuoteSimulatedSpout;
import storm.applications.util.ConfigUtility;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BargainIndexTopology extends AbstractTopology {
    private int tradeQuoteSpoutThreads;
    private int vwapThreads;
    private int bargainIndexThreads;
    private int fileSinkThreads;
    private String fileSinkPath;
    
    public BargainIndexTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void prepare() {
        tradeQuoteSpoutThreads = ConfigUtility.getInt(config, "bargain.index.tradequote.threads");
        vwapThreads = ConfigUtility.getInt(config, "bargain.index.vwap.threads");
        bargainIndexThreads = ConfigUtility.getInt(config, "bargain.index.bargainindex.threads");
        fileSinkThreads = ConfigUtility.getInt(config, "bargain.index.filesink.threads");
        fileSinkPath = ConfigUtility.getString(config, "bargain.index.filesink.path");
    }

    @Override
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();
        
        GoogleQuoteFetcher fetcher = new GoogleQuoteFetcher();
        
        builder.setSpout(TRADE_QUOTE_SPOUT, new TradeQuoteSimulatedSpout(fetcher, "AAPL"),
                tradeQuoteSpoutThreads);
        
        // TODO: use tick tuples to have time-based windows
        // instead of periodicity?
        // maybe a more flexible data structure, enabling to set custom time periods
        // like 5 minutes, 15 hours,  ...
        builder.setBolt(VWAP_BOLT , new VwapBolt(VwapBolt.Periodicity.DAILY), vwapThreads)
               .fieldsGrouping(TRADE_QUOTE_SPOUT, TRADE_STREAM, new Fields(STOCK_FIELD));
        
        builder.setBolt(BARGAIN_INDEX_BOLT, new BargainIndexBolt(), bargainIndexThreads)
               .fieldsGrouping(VWAP_BOLT, new Fields(STOCK_FIELD))
               .fieldsGrouping(TRADE_QUOTE_SPOUT, QUOTE_STREAM, new Fields(STOCK_FIELD));
        
        builder.setBolt(FILE_SINK, new FileSink(fileSinkPath), fileSinkThreads)
               .fieldsGrouping(BARGAIN_INDEX_BOLT, new Fields(STOCK_FIELD));
        
        return builder.createTopology();
    }
    
}
