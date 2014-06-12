package org.storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import static org.storm.applications.BargainIndexConstants.*;
import org.storm.applications.quote.Quote;
import org.storm.applications.quote.QuoteFetcher;
import org.storm.applications.util.RandomUtil;

/**
 * Fetches 
 * @author mayconbordin <mayconbordin@gmail.com>
 */
public class TradeQuoteSimulatedSpout extends BaseRichSpout {
    private static Logger LOG = Logger.getLogger(TradeQuoteSimulatedSpout.class);

    private SpoutOutputCollector outputCollector;
    private QuoteFetcher fetcher;
    private String symbol;
    private int days = 30;
    
    /**
     * Interval between quotes in seconds.
     */
    private int interval = 60;
    private List<Quote> quotes;
    private int nextQuote = 0;

    public TradeQuoteSimulatedSpout(QuoteFetcher fetcher, String symbol) {
        this.fetcher = fetcher;
        this.symbol = symbol;
    }
    
    public TradeQuoteSimulatedSpout(QuoteFetcher fetcher, String symbol, int days) {
        this.fetcher = fetcher;
        this.symbol = symbol;
        this.days = days;
    }
    
    public TradeQuoteSimulatedSpout(QuoteFetcher fetcher, String symbol, int days, int interval) {
        this.fetcher = fetcher;
        this.symbol = symbol;
        this.days = days;
        this.interval = interval;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("stock", "quote"));
        declarer.declareStream(TRADE_STREAM, new Fields(STOCK_FIELD, PRICE_FIELD, 
                VOLUME_FIELD, DATE_FIELD, INTERVAL_FIELD));
        declarer.declareStream(QUOTE_STREAM, new Fields(STOCK_FIELD, PRICE_FIELD, 
                VOLUME_FIELD, DATE_FIELD));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
        
        // need to diferentiate between quotes and trades
        
        try {
            String quoteStr = fetcher.fetchQuotes(symbol, days, interval);
            quotes = fetcher.parseQuotes(quoteStr, interval);
        } catch (Exception ex) {
            LOG.error(ex);
        }
    }

    @Override
    public void nextTuple() {
        if (nextQuote < quotes.size()) {
            Quote quote = quotes.get(nextQuote++);
            outputCollector.emit(TRADE_STREAM, new Values(symbol, quote.getAverage(), quote.getVolume(), quote.getOpenDate(), quote.getInterval()));
            
            // here we simulate an askPrice that range between 90% and 110% of the
            // average current price for the same symbol
            double askPrice = RandomUtil.randDouble(quote.getAverage()*0.9, quote.getAverage()*1.1);
            
            // the size offered ranges from 10% to 80% of the volume
            int askSize = RandomUtil.randInt((int)(quote.getVolume()*0.1), (int)(quote.getVolume()*0.8));
            
            outputCollector.emit(QUOTE_STREAM, new Values(symbol, askPrice, askSize, quote.getOpenDate().plusMinutes(2)));
        }
    }
    
}
