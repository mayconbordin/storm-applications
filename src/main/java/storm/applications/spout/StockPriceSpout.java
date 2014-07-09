package storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.model.finance.Quote;
import storm.applications.model.finance.QuoteFetcher;

/**
 * Change name to TAQ (Trade and Quotes)
 * @author mayconbordin <mayconbordin@gmail.com>
 */
public class StockPriceSpout extends BaseRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(StockPriceSpout.class);

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

    public StockPriceSpout(QuoteFetcher fetcher, String symbol) {
        this.fetcher = fetcher;
        this.symbol = symbol;
    }
    
    public StockPriceSpout(QuoteFetcher fetcher, String symbol, int days) {
        this.fetcher = fetcher;
        this.symbol = symbol;
        this.days = days;
    }
    
    public StockPriceSpout(QuoteFetcher fetcher, String symbol, int days, int interval) {
        this.fetcher = fetcher;
        this.symbol = symbol;
        this.days = days;
        this.interval = interval;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stock", "quote"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
        
        // need to diferentiate between quotes and trades
        
        try {
            String quoteStr = fetcher.fetchQuotes(symbol, days, interval);
            quotes = fetcher.parseQuotes(quoteStr, interval);
        } catch (Exception ex) {
            LOG.error("Unable to fetch quotes", ex);
        }
    }

    @Override
    public void nextTuple() {
        if (nextQuote < quotes.size()) {
            outputCollector.emit(new Values(symbol, quotes.get(nextQuote++)));
        }
    }
    
}
