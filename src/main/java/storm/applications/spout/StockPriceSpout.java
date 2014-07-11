package storm.applications.spout;

import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BargainIndexConstants.Conf;
import storm.applications.model.finance.Quote;
import storm.applications.model.finance.QuoteFetcher;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;

/**
 * Change name to TAQ (Trade and Quotes)
 * @author mayconbordin <mayconbordin@gmail.com>
 */
public class StockPriceSpout extends AbstractSpout {
    private static Logger LOG = LoggerFactory.getLogger(StockPriceSpout.class);

    private QuoteFetcher fetcher;
    protected LinkedBlockingQueue<Quote> queue;
    
    private String[] symbols;
    private int days;
    private int interval;
    
    private String fetcherKey  = Conf.SPOUT_FETCHER;
    private String symbolsKey  = Conf.SPOUT_SYMBOLS;
    private String daysKey     = Conf.SPOUT_DAYS;
    private String intervalKey = Conf.SPOUT_INTERVAL;

    @Override
    public void initialize() {
        days     = ConfigUtility.getInt(config, daysKey);
        interval = ConfigUtility.getInt(config, intervalKey);
        
        String symbolsStr   = ConfigUtility.getString(config, symbolsKey);
        symbols = symbolsStr.split(",");
        
        String fetcherClass = ConfigUtility.getString(config, fetcherKey);
        fetcher = (QuoteFetcher) ClassLoaderUtils.newInstance(fetcherClass, "fetcher", LOG);
        
        queue = new LinkedBlockingQueue<>();
        
        fetchPrices();
    }
    
    private void fetchPrices() {
        List<Quote> quotes = new ArrayList<>();
        
        for (String symbol : symbols) {
            try {
                String quoteStr = fetcher.fetchQuotes(symbol, days, interval);
                quotes.addAll(fetcher.parseQuotes(symbol, quoteStr, interval));
            } catch (Exception ex) {
                LOG.error("Unable to fetch quotes", ex);
            }
        }
        
        Collections.sort(quotes, new Comparator<Quote>() {
            @Override
            public int compare(Quote a, Quote b) {
                return a.getOpenDate().compareTo(b.getOpenDate());
            }
        });
        
        queue.addAll(quotes);
    }

    @Override
    public void nextTuple() {
        Quote quote = queue.poll();
        
        if (quote != null) {
            collector.emit(new Values(quote.getSymbol(), quote));
        }
    }

    public void setFetcherKey(String fetcherKey) {
        this.fetcherKey = fetcherKey;
    }

    public void setSymbolsKey(String symbolsKey) {
        this.symbolsKey = symbolsKey;
    }

    public void setDaysKey(String daysKey) {
        this.daysKey = daysKey;
    }

    public void setIntervalKey(String intervalKey) {
        this.intervalKey = intervalKey;
    }
    
}
