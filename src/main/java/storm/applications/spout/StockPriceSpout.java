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
import storm.applications.util.config.ClassLoaderUtils;

/**
 * Change name to TAQ (Trade and Quotes)
 * @author mayconbordin <mayconbordin@gmail.com>
 */
public class StockPriceSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(StockPriceSpout.class);

    private QuoteFetcher fetcher;
    protected LinkedBlockingQueue<Quote> queue;
    
    private String[] symbols;
    private int days;
    private int interval;

    @Override
    public void initialize() {
        days     = config.getInt(Conf.SPOUT_DAYS);
        interval = config.getInt(Conf.SPOUT_INTERVAL);
        
        String symbolsStr   = config.getString(Conf.SPOUT_SYMBOLS);
        symbols = symbolsStr.split(",");
        
        String fetcherClass = config.getString(Conf.SPOUT_FETCHER);
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
        if (queue.isEmpty()) {
            fetchPrices();
        }
        
        Quote quote = queue.poll();
        
        if (quote != null) {
            collector.emit(new Values(quote.getSymbol(), quote.getAverage(), quote.getVolume(), quote.getOpenDate(), quote.getInterval()));
        }
    }
}
