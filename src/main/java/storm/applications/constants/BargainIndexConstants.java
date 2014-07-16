package storm.applications.constants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface BargainIndexConstants extends BaseConstants {
    String PREFIX = "bi";
    
    interface Conf extends BaseConf {
        String VWAP_THREADS = "bi.vwap.threads";
        String VWAP_PERIOD  = "bi.vwap.period";
        String BARGAIN_INDEX_THREADS = "bi.bargainindex.threads";
        String BARGAIN_INDEX_THRESHOLD = "bi.bargainindex.threshold";
        
        String SPOUT_FETCHER  = "bi.spout.fetcher";
        String SPOUT_SYMBOLS  = "bi.spout.symbols";
        String SPOUT_DAYS     = "bi.spout.days";
        String SPOUT_INTERVAL = "bi.spout.interval";
    }
    
    interface Component extends BaseComponent {
        String VWAP = "vwapBolt";
        String BARGAIN_INDEX = "bargainIndexBolt";
    }
    
    interface Periodicity {
        String MINUTELY = "minutely";
        String HOURLY   = "hourly";
        String DAILY    = "daily";
        String WEEKLY   = "weekly";
        String MONTHLY  = "monthly";
    }
    
    interface Stream {
        String QUOTES = "quoteStream";
        String TRADES = "tradeStream";
    }
    
    interface Field {
        String STOCK = "stock";
        String PRICE = "price";
        String VOLUME = "volume";
        String DATE   = "date";
        String START_DATE = "startDate";
        String END_DATE   = "endDate";
        String INTERVAL = "interval";
        String VWAP = "vwap";
        String BARGAIN_INDEX = "bargainIndex";
    }
}
