package storm.applications.constants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface AdsAnalyticsConstants extends BaseConstants {
    String PREFIX = "aa";
    
    interface Conf extends BaseConf {
        String CTR_THREADS = "aa.ctr.threads";
        String CTR_EMIT_FREQUENCY = "aa.ctr.emit_frequency";
        String CTR_WINDOW_LENGTH = "aa.ctr.window_length";
    }
    
    interface Component extends BaseComponent {
        String CTR = "ctrBolt";
        String CTR_AGGREGATOR = "aggregatorCtrBolt";
    }
    
    interface Stream {
        String CLICKS = "clickStream";
        String IMPRESSIONS = "impressionStream";
    }
    
    interface Field {
        String QUERY_ID      = "queryId";
        String AD_ID         = "adId";
        String EVENT         = "event";
        String CTR           = "ctr";
        String IMPRESSIONS   = "impressions";
        String CLICKS        = "clicks";
        String WINDOW_LENGTH = "actualWindowLengthInSeconds";
    }
}
