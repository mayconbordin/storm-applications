package storm.applications.constants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface AdsAnalyticsConstants {
    interface Conf {
        String SINK_FILE = "aa.sink.file";
        String SPOUT_FILE = "aa.spout.file";
    }
    
    interface Component {
        String SPOUT = "spout";
        String CTR_BOLT = "ctrBolt";
        String CTR_AGGREGATOR_BOLT = "aggregatorCtrBolt";
        String SINK = "sink";
    }
    
    interface Stream {
        String CLICKS = "clickStream";
        String IMPRESSIONS = "impressionStream";
    }
}
