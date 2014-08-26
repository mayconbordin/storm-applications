package storm.applications.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.AdsAnalyticsConstants.Field;
import storm.applications.constants.AdsAnalyticsConstants.Stream;
import storm.applications.model.ads.AdEvent;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CtrBolt extends AbstractBolt {
    private Map<String, Summary> summaries;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.QUERY_ID, Field.AD_ID, Field.CTR);
    }

    @Override
    public void initialize() {
        summaries = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        AdEvent event = (AdEvent) input.getValueByField(Field.EVENT);
        
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        Summary summary = summaries.get(key);
        
        // create summary if it don't exists
        if (summary == null) {
            summary = new Summary();
            summaries.put(key, summary);
        }
        
        // update summary
        if (input.getSourceStreamId().equals(Stream.CLICKS)) {
            summary.clicks++;
        } else {
            summary.impressions++;
        }
        
        // calculate ctr
        double ctr = (double)summary.clicks / (double)summary.impressions;
        
        collector.emit(input, new Values(event.getQueryId(), event.getAdID(), ctr));
        collector.ack(input);
    }
    
    private static class Summary {
        public long impressions = 0;
        public long clicks = 0;
    }
}
