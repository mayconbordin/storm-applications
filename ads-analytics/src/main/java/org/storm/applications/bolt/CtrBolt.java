package org.storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.storm.applications.model.AdEvent;
import org.storm.applications.spout.AdEventSpout;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CtrBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, Summary> summaries;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("queryId", "adId", "ctr"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        summaries = new HashMap<String, Summary>();
    }

    public void execute(Tuple input) {
        AdEvent event = (AdEvent) input.getValueByField("adEvent");
        
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        Summary summary = summaries.get(key);
        
        // create summary if it don't exists
        if (summary == null) {
            summary = new Summary();
            summaries.put(key, summary);
        }
        
        // update summary
        if (input.getSourceStreamId().equals(AdEventSpout.CLICK_STREAM))
            summary.clicks++;
        else
            summary.impressions++;
        
        // calculate ctr
        double ctr = (double)summary.clicks / (double)summary.impressions;
        outputCollector.emit(new Values(event.getQueryId(), event.getAdID(), ctr));
    }
    
    private static class Summary {
        public long impressions = 0;
        public long clicks = 0;
    }
}
