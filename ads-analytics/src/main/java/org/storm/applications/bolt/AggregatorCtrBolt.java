package org.storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import org.storm.applications.spout.AdEventSpout;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AggregatorCtrBolt extends RollingCtrBolt {

    public AggregatorCtrBolt() {
    }

    public AggregatorCtrBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        super(windowLengthInSeconds, emitFrequencyInSeconds);
    }
    
    @Override
    protected void countObjAndAck(Tuple tuple) {
        String queryId = tuple.getStringByField("queryId");
        String adId = tuple.getStringByField("adId");
        long clicks = tuple.getLongByField("clicks");
        long impressions = tuple.getLongByField("impressions");
        
        String key = String.format("%d:%d", queryId, adId);
        
        if (tuple.getSourceStreamId().equals(AdEventSpout.CLICK_STREAM))
            clickCounter.incrementCount(key, clicks);
        else
            impressionCounter.incrementCount(key, impressions);
        
        collector.ack(tuple);
    }
    
}
