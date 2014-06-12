package org.storm.applications.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.log4j.Logger;
import org.storm.applications.model.AdEvent;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdEventSpout extends AbstractFileSpout {
    public static final String CLICK_STREAM = "ClickStream";
    public static final String IMPRESSION_STREAM = "ImpressionStream";
    
    private static Logger LOG = Logger.getLogger(AdEventSpout.class);
    
    private Queue<AdEvent> queue = new LinkedList<AdEvent>();

    public AdEventSpout(String path) {
        super(path);
    }
    
    @Override
    public void nextTuple() {
        AdEvent event = null;
        
        if (queue.isEmpty()) {
            String strRecord = readFile();

            if (strRecord != null) {
                event = nextEvent(strRecord);
            } else {
                LOG.info("End of data");
            }
        } else {
            event = queue.poll();
        }
        
        if (event != null) {
            String streamId = (event.getType() == AdEvent.Type.Click) 
                    ? CLICK_STREAM : IMPRESSION_STREAM;
            
            collector.emit(streamId, new Values(event.getQueryId(), event.getAdID(), event));
        }
    }

    protected AdEvent nextEvent(String strRecord) {
        String[] record = strRecord.split("\t");
        
        if (record.length != 12)
            return null;
           
        int clicks        = Integer.parseInt(record[0]);
        int views         = Integer.parseInt(record[1]);
        String displayUrl = record[2];
        long adId         = Long.parseLong(record[3]);
        long advertiserId = Long.parseLong(record[4]);
        int depth         = Integer.parseInt(record[5]);
        int position      = Integer.parseInt(record[6]);
        long queryId      = Long.parseLong(record[7]);
        long keywordId    = Long.parseLong(record[8]);
        long titleId      = Long.parseLong(record[9]);
        long descriptionId= Long.parseLong(record[10]);
        long userId       = Long.parseLong(record[11]);

        AdEvent firstEvent = null;
        for (int i=0; i<views+clicks; i++) {
            AdEvent event = new AdEvent(displayUrl, queryId, adId, userId, advertiserId, 
                keywordId, titleId, descriptionId, depth, position);
            
            event.setType((i < views) ? AdEvent.Type.Impression : AdEvent.Type.Click);
            
            if (i == 0)
                firstEvent = event;
            else
                queue.add(event);
        }
        
        return firstEvent;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CLICK_STREAM, new Fields("queryId", "adId", "adEvent"));
        declarer.declareStream(IMPRESSION_STREAM, new Fields("queryId", "adId", "adEvent"));
    }

    @Override
    protected Values nextRecord(String strRecord) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
