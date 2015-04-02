package storm.applications.spout.parser;

import java.util.ArrayList;
import java.util.List;
import storm.applications.constants.AdsAnalyticsConstants.Stream;
import storm.applications.model.ads.AdEvent;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author mayconbordin
 */
public class AdEventParser extends Parser {
    private static final int CLICKS        = 0;
    private static final int VIEWS         = 1;
    private static final int DISPLAY_URL   = 2;
    private static final int AD_ID         = 3;
    private static final int ADVERTISER_ID = 4;
    private static final int DEPTH         = 5;
    private static final int POSITION      = 6;
    private static final int QUERY_ID      = 7;
    private static final int KEYWORD_ID    = 8;
    private static final int TITLE_ID      = 9;
    private static final int DESC_ID       = 10;
    private static final int USER_ID       = 11;
    
    @Override
    public List<StreamValues> parse(String input) {
        String[] record = input.split("\t");
        
        if (record.length != 12)
            return null;
           
        int clicks         = Integer.parseInt(record[CLICKS]);
        int views          = Integer.parseInt(record[VIEWS]);
        String displayUrl  = record[DISPLAY_URL];
        long adId          = Long.parseLong(record[AD_ID]);
        long advertiserId  = Long.parseLong(record[ADVERTISER_ID]);
        int depth          = Integer.parseInt(record[DEPTH]);
        int position       = Integer.parseInt(record[POSITION]);
        long queryId       = Long.parseLong(record[QUERY_ID]);
        long keywordId     = Long.parseLong(record[KEYWORD_ID]);
        long titleId       = Long.parseLong(record[TITLE_ID]);
        long descriptionId = Long.parseLong(record[DESC_ID]);
        long userId        = Long.parseLong(record[USER_ID]);

        List<StreamValues> tuples = new ArrayList<>();
        
        for (int i=0; i<views+clicks; i++) {
            AdEvent event = new AdEvent(displayUrl, queryId, adId, userId, advertiserId, 
                keywordId, titleId, descriptionId, depth, position);
            
            event.setType((i < views) ? AdEvent.Type.Impression : AdEvent.Type.Click);
            
            String streamId = (event.getType() == AdEvent.Type.Click) 
                    ? Stream.CLICKS : Stream.IMPRESSIONS;
            
            StreamValues values = new StreamValues(queryId, adId, event);
            values.setStreamId(streamId);
            tuples.add(values);
        }
        
        return tuples;
    }
    
}
