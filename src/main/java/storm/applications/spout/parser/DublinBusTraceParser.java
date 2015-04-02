package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import storm.applications.util.stream.StreamValues;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class DublinBusTraceParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(DublinBusTraceParser.class);
    
    private static final int TIMESTAMP_FIELD            = 0;
    private static final int LINE_ID_FIELD              = 1;
    private static final int DIRECTION_FIELD            = 2;
    private static final int JOURNEY_PATTERN_ID_FIELD   = 3;
    private static final int TIME_FRAME_FIELD           = 4;
    private static final int VEHICLE_JOURNEY_ID_FIELD   = 5;
    private static final int OPERATOR_FIELD             = 6;
    private static final int CONGESTION_FIELD           = 7;
    private static final int LONGITUDE_FIELD            = 8;
    private static final int LATITUDE_FIELD             = 9;
    private static final int DELAY_FIELD                = 10;
    private static final int BLOCK_ID_FIELD             = 11;
    private static final int VEHICLE_ID_FIELD           = 12;
    private static final int STOP_ID_FIELD              = 13;
    private static final int AT_STOP_FIELD              = 14;
    
    
    @Override
    public List<StreamValues> parse(String input) {
        String[] fields = input.split(",");
        
        if (fields.length != 7)
            return null;
        
        try {
            String carId  = fields[VEHICLE_ID_FIELD];
            Date date     = new Date(Long.parseLong(fields[TIMESTAMP_FIELD])/1000);
            boolean occ   = true;
            double lat    = Double.parseDouble(fields[LATITUDE_FIELD]);
            double lon    = Double.parseDouble(fields[LONGITUDE_FIELD]);
            int speed     = 0;
            int bearing   = Integer.parseInt(fields[DIRECTION_FIELD]);
            
            int msgId = String.format("%s:%s", carId, date.toString()).hashCode();
                        
            StreamValues values = new StreamValues(carId, date, occ, speed, bearing, lat, lon);
            values.setMessageId(msgId);
            
            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Error parsing date/time value", ex);
        }
        
        return null;
    }
    
}