package storm.applications.spout.parser;

import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BeijingTaxiTraceParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(BeijingTaxiTraceParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
    
    private static final int ID_FIELD    = 0;
    private static final int NID_FIELD   = 1;
    private static final int DATE_FIELD  = 2;
    private static final int LAT_FIELD   = 3;
    private static final int LON_FIELD   = 4;
    private static final int SPEED_FIELD = 5;
    private static final int DIR_FIELD   = 6;
    
    @Override
    public List<StreamValues> parse(String input) {
        String[] values = input.split(",");
        
        if (values.length != 7)
            return null;
        
        try {
            String carId  = values[ID_FIELD];
            DateTime date = formatter.parseDateTime(values[DATE_FIELD]);
            boolean occ   = true;
            double lat    = Double.parseDouble(values[LAT_FIELD]);
            double lon    = Double.parseDouble(values[LON_FIELD]);
            int speed     = ((Double)Double.parseDouble(values[SPEED_FIELD])).intValue();
            int bearing   = Integer.parseInt(values[DIR_FIELD]);
            
            return list(new StreamValues(carId, date, occ, speed, bearing, lat, lon));
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Error parsing date/time value", ex);
        }
        
        return null;
    }
    
}
