package storm.applications.spout.parser;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.util.Configuration;
import storm.applications.util.StreamValues;

/**
 *
 * @author mayconbordin
 */
public class SensorParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(SensorParser.class);
    
    private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
            .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
            .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
            .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
            .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();
    
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    
    private static final int DATE_FIELD   = 0;
    private static final int TIME_FIELD   = 1;
    private static final int EPOCH_FIELD  = 2;
    private static final int MOTEID_FIELD = 3;
    private static final int TEMP_FIELD   = 4;
    private static final int HUMID_FIELD  = 5;
    private static final int LIGHT_FIELD  = 6;
    private static final int VOLT_FIELD   = 7;
    
    private static final ImmutableMap<String, Integer> fieldList = ImmutableMap.<String, Integer>builder()
            .put("temp", TEMP_FIELD)
            .put("humid", HUMID_FIELD)
            .put("light", LIGHT_FIELD)
            .put("volt", VOLT_FIELD)
            .build();
    
    private String valueField;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        valueField = config.getString(Conf.PARSER_VALUE_FIELD);
    }

    @Override
    public List<StreamValues> parse(String input) {
        String[] fields = input.split("\\s+");
        
        if (fields.length != 8)
            return null;
        
        String dateStr = String.format("%s %s", fields[DATE_FIELD], fields[TIME_FIELD]);
        DateTime date = null;
        
        try {
            date = formatterMillis.parseDateTime(dateStr);
        } catch (IllegalArgumentException ex) {
            try {
                date = formatter.parseDateTime(dateStr);
            } catch (IllegalArgumentException ex2) {
                LOG.warn("Record: " + input);
                LOG.warn("Error parsing record date/time field", ex);
                return null;
            }
        }
        
        
        try {
            StreamValues values = new StreamValues();
            values.add(fields[MOTEID_FIELD]);
            values.add(date.toDate());

            //values.add(Integer.parseInt(fields[EPOCH_FIELD]));
            //values.add(Double.parseDouble(fields[TEMP_FIELD]));
            //values.add(Double.parseDouble(fields[HUMID_FIELD]));
            //values.add(Double.parseDouble(fields[LIGHT_FIELD]));
            //values.add(Double.parseDouble(fields[VOLT_FIELD]));
            
            int valueFieldKey = fieldList.get(valueField);
            values.add(Double.parseDouble(fields[valueFieldKey]));
            
            return list(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Record: " + input);
            LOG.warn("Error parsing record numeric field", ex);
        }
        
        return null;
    }
    
}
