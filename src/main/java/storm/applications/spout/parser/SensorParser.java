package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

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
    private int valueFieldKey;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        valueField = config.getString(Conf.PARSER_VALUE_FIELD);
        valueFieldKey = fieldList.get(valueField);
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
                LOG.warn("Error parsing record date/time field, input record: " + input, ex2);
                return null;
            }
        }
        
        try {
            StreamValues values = new StreamValues();
            values.add(fields[MOTEID_FIELD]);
            values.add(date.toDate());
            values.add(Double.parseDouble(fields[valueFieldKey]));
            
            int msgId = String.format("%s:%s", fields[MOTEID_FIELD], date.toString()).hashCode();
            values.setMessageId(msgId);
            
            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing record numeric field, input record: " + input, ex);
        }
        
        return null;
    }
    
}
