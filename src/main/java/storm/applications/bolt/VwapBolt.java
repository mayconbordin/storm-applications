package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import static storm.applications.constants.BargainIndexConstants.*;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class VwapBolt extends AbstractBolt {
    private static final DateTimeComparator dateOnlyComparator = DateTimeComparator.getDateOnlyInstance();
    
    private Map<String, Vwap> stocks;
    private String period;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.STOCK, Field.VWAP, Field.START_DATE, Field.END_DATE);
    }

    @Override
    public void initialize() {
        period = config.getString(Conf.VWAP_PERIOD, Periodicity.DAILY);
        
        stocks = new HashMap<>();
        
    }

    @Override
    public void execute(Tuple input) {
        String stock  = input.getStringByField(Field.STOCK);
        double price  = (double) input.getDoubleByField(Field.PRICE);
        int volume    = (int) input.getIntegerByField(Field.VOLUME);
        DateTime date = (DateTime) input.getValueByField(Field.DATE);
        int inteval   = input.getIntegerByField(Field.INTERVAL);

        Vwap vwap = stocks.get(stock);

        if (withinPeriod(vwap, date)) {
            vwap.update(volume, price, date.plusSeconds(inteval));
            collector.emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        } else {
            if (vwap != null) {
                collector.emit(new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
            }
            
            vwap = new Vwap(volume, price, date, date.plusSeconds(inteval));
            stocks.put(stock, vwap);

            collector.emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        }
        
        collector.ack(input);
    }
    
    private boolean withinPeriod(Vwap vwap, DateTime quoteDate) {
        if (vwap == null) return false;
        
        DateTime vwapDate  = vwap.getStartDate();
        
        switch (period) {
            case Periodicity.MINUTELY:
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getMinuteOfDay() == quoteDate.getMinuteOfDay());
            
            case Periodicity.HOURLY:
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getHourOfDay() == quoteDate.getHourOfDay());
                
            case Periodicity.DAILY:
                return (dateOnlyComparator.compare(vwapDate, quoteDate) == 0);
                
            case Periodicity.WEEKLY:
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getWeekOfWeekyear() == quoteDate.getWeekOfWeekyear());
                
            case Periodicity.MONTHLY:
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getMonthOfYear() == quoteDate.getMonthOfYear());
        }
        
        return false;
    }
    
    public static final class Vwap {
        private long totalShares = 0;
        private double tradedValue = 0;
        private double vwap = 0;
        private DateTime startDate;
        private DateTime endDate;

        public Vwap(long shares, double price, DateTime start, DateTime end) {
            this.startDate = start;
            this.endDate = end;
            
            update(shares, price, null);
        }
        
        public void update(long shares, double price, DateTime date) {
            totalShares += shares;
            tradedValue += (shares*price);
            vwap = tradedValue/totalShares;
            
            if (date != null) {
                endDate = date;
            }
        }

        public double getVwap() {
            return vwap;
        }

        public DateTime getStartDate() {
            return startDate;
        }

        public DateTime getEndDate() {
            return endDate;
        }
    }
    
}
