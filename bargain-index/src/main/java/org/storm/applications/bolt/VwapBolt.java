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
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import static org.storm.applications.BargainIndexConstants.*;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class VwapBolt extends BaseRichBolt {
    private static final DateTimeComparator dateOnlyComparator = DateTimeComparator.getDateOnlyInstance();
    
    public static enum Periodicity {
        MINUTELY, HOURLY, DAILY, WEEKLY, MONTHLY
    }
    
    private OutputCollector outputCollector;
    private Map<String, Vwap> stocks;
    private Periodicity period;

    public VwapBolt(Periodicity period) {
        this.period = period;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(STOCK_FIELD, VWAP_FIELD, START_DATE_FIELD, END_DATE_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        stocks = new HashMap<String, Vwap>();
        
    }

    @Override
    public void execute(Tuple input) {
        String stock  = input.getStringByField(STOCK_FIELD);
        double price  = (double) input.getDoubleByField(PRICE_FIELD);
        int volume    = (int) input.getIntegerByField(VOLUME_FIELD);
        DateTime date = (DateTime) input.getValueByField(DATE_FIELD);
        int inteval   = input.getIntegerByField(INTERVAL_FIELD);

        Vwap vwap = stocks.get(stock);

        if (withinPeriod(vwap, date)) {
            vwap.update(volume, price, date.plusSeconds(inteval));
            outputCollector.emit(new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        } else {
            if (vwap != null)
                outputCollector.emit(new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));

            vwap = new Vwap(volume, price, date, date.plusSeconds(inteval));
            stocks.put(stock, vwap);

            outputCollector.emit(new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        }
    }
    
    private boolean withinPeriod(Vwap vwap, DateTime quoteDate) {
        if (vwap == null) return false;
        
        DateTime vwapDate  = vwap.getStartDate();
        
        switch (period) {
            case MINUTELY:
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getMinuteOfDay() == quoteDate.getMinuteOfDay());
            
            case HOURLY:
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getHourOfDay() == quoteDate.getHourOfDay());
                
            case DAILY:
                return (dateOnlyComparator.compare(vwapDate, quoteDate) == 0);
                
            case WEEKLY:
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getWeekOfWeekyear() == quoteDate.getWeekOfWeekyear());
                
            case MONTHLY:
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
