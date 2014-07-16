package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import static storm.applications.constants.BargainIndexConstants.*;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class BargainIndexBolt extends AbstractBolt {
    private Map<String, TradeSummary> trades;
    private double threshold;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.STOCK, Field.PRICE, Field.VOLUME, Field.BARGAIN_INDEX);
    }

    @Override
    public void initialize() {
        threshold = config.getDouble(Conf.BARGAIN_INDEX_THRESHOLD, 0.001);
        trades = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String stream = input.getSourceStreamId();
        
        if (stream.equals(Stream.QUOTES)) {
            String stock    = input.getStringByField(Field.STOCK);
            double askPrice = input.getDoubleByField(Field.PRICE);
            int askSize     = input.getIntegerByField(Field.VOLUME);
            DateTime date   = (DateTime) input.getValueByField(Field.DATE);
            
            TradeSummary summary = trades.get(stock);
            double bargainIndex = 0;
            
            
            if (summary != null) {
                if (summary.vwap > askPrice) {
                    bargainIndex = Math.exp(summary.vwap - askPrice) * askSize;
                    
                    if (bargainIndex > threshold)
                        collector.emit(new Values(stock, askPrice, askSize, bargainIndex));
                }
            }
        } else if (stream.equals(Stream.TRADES)) {
            String stock = input.getStringByField(Field.STOCK);
            double vwap  = (Double) input.getValueByField(Field.VWAP);
            DateTime endDate = (DateTime) input.getValueByField(Field.END_DATE);

            if (trades.containsKey(stock)) {
                TradeSummary summary = trades.get(stock);
                summary.vwap = vwap;
                summary.date = endDate;
            } else {
                trades.put(stock, new TradeSummary(stock, vwap, endDate));
            }
        }
    }
    
    private static class TradeSummary {
        public String symbol;
        public double vwap;
        public DateTime date;

        public TradeSummary(String symbol, double vwap, DateTime date) {
            this.symbol = symbol;
            this.vwap = vwap;
            this.date = date;
        }
    }
}
