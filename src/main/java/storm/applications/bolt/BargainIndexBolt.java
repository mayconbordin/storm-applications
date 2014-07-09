package storm.applications.bolt;

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
import static storm.applications.constants.BargainIndexConstants.*;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class BargainIndexBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, TradeSummary> trades;

    public BargainIndexBolt() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(STOCK_FIELD, PRICE_FIELD, VOLUME_FIELD, BARGAIN_INDEX_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        trades = new HashMap<String, TradeSummary>();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(QUOTE_STREAM)) {
            String stock    = input.getStringByField(STOCK_FIELD);
            double askPrice = input.getDoubleByField(PRICE_FIELD);
            int askSize     = input.getIntegerByField(VOLUME_FIELD);
            DateTime date   = (DateTime) input.getValueByField(DATE_FIELD);
            
            TradeSummary summary = trades.get(stock);
            double bargainIndex = 0;
            
            
            if (summary != null) {
                if (summary.vwap > askPrice) {
                    bargainIndex = Math.exp(summary.vwap - askPrice) * askSize;
                    outputCollector.emit(new Values(stock, askPrice, askSize, bargainIndex));
                }
            }
            
            // if vwap > askPrice
                // calculate the bargain index
                // double bargainIndex = Math.exp(vwap - askPrice) * askSize;
            // else
                // double bargainIndex = 0;
            
                // if bargainIndex > 1.0
                    // by how much (set threshold for placing orders)
                    // send order with askPrice and askSize
        } else {
            String stock = input.getStringByField(STOCK_FIELD);
            double vwap  = (Double) input.getValueByField(VWAP_FIELD);
            DateTime endDate = (DateTime) input.getValueByField(END_DATE_FIELD);

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
