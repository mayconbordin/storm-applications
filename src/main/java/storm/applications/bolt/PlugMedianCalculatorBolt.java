package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import static storm.applications.constants.SmartGridConstants.*;
import storm.applications.util.math.RunningMedianCalculator;

/**
 * Author: Thilina
 * Date: 12/5/14
 */
public class PlugMedianCalculatorBolt extends AbstractBolt {
    private Map<String, RunningMedianCalculator> runningMedians;
    private Map<String, Long> lastUpdatedTsMap;

    @Override
    public void initialize() {
        runningMedians = new HashMap<>();
        lastUpdatedTsMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        int operation  = tuple.getIntegerByField(Field.SLIDING_WINDOW_ACTION);
        double value   = tuple.getDoubleByField(Field.VALUE);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        String key     = getKey(tuple);

        RunningMedianCalculator medianCalc = runningMedians.get(key);
        if (medianCalc == null) {
            medianCalc =  new RunningMedianCalculator();
            runningMedians.put(key, medianCalc);
        }
        
        Long lastUpdatedTs = lastUpdatedTsMap.get(key);
        if (lastUpdatedTs == null) {
            lastUpdatedTs = 0l;
        }

        if (operation == SlidingWindowAction.ADD){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTsMap.put(key, timestamp);
                collector.emit(new Values(key, timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.PLUG_SPECIFIC_KEY, Field.TIMESTAMP, Field.PER_PLUG_MEDIAN);
    }

    private String getKey(Tuple tuple) {
        return tuple.getStringByField(Field.HOUSE_ID) + ':' + 
                tuple.getStringByField(Field.HOUSEHOLD_ID) + ':' +
                tuple.getStringByField(Field.PLUG_ID);
    }

}