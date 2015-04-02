package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.applications.constants.SmartGridConstants.Field;
import storm.applications.constants.SmartGridConstants.SlidingWindowAction;
import storm.applications.util.math.RunningMedianCalculator;

/**
 *
 * @author mayconbordin
 */
public class GlobalMedianCalculatorBolt extends AbstractBolt {
    private RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;
    
    @Override
    public void initialize() {
        medianCalc = new RunningMedianCalculator();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.GLOBAL_MEDIAN_LOAD);
    }

    @Override
    public void execute(Tuple tuple) {
        int operation  = tuple.getIntegerByField(Field.SLIDING_WINDOW_ACTION);
        double value   = tuple.getDoubleByField(Field.VALUE);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        
        if (operation == SlidingWindowAction.ADD){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                collector.emit(new Values(timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }
    
}
