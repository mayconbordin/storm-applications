package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import storm.applications.constants.SmartGridConstants.Component;
import storm.applications.constants.SmartGridConstants.Field;
import storm.applications.util.collections.FixedMap;
import storm.applications.util.math.OutlierTracker;

/**
 * Author: Thilina
 * Date: 12/6/14
 */
public class OutlierDetectionBolt extends AbstractBolt {
    private FixedMap<Long, Double> globalMedianBacklog;
    private Map<String, OutlierTracker> outliers;
    private PriorityQueue<ComparableTuple> unprocessedMessages;

    @Override
    public void initialize() {
        globalMedianBacklog = new FixedMap<>(300, 300);
        outliers = new HashMap<>();
        unprocessedMessages = new PriorityQueue<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String component = tuple.getSourceComponent();
        
        if (component.equals(Component.GLOBAL_MEDIAN)) {
            long timestamp = tuple.getLongByField(Field.TIMESTAMP);
            double globalMedianLoad = tuple.getDoubleByField(Field.GLOBAL_MEDIAN_LOAD);
            
            globalMedianBacklog.put(timestamp, globalMedianLoad);
            
            // ordered based on the timestamps
            while (!unprocessedMessages.isEmpty() &&
                    unprocessedMessages.peek().tuple.getLongByField(Field.TIMESTAMP).equals(timestamp)) {
                Tuple perPlugMedianTuple = unprocessedMessages.poll().tuple;
                processPerPlugMedianTuple(perPlugMedianTuple);
            }
        } else {
            processPerPlugMedianTuple(tuple);
        }
    }

    private void processPerPlugMedianTuple(Tuple tuple) {
        String key     = tuple.getStringByField(Field.PLUG_SPECIFIC_KEY);
        String houseId = key.split(":")[0];
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        double value   = tuple.getDoubleByField(Field.PER_PLUG_MEDIAN);
        
        if (globalMedianBacklog.containsKey(timestamp)) {
            OutlierTracker tracker;
            
            if (outliers.containsKey(houseId)) {
                tracker = outliers.get(houseId);
            } else {
                tracker = new OutlierTracker();
                outliers.put(houseId, tracker);
            }
            
            if (!tracker.isMember(key)) {
                tracker.addMember(key);
            }
            
            double globalMedian = globalMedianBacklog.get(timestamp);
            if (globalMedian < value) { // outlier
                if (!tracker.isOutlier(key)) {
                    tracker.addOutlier(key);
                    collector.emit(new Values(timestamp - 24 * 60 * 60, timestamp,
                            houseId, tracker.getCurrentPercentage()));
                }
            } else {
                if (tracker.isOutlier(key)) {
                    tracker.removeOutlier(key);
                    //emit
                    collector.emit(new Values(timestamp - 24 * 60 * 60, timestamp,
                            houseId, tracker.getCurrentPercentage()));
                }
            }
        } else {    // global median has not arrived
            unprocessedMessages.add(new ComparableTuple(tuple));
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.SLIDING_WINDOW_START, Field.SLIDING_WINDOW_END, 
                Field.HOUSE_ID, Field.OUTLIER_PERCENTAGE);
    }
    
    private class ComparableTuple implements Serializable, Comparable<ComparableTuple> {
        private final Tuple tuple;

        private ComparableTuple(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public int compareTo(ComparableTuple o) {
            return this.tuple.getLongByField(Field.TIMESTAMP).compareTo(
                    o.tuple.getLongByField(Field.TIMESTAMP));
        }
    }
}