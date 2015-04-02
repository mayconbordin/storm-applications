package storm.applications.bolt;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.SmartGridConstants.*;
import storm.applications.util.math.AverageTracker;
import storm.applications.util.math.SummaryArchive;
import storm.applications.util.stream.TupleUtils;

/**
 * Author: Thilina
 * Date: 11/21/14
 */
public abstract class LoadPredictorBolt extends AbstractBolt {
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 15;

    private final int emitFrequencyInSeconds;

    protected long currentSliceStart;
    protected long sliceLength = 60l;
    protected int tickCounter = 0;

    protected Map<String, AverageTracker> trackers;
    protected Map<String, SummaryArchive> archiveMap;
    
    public LoadPredictorBolt() {
        this(DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }
    
    public LoadPredictorBolt(int emitFrequencyInSeconds) {
        super();
        
        if (emitFrequencyInSeconds < 1) {
          throw new IllegalArgumentException(
              "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void initialize() {
        trackers = new HashMap<>();
        archiveMap = new HashMap<>();
        sliceLength = config.getLong(Conf.SLICE_LENGTH, 60l);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            tickCounter = (tickCounter + 1) % 2;
            // time to emit
            if (tickCounter == 0) {
                emitOutputStream();
            }
            return;
        }
        
        int type = tuple.getIntegerByField(Field.PROPERTY);

        if (type == Measurement.WORK) {
            return;
        }

        AverageTracker averageTracker = getTracker(getKey(tuple));
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        double value   = tuple.getDoubleByField(Field.VALUE);

        // Initialize the very first slice
        if (currentSliceStart == 0l) {
            currentSliceStart = timestamp;
        }
        // Check the slice
        // This update is within current slice.
        if ((currentSliceStart + sliceLength) >= timestamp) {
            averageTracker.track(value);
        } else {    // start a new slice
            startSlice();
            currentSliceStart = currentSliceStart + sliceLength;
            // there may be slices without any records.
            while ((currentSliceStart + sliceLength) < timestamp) {
                startSlice();
                currentSliceStart = currentSliceStart + sliceLength;
            }
            averageTracker.track(value);
        }
    }

    private AverageTracker getTracker(String trackerId) {
        AverageTracker tracker;
        if (trackers.containsKey(trackerId)) {
            tracker = trackers.get(trackerId);
        } else {
            tracker = new AverageTracker();
            trackers.put(trackerId, tracker);
        }
        return tracker;
    }

    private SummaryArchive getSummaryArchive(String trackerId) {
        SummaryArchive archive;
        if (archiveMap.containsKey(trackerId)) {
            archive = archiveMap.get(trackerId);
        } else {
            archive = new SummaryArchive(sliceLength);
            archiveMap.put(trackerId, archive);
        }
        return archive;
    }

    protected double predict(double currentAvg, double median) {
        return currentAvg + median;
    }

    private void startSlice() {
        for (String trackerId : trackers.keySet()) {
            AverageTracker tracker = getTracker(trackerId);
            getSummaryArchive(trackerId).archive(tracker.retrieve());
            tracker.reset();
        }
    }

    protected void emitOutputStream() {
        for (String key : trackers.keySet()) {
            double currentAvg = trackers.get(key).retrieve();
            double median = 0;
            
            if (archiveMap.containsKey(key)) {
                median = archiveMap.get(key).getMedian();
            }
            
            double prediction = predict(currentAvg, median);
            long predictedTimeStamp = currentSliceStart + 2 * sliceLength;
            collector.emit(getOutputTuple(predictedTimeStamp, key, prediction));
        }
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    @Override
    public abstract Fields getDefaultFields();
    protected abstract String getKey(Tuple tuple);
    protected abstract Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue);
}