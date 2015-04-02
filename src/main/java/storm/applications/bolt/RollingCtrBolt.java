package storm.applications.bolt;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.AdsAnalyticsConstants.Conf;
import storm.applications.constants.AdsAnalyticsConstants.Field;
import storm.applications.model.ads.AdEvent;
import storm.applications.tools.NthLastModifiedTimeTracker;
import storm.applications.tools.SlidingWindowCounter;
import storm.applications.util.stream.TupleUtils;

public class RollingCtrBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RollingCtrBolt.class);
    
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    protected SlidingWindowCounter<String> clickCounter;
    protected SlidingWindowCounter<String> impressionCounter;
    
    protected int windowLengthInSeconds;
    protected int emitFrequencyInSeconds;
    
    protected NthLastModifiedTimeTracker lastModifiedTracker;
    
    public RollingCtrBolt() {
        this(60);
    }

    public RollingCtrBolt(int emitFrequencyInSeconds) {
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void initialize() {
        windowLengthInSeconds = config.getInt(Conf.CTR_WINDOW_LENGTH, 300);
        
        int windowLenghtInSlots = windowLengthInSeconds / emitFrequencyInSeconds;

        clickCounter      = new SlidingWindowCounter<>(windowLenghtInSlots);
        impressionCounter = new SlidingWindowCounter<>(windowLenghtInSlots);
        
        lastModifiedTracker = new NthLastModifiedTimeTracker(windowLenghtInSlots);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            LOG.debug("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowCounts();
        } else {
            countObjAndAck(tuple);
        }
    }

    private void emitCurrentWindowCounts() {
        Map<String, Long> clickCounts = clickCounter.getCountsThenAdvanceWindow();
        Map<String, Long> impressionCounts = impressionCounter.getCountsThenAdvanceWindow();
        
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        
        emit(clickCounts, impressionCounts, actualWindowLengthInSeconds);
    }

    private void emit(Map<String, Long> clickCounts, Map<String, Long> impressionCounts, int actualWindowLengthInSeconds) {
        for (Entry<String, Long> entry : clickCounts.entrySet()) {
            String key = entry.getKey();
            String[] ids = key.split(":");
            
            long clicks = entry.getValue();
            long impressions = impressionCounts.get(key);
            double ctr = (double)clicks / (double)impressions;
            
            collector.emit(new Values(ids[0], ids[1], ctr, impressions, clicks, actualWindowLengthInSeconds));
        }
    }

    protected void countObjAndAck(Tuple tuple) {
        AdEvent event = (AdEvent) tuple.getValueByField(Field.EVENT);
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        
        if (event.getType() == AdEvent.Type.Click) {
            clickCounter.incrementCount(key);
        } else if (event.getType() == AdEvent.Type.Impression) {
            impressionCounter.incrementCount(key);
        }
        
        collector.ack(tuple);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.QUERY_ID, Field.AD_ID, Field.CTR, Field.IMPRESSIONS,
                Field.CLICKS, Field.WINDOW_LENGTH);
    }
}