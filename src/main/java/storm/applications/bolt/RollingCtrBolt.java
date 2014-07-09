package storm.applications.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.AdsAnalyticsConstants.Stream;
import storm.applications.model.ads.AdEvent;
import storm.applications.tools.NthLastModifiedTimeTracker;
import storm.applications.tools.SlidingWindowCounter;
import storm.applications.util.TupleUtils;

public class RollingCtrBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = LoggerFactory.getLogger(RollingCtrBolt.class);
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    protected final SlidingWindowCounter<String> clickCounter;
    protected final SlidingWindowCounter<String> impressionCounter;
    protected final int windowLengthInSeconds;
    protected final int emitFrequencyInSeconds;
    protected OutputCollector collector;
    protected NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingCtrBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RollingCtrBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;

        clickCounter = new SlidingWindowCounter<String>(deriveNumWindowChunksFrom(
                this.windowLengthInSeconds, this.emitFrequencyInSeconds));
        impressionCounter = new SlidingWindowCounter<String>(deriveNumWindowChunksFrom(
                this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(
                this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            LOG.info("Received tick tuple, triggering emit of current window counts");
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
        AdEvent event = (AdEvent) tuple.getValueByField("adEvent");
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        
        if (tuple.getSourceStreamId().equals(Stream.CLICKS))
            clickCounter.incrementCount(key);
        else
            impressionCounter.incrementCount(key);
        
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("queryId", "adId", "ctr", "impressions", 
                "clicks", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}