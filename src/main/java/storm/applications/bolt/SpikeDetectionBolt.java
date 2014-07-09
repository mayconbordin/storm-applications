package storm.applications.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionBolt.class);

    private float spikeThreshold = 0.03f;

    public SpikeDetectionBolt() {
    }

    public SpikeDetectionBolt(float spikeThreshold) {
        this.spikeThreshold = spikeThreshold;
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        final String deviceID = tuple.getString(0);
        final double movingAverageInstant = tuple.getDouble(1);
        final double nextDouble = tuple.getDouble(2);
        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            collector.emit(new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
            LOG.info(deviceID + "  " + movingAverageInstant + "   " + nextDouble  + " spike detected");
        }
    }
    
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("deviceID", "movingAverage", "value", "message"));
    }
}