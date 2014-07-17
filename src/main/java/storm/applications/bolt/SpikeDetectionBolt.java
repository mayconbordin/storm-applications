package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.constants.SpikeDetectionConstants.Field;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionBolt.class);

    private double spikeThreshold;

    @Override
    public void initialize() {
        this.spikeThreshold = config.getDouble(Conf.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public void execute(final Tuple tuple) {
        String deviceID = tuple.getStringByField(Field.DEVICE_ID);
        double movingAverageInstant = tuple.getDoubleByField(Field.MOVING_AVG);
        double nextDouble = tuple.getDoubleByField(Field.VALUE);
        
        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            collector.emit(new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
        }
    }
    
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE, Field.MESSAGE);
    }
}