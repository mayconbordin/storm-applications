package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.LogProcessingConstants.Field;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountBolt  extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCountBolt.class);
    private Map<String, Integer> counts;

    @Override
    public void initialize() {
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String statusCode = tuple.getStringByField(Field.RESPONSE);
        int count = 0;
        
        if (this.counts.containsKey(statusCode)) {
            count = counts.get(statusCode);
        }
        
        count++;
        counts.put(statusCode, count);
        collector.emit(new Values(statusCode, count));
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.RESPONSE, Field.COUNT);
    }
}
