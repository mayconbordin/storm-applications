package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import static storm.applications.constants.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class VisitStatsBolt extends AbstractBolt {
    private int total = 0;
    private int uniqueCount = 0;

    @Override
    public void initialize() {
    }

    @Override
    public void execute(Tuple tuple) {
        boolean unique = Boolean.parseBoolean(tuple.getStringByField(Field.UNIQUE));
        total++;
        if(unique) uniqueCount++;
        
        collector.emit(new Values(total, uniqueCount));

    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TOTAL_COUNT, Field.TOTAL_UNIQUE);
    }
}
