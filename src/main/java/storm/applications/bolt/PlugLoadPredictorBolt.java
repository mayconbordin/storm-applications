package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.applications.constants.SmartGridConstants.Field;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class PlugLoadPredictorBolt extends LoadPredictorBolt {

    public PlugLoadPredictorBolt() {
        super();
    }

    public PlugLoadPredictorBolt(int emitFrequencyInSeconds) {
        super(emitFrequencyInSeconds);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.HOUSE_ID, Field.HOUSEHOLD_ID,
                Field.PLUG_ID, Field.PREDICTED_LOAD);
    }
    
    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getStringByField(Field.HOUSE_ID) + ":" +
                tuple.getStringByField(Field.HOUSEHOLD_ID) + ":" +
                tuple.getStringByField(Field.PLUG_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        String[] segments = keyString.split(":");
        return new Values(predictedTimeStamp, segments[0], segments[1], segments[2], predictedValue);
    }
}