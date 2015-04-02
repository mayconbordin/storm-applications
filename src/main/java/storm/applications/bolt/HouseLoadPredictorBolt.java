package storm.applications.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.applications.constants.SmartGridConstants.Field;

/**
 * Author: Thilina
 * Date: 10/31/14
 */
public class HouseLoadPredictorBolt extends LoadPredictorBolt {

    public HouseLoadPredictorBolt() {
        super();
    }

    public HouseLoadPredictorBolt(int emitFrequencyInSeconds) {
        super(emitFrequencyInSeconds);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.HOUSE_ID, Field.PREDICTED_LOAD);
    }

    @Override
    protected String getKey(Tuple tuple) {
        return tuple.getStringByField(Field.HOUSE_ID);
    }

    @Override
    protected Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue) {
        return new Values(predictedTimeStamp, keyString, predictedValue);
    }
}