package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.math.VariableEWMA;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACDBolt.class);
    
    private VariableEWMA avgCallDuration;
    private double decayFactor;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.AVERAGE);
    }

    @Override
    public void initialize() {
        decayFactor = config.getDouble(Conf.ACD_DECAY_FACTOR, 86400); //86400s = 24h
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        long timestamp = cdr.getAnswerTime().getMillis()/1000;

        avgCallDuration.add(cdr.getCallDuration());
        collector.emit(new Values(timestamp, avgCallDuration.getAverage()));
    }
}