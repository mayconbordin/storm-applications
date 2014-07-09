package storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.model.cdr.CallDetailRecord;
import static storm.applications.constants.VoIPSTREAMConstants.*;

/**
 * Per-user new callee rate
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ENCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ENCRBolt.class);

    public ENCRBolt() {
        super("encr", RATE_FIELD);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        boolean newCallee = input.getBooleanByField(NEW_CALLEE_FIELD);
        
        if (cdr.isCallEstablished() && newCallee) {
            String caller = input.getStringByField(CALLING_NUM_FIELD);
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            filter.add(caller, 1, timestamp);
            double rate = filter.estimateCount(caller, timestamp);

            collector.emit(new Values(caller, timestamp, rate, cdr));
        }
    }
}