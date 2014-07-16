package storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ECRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ECRBolt.class);

    public ECRBolt(String configPrefix) {
        super(configPrefix, Field.RATE);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        
        if (cdr.isCallEstablished()) {
            String caller  = cdr.getCallingNumber();
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);

            collector.emit(new Values(caller, timestamp, ecr, cdr));
        }
    }
}
