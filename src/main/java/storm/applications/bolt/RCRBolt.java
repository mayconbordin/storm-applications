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
public class RCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RCRBolt.class);

    public RCRBolt() {
        super("rcr", Field.RATE);
    }
    
    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        
        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis()/1000;
            
            if (input.getSourceStreamId().equals(Stream.DEFAULT)) {
                String callee = cdr.getCalledNumber();
                filter.add(callee, 1, timestamp);
            }

            else if (input.getSourceStreamId().equals(Stream.BACKUP)) {
                String caller = cdr.getCallingNumber();
                double rcr = filter.estimateCount(caller, timestamp);
                
                collector.emit(new Values(caller, timestamp, rcr, cdr));
            }
        }
    }
}
