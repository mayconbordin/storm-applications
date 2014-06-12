package org.storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static backtype.storm.utils.Utils.DEFAULT_STREAM_ID;
import org.apache.log4j.Logger;
import static org.storm.applications.VoIPSTREAMConstants.*;
import org.storm.applications.cdr.CallDetailRecord;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class RCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = Logger.getLogger(RCRBolt.class);

    public RCRBolt() {
        super("rcr", RATE_FIELD);
    }
    
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        
        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis()/1000;
            
            if (input.getSourceStreamId().equals(DEFAULT_STREAM_ID)) {
                String callee = cdr.getCalledNumber();
                filter.add(callee, 1, timestamp);
            }

            else if (input.getSourceStreamId().equals(BACKUP_STREAM)) {
                String caller = cdr.getCallingNumber();
                double rcr = filter.estimateCount(caller, timestamp);
                
                collector.emit(new Values(caller, timestamp, rcr, cdr));
            }
        }
    }
}
