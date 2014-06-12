package org.storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import static org.storm.applications.VoIPSTREAMConstants.*;
import org.storm.applications.cdr.CallDetailRecord;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ECRBolt extends AbstractFilterBolt {
    private static final Logger LOG = Logger.getLogger(ECRBolt.class);

    public ECRBolt(String configPrefix) {
        super(configPrefix, RATE_FIELD);
    }

    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        
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
