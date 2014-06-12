package org.storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import static org.storm.applications.VoIPSTREAMConstants.*;
import org.storm.applications.cdr.CallDetailRecord;

/**
 * Per-user total call time
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTBolt extends AbstractFilterBolt {
    private static final Logger LOG = Logger.getLogger(CTBolt.class);

    public CTBolt(String configPrefix) {
        super(configPrefix, CALLTIME_FIELD);
    }
    
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        boolean newCallee = input.getBooleanByField(NEW_CALLEE_FIELD);
        
        if (cdr.isCallEstablished() && newCallee) {
            String caller = input.getStringByField(CALLING_NUM_FIELD);
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            filter.add(caller, cdr.getCallDuration(), timestamp);
            double calltime = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("CallTime: %f", calltime));
            collector.emit(new Values(caller, timestamp, calltime, cdr));
        }
    }
}