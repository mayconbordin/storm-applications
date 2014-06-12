package org.storm.applications.bolt;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static org.storm.applications.VoIPSTREAMConstants.*;
import org.storm.applications.cdr.CallDetailRecord;
import org.storm.applications.util.ConfigUtility;
import org.streaminer.stream.membership.BloomFilterAlt;

public class VariationDetectorBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(VariationDetectorBolt.class);
    
    private OutputCollector collector;
    private BloomFilterAlt<String> detector;
    private BloomFilterAlt<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(CALLING_NUM_FIELD, CALLED_NUM_FIELD, ANSWER_TIME_FIELD,
                NEW_CALLEE_FIELD, RECORD_FIELD);
        
        declarer.declare(fields);
        declarer.declareStream(BACKUP_STREAM, fields);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        approxInsertSize = ConfigUtility.getInt(stormConf, "voipstream.variation.aprox_size");
        falsePostiveRate = ConfigUtility.getDouble(stormConf, "voipstream.variation.error_rate");
        
        detector = new BloomFilterAlt<String>(falsePostiveRate, approxInsertSize);
        learner  = new BloomFilterAlt<String>(falsePostiveRate, approxInsertSize);
        
        cycleThreshold = detector.size()/Math.sqrt(2);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
        boolean newCallee = false;
        
        // add pair to learner
        learner.add(key);
        
        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }
        
        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }
        
        Values v = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(), 
                cdr.getAnswerTime(), newCallee, cdr);
        
        collector.emit(v);
        collector.emit(BACKUP_STREAM, v);
    }
    
    private void rotateFilters() {
        BloomFilterAlt<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}