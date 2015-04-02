package storm.applications.bolt;

import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.bloom.BloomFilter;

public class VariationDetectorBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VariationDetectorBolt.class);
    
    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.NEW_CALLEE, Field.RECORD);
        
        streams.put(Stream.DEFAULT, fields);
        streams.put(Stream.BACKUP, fields);
        
        return streams;
    }

    @Override
    public void initialize() {
        approxInsertSize = config.getInt(Conf.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getDouble(Conf.VAR_DETECT_ERROR_RATE);
        
        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner  = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        
        cycleThreshold = detector.size()/Math.sqrt(2);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
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
        
        Values values = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(), 
                cdr.getAnswerTime(), newCallee, cdr);
        
        collector.emit(values);
        collector.emit(Stream.BACKUP, values);
    }
    
    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}