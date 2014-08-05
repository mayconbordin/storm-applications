package storm.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ScoreBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ScoreBolt.class);

    private double[] weights;

    public ScoreBolt() {
        super(null);
    }

    @Override
    public void initialize() {
        super.initialize();
                
        // parameters
        double fofirWeight = config.getDouble(Conf.FOFIR_WEIGHT);
        double urlWeight   = config.getDouble(Conf.URL_WEIGHT);
        double acdWeight   = config.getDouble(Conf.ACD_WEIGHT);
        
        weights = new double[3];
        weights[0] = fofirWeight;
        weights[1] = urlWeight;
        weights[2] = acdWeight;
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        Source src     = parseComponentId(input.getSourceComponent());
        String caller  = cdr.getCallingNumber();
        long timestamp = cdr.getAnswerTime().getMillis()/1000;
        double score   = input.getDouble(2);
        String key     = String.format("%s:%d", caller, timestamp);
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            
            if (e.isFull()) {
                double mainScore = sum(e.getValues(), weights);
                
                LOG.debug(String.format("Score=%f; Scores=%s", mainScore, Arrays.toString(e.getValues())));
                
                collector.emit(new Values(caller, timestamp, mainScore, cdr));
            } else {
                e.set(src, score);
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, score);
            map.put(key, e);
        }
    }
    
    /**
     * Computes weighted sum of a given sequence. 
     * @param data data array
     * @param weights weights
     * @return weighted sum of the data 
     */
    private static double sum(double[] data, double[] weights) {
        double sum = 0.0;

        for (int i=0; i<data.length; i++) {
            sum += (data[i] * weights[i]);
        }
        
        return sum;
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.FOFIR, Source.URL, Source.ACD};
    }
}
