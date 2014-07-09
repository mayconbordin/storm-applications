package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.ConfigUtility;

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
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
                
        // parameters
        double fofirWeight = ConfigUtility.getDouble(stormConf, "voipstream.fofir.weight");
        double urlWeight   = ConfigUtility.getDouble(stormConf, "voipstream.url.weight");
        double acdWeight   = ConfigUtility.getDouble(stormConf, "voipstream.acd.weight");
        
        weights = new double[3];
        weights[0] = fofirWeight;
        weights[1] = urlWeight;
        weights[2] = acdWeight;
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        Source src     = parseComponentId(input.getSourceComponent());
        String caller  = cdr.getCallingNumber();
        long timestamp = cdr.getAnswerTime().getMillis()/1000;
        double score   = input.getDouble(2);
        String key     = String.format("%s:%d", caller, timestamp);
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            
            if (e.isFull()) {
                double mainScore = sum(e.getValues(), weights);
                
                LOG.info(String.format("Score=%f; Scores=%s", mainScore, Arrays.toString(e.getValues())));
                
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
