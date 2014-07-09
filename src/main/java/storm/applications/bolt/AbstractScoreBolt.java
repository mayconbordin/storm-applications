package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.ConfigUtility;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractScoreBolt extends BaseRichBolt {
    protected static enum Source {
        ECR, RCR, ECR24, ENCR, CT24, VD, FOFIR, ACD, GACD, URL, NONE
    }
    
    protected OutputCollector collector;
    protected double thresholdMin;
    protected double thresholdMax;
    protected String configPrefix;
    protected Map<String, Entry> map;

    public AbstractScoreBolt(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(CALLING_NUM_FIELD, TIMESTAMP_FIELD, SCORE_FIELD, RECORD_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        map = new HashMap<String, Entry>();

        // parameters
        if (configPrefix != null) {
            thresholdMin = ConfigUtility.getDouble(stormConf, "voipstream." + configPrefix + ".threshold.min");
            thresholdMax = ConfigUtility.getDouble(stormConf, "voipstream." + configPrefix + ".threshold.max");
        }
    }
    
    protected abstract Source[] getFields();
    
    protected static Source parseComponentId(String id) {
        if (id.equals(VARIATION_DETECTOR_BOLT))
            return Source.VD;
        else if (id.equals(ECR24_BOLT))
            return Source.ECR24;
        else if (id.equals(CT24_BOLT))
            return Source.CT24;
        else if (id.equals(ECR_BOLT))
            return Source.ECR;
        else if (id.equals(RCR_BOLT))
            return Source.RCR;
        else if (id.equals(ENCR_BOLT))
            return Source.ENCR;
        else if (id.equals(ACD_BOLT))
            return Source.ACD;
        else if (id.equals(GLOBAL_ACD_BOLT))
            return Source.GACD;
        else if (id.equals(URL_BOLT))
            return Source.URL;
        else if (id.equals(FOFIR_BOLT))
            return Source.FOFIR;
        else
            return Source.NONE;
    }
    
    protected static double score(double v1, double v2, double vi) {
        double score = vi/(v1 + (v2-v1));
        if (score < 0) score = 0; 
        if (score > 1) score = 1;
        return score;
    }
    
    protected class Entry {
        public CallDetailRecord cdr;
        
        public Source[] fields;
        public double[] values;

        public Entry(CallDetailRecord cdr) {
            this.cdr = cdr;
            this.fields = getFields();
            
            values = new double[fields.length];
            Arrays.fill(values, Double.NaN);
        }

        public void set(Source src, double rate) {
            values[pos(src)] = rate;
        }
        
        public double get(Source src) {
            return values[pos(src)];
        }
        
        public boolean isFull() {
            for (double value : values)
                if (Double.isNaN(value))
                    return false;
            return true;
        }
        
        private int pos(Source src) {
            for (int i=0; i<fields.length; i++)
                if (fields[i] == src)
                    return i;
            return -1;
        }

        public double[] getValues() {
            return values;
        }

        @Override
        public String toString() {
            return "Entry{" + "cdr=" + cdr + ", fields=" + Arrays.toString(fields) + ", values=" + Arrays.toString(values) + '}';
        }

    }
}