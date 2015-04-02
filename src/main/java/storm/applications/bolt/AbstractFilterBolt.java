package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.util.bloom.ODTDBloomFilter;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractFilterBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFilterBolt.class);
    
    protected ODTDBloomFilter filter;
    
    protected String configPrefix;
    protected String outputField;

    public AbstractFilterBolt(String configPrefix, String outputField) {
        this.configPrefix = configPrefix;
        this.outputField = outputField;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD);
    }

    @Override
    public void initialize() {
        int numElements       = config.getInt(String.format(Conf.FILTER_NUM_ELEMENTS, configPrefix));
        int bucketsPerElement = config.getInt(String.format(Conf.FILTER_BUCKETS_PEL, configPrefix));
        int bucketsPerWord    = config.getInt(String.format(Conf.FILTER_BUCKETS_PWR, configPrefix));
        double beta           = config.getDouble(String.format(Conf.FILTER_BETA, configPrefix));
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }
}