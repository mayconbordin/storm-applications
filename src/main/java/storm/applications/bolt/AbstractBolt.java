package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.BaseConstants;
import storm.applications.util.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    protected String configPrefix = BaseConstants.PREFIX;
    private Map<String, Fields> fields;

    public AbstractBolt() {
        fields = new HashMap<>();
    }
    
    public void setFields(Fields fields) {
        this.fields.put(Utils.DEFAULT_STREAM_ID, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }
    
    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields.isEmpty()) {
            if (getDefaultFields() != null)
                fields.put(Utils.DEFAULT_STREAM_ID, getDefaultFields());
            
            if (getDefaultStreamFields() != null)
                fields.putAll(getDefaultStreamFields());
        }
        
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }
    
    public Fields getDefaultFields() {
        return null;
    }
    
    public Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config = new Configuration(stormConf);
        this.context = context;
        this.collector = collector;
        
        initialize();
    }

    public void initialize() {
        
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
}
