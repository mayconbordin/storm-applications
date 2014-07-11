package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected OutputCollector collector;
    protected Map config;
    protected TopologyContext context;
    protected Map<String, Fields> fields;

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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config = stormConf;
        this.context = context;
        this.collector = collector;
        
        initialize();
    }

    public abstract void initialize();
}
