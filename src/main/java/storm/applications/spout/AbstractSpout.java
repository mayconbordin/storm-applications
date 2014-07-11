package storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractSpout extends BaseRichSpout {
    protected Map config;
    protected SpoutOutputCollector collector;
    protected Map<String, Fields> fields;

    public AbstractSpout() {
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
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.config = conf;
        this.collector = collector;
        
        initialize();
    }

    protected abstract void initialize();
}
