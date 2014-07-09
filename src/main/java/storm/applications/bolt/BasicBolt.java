package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class BasicBolt extends BaseRichBolt {
    protected OutputCollector collector;
    protected Map stormConf;
    protected TopologyContext context;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);
    public abstract void execute(Tuple input);
    
}
