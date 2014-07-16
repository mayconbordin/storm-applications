package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.MutableLong;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.WordCountConstants.Field;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, MutableLong> counts = new HashMap<String, MutableLong>();

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.WORD, Field.COUNT));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        MutableLong count = counts.get(word);
        
        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();
        
        collector.emit(new Values(word, count.get()));
    }
    
}
