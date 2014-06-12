package org.storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.storm.applications.SpamFilterConstants.Component;
import org.storm.applications.SpamFilterConstants.Field;
import org.storm.applications.SpamFilterConstants.Stream;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ParserBolt extends BaseRichBolt {
    private OutputCollector collector;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Stream.TRAINING, new Fields(Field.ID, Field.MESSAGE, Field.IS_SPAM));
        declarer.declareStream(Stream.ANALYSIS, new Fields(Field.ID, Field.MESSAGE));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        JSONObject obj = (JSONObject) JSONValue.parse(String.valueOf(input.getValue(0)));
        
        if (input.getSourceComponent().equals(Component.TRAINING_SPOUT))
            collector.emit(Stream.TRAINING, new Values(obj.get(Field.ID), obj.get(Field.MESSAGE), obj.get(Field.IS_SPAM)));
        else if (input.getSourceComponent().equals(Component.ANALYSIS_SPOUT))
            collector.emit(Stream.ANALYSIS, new Values(obj.get(Field.ID), obj.get(Field.MESSAGE)));
    }
    
}
