package storm.applications.bolt;

import storm.applications.model.log.LogEntry;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.util.Map;
import storm.applications.constants.LogProcessingConstants.Field;

/**
 * This class will parse the json events coming from spout and emits values as 'LogEntry' object
 */
public class LogParserBolt extends BaseRichBolt {
    public static Logger LOG = Logger.getLogger(LogParserBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // LOG.info(String.valueOf(tuple.getValue(0)));
        JSONObject obj=(JSONObject) JSONValue.parse(String.valueOf(tuple.getValue(0)));
        LogEntry entry = new LogEntry(obj);
        collector.emit(new Values(entry));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.LOG_ENTRY));
    }
}
