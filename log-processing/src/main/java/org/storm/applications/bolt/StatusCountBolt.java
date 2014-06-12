package org.storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.storm.applications.LogProcessingConstants.Field;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountBolt  extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCountBolt.class);
    private OutputCollector collector;
    private Map<String, Integer> counts;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Integer>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String statusCode = tuple.getStringByField(Field.LOG_STATUS_CODE);
        int count = 0;
        
        if (this.counts.containsKey(statusCode)) {
            count = this.counts.get(statusCode);
        }
        count ++;
        this.counts.put(statusCode, count);
        collector.emit(new Values(statusCode, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.LOG_STATUS_CODE, Field.STATUS_CODE_COUNT));
    }
}
