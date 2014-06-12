package org.storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import redis.clients.jedis.Jedis;

import java.util.Map;
import static org.storm.applications.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class RepeatVisitBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Void> map;

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        map = new HashMap<String, Void>();
    }

    @Override
    public void execute(Tuple tuple) {
        String ip = tuple.getStringByField(IP_FIELD);
        String clientKey = tuple.getStringByField(CLIENT_KEY_FIELD);
        String url = tuple.getStringByField(URL_FIELD);
        String key = url + ":" + clientKey;
        
        if (map.containsKey(key)) {
             collector.emit(new Values(clientKey, Boolean.FALSE.toString()));
        } else {
            map.put(key, null);
            collector.emit(new Values(clientKey, Boolean.TRUE.toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new backtype.storm.tuple.Fields(CLIENT_KEY_FIELD, URL_FIELD, UNIQUE_FIELD));
    }
}
