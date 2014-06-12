package org.storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.storm.applications.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class RedisClickSpout extends BaseRichSpout {
    public static final Logger LOG = LoggerFactory.getLogger(RedisClickSpout.class);

    private Jedis jedis;
    private String host;
    private int port;
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(IP_FIELD, URL_FIELD, CLIENT_KEY_FIELD));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        host = conf.get("clickanalytics.redis.host").toString();
        port = Integer.valueOf(conf.get("clickanalytics.redis.port").toString());
        this.collector = spoutOutputCollector;
        connectToRedis();
    }

    private void connectToRedis(){
        jedis = new Jedis(host, port);
    }

    @Override
    public void nextTuple() {
        String content = jedis.rpop("count");
        
        if (content == null || "nil".equals(content)){
            try{ Thread.sleep(300);}catch(InterruptedException e){}
        } else {
            JSONObject obj = (JSONObject) JSONValue.parse(content);
            String ip = obj.get(IP_FIELD).toString();
            String url = obj.get(URL_FIELD).toString();
            String clientKey = obj.get(CLIENT_KEY_FIELD).toString();
            collector.emit(new Values(ip, url, clientKey));
        }
    }
}
