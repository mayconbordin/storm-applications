package org.storm.applications.sink;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.storm.applications.bolt.BasicBolt;


public abstract class BaseSink extends BasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }
}
