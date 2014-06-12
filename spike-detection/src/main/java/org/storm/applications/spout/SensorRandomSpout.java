package org.storm.applications.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Simulates a sensor that emits tuples with random values.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SensorRandomSpout extends backtype.storm.topology.base.BaseRichSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private int count = 1000000;  
    private String deviceID = "Arduino";

    private final Random random = new Random();

    public SensorRandomSpout() {
    }
    
    public SensorRandomSpout(String deviceID, int count) {
        this.deviceID = deviceID;
        this.count = count;
    }

    @Override
    public void open(final Map conf, final TopologyContext context, 
            final SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {                
        if (count-- > 0) {
            collector.emit(new Values(deviceID, (random.nextDouble() * 10) + 50));                        
        } else if (count-- == -1) {
            collector.emit(new Values(deviceID, -1.0));
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("deviceID", "value"));
    }
}