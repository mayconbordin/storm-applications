package org.storm.applications.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class MovingAverageBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(MovingAverageBolt.class);
    
    private int movingAverageWindow = 1000;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap = new HashMap<String, LinkedList<Double>>();
    private Map<String, Double> deviceIDtoSumOfEvents = new HashMap<String, Double>();

    public MovingAverageBolt() {

    }

    public MovingAverageBolt(int movingAverageWindow) {
        this.movingAverageWindow = movingAverageWindow;
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        final String deviceID = tuple.getString(0);
        final double nextDouble = (double)tuple.getInteger(1);
        double movingAvergeInstant = movingAverage(deviceID, nextDouble);
        
        LOG.info(movingAvergeInstant + " : " + nextDouble);
        collector.emit(new Values(deviceID, movingAvergeInstant, nextDouble));
    }

    public double movingAverage(String deviceID, double nextDouble) {
        LinkedList<Double> valueList = new LinkedList<Double>();
        double sum = 0.0;
        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow-1) {
                double valueToRemove = valueList.removeFirst();			
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum/valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("deviceID", "movingAverage", "value"));
    }
}