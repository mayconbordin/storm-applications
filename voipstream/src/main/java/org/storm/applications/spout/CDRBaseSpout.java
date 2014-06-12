package org.storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.storm.applications.cdr.CallDetailRecord;
import java.util.Map;

/**
 *
 * @author maycon
 */
public abstract class CDRBaseSpout extends BaseRichSpout {
    protected SpoutOutputCollector outputCollector;

    public CDRBaseSpout() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("callingNumber", "calledNumber", "answerTime", "record"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void nextTuple() {
        CallDetailRecord cdr = nextRecord();
        Values values = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(), 
                cdr.getAnswerTime(), cdr);
        
        outputCollector.emit(values);
    }
    
    public abstract CallDetailRecord nextRecord();
    
    
}
