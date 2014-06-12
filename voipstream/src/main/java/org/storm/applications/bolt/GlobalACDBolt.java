package org.storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.apache.log4j.Logger;
import static org.storm.applications.VoIPSTREAMConstants.*;
import org.storm.applications.cdr.CallDetailRecord;
import org.storm.applications.util.ConfigUtility;
import org.streaminer.stream.avg.VariableEWMA;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(GlobalACDBolt.class);
    
    private OutputCollector collector;
    private VariableEWMA avgCallDuration;
    private double age;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP_FIELD, AVERAGE_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        age = ConfigUtility.getDouble(stormConf, "voipstream.acd.age"); //86400s = 24h
        avgCallDuration = new VariableEWMA(age);
    }

    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        long timestamp = cdr.getAnswerTime().getMillis()/1000;

        avgCallDuration.add(cdr.getCallDuration());
        collector.emit(new Values(timestamp, avgCallDuration.getAverage()));
    }
}