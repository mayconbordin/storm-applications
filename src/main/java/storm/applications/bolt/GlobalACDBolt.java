package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.VoIPSTREAMConstants.*;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.ConfigUtility;
import storm.applications.util.VariableEWMA;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACDBolt.class);
    
    private OutputCollector collector;
    private VariableEWMA avgCallDuration;
    private double age;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP_FIELD, AVERAGE_FIELD));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        age = ConfigUtility.getDouble(stormConf, "voipstream.acd.age"); //86400s = 24h
        avgCallDuration = new VariableEWMA(age);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(RECORD_FIELD);
        long timestamp = cdr.getAnswerTime().getMillis()/1000;

        avgCallDuration.add(cdr.getCallDuration());
        collector.emit(new Values(timestamp, avgCallDuration.getAverage()));
    }
}