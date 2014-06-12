package org.storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import static org.storm.applications.CreditCardFraudConstants.*;
import org.storm.applications.predictor.MarkovModelPredictor;
import org.storm.applications.predictor.ModelBasedPredictor;
import org.storm.applications.predictor.Prediction;

/**
 *
 * @author maycon
 */
public class PredictorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ModelBasedPredictor predictor;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String strategy = stormConf.get("predictor.model").toString();
        
        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(stormConf);
        }
    }

    public void execute(Tuple input) {
        String entityID = input.getString(0);
        String record  = input.getString(1);
        Prediction p = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier())
            collector.emit(new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));

        //ack
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ENTITY_ID_FIELD, SCORE_FIELD, STATES_FIELD));
    }
}