package storm.applications.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;
import storm.applications.model.scorer.DataInstanceScorer;
import storm.applications.model.scorer.DataInstanceScorerFactory;
import storm.applications.model.scorer.ScorePackage;

public class ObservationScoreBolt extends BaseRichBolt{
    private long previousTimestamp;
    private String dataTypeName;
    private OutputCollector collector;
    private DataInstanceScorer dataInstanceScorer;
    private List<Object> observationList;

    public ObservationScoreBolt(String dataTypeName) {
        this.previousTimestamp = 0;
        this.dataTypeName = dataTypeName;
        this.observationList = new ArrayList<Object>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLong(0);
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit 
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    collector.emit(new Values(scorePackage.getId(), scorePackage.getScore(), 
                            previousTimestamp, scorePackage.getObj()));
                }
                observationList.clear();
            }

            previousTimestamp = timestamp;
        }

        observationList.add(input.getValue(2));
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID_FIELD, DATAINST_ANOMALY_SCORE_FIELD,
                TIMESTAMP_FIELD, OBSERVATION_FIELD));
    }

    private void print(List<ScorePackage> scorePackageList) {
        for(int i = 0; i < 15; ++i) {
            System.out.println();
        }

        System.out.println(previousTimestamp + "\t" + observationList.size());

        for(ScorePackage pack : scorePackageList) {
            System.out.println(pack.getId() + "\t" + pack.getScore());
        }

        for(int i = 0; i < 15; ++i) {
            System.out.println();
        }
    }
}
