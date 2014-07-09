package storm.applications.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.MachineOutlierConstants.*;

/**
 * DataStreamAnomalyScoreBolt keeps and update the stream anomaly score for each stream.
 * @author yexijiang
 * @param <T>
 *
 */
public class DataStreamAnomalyScoreBolt<T> extends BaseRichBolt {
    private Map<String, StreamProfile<T>> streamProfiles;
    private OutputCollector collector;
    private double lambda;
    private double factor;
    private double threashold;
    private boolean shrinkNextRound;
    private long previousTimestamp;

    public DataStreamAnomalyScoreBolt() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.lambda = Double.parseDouble(stormConf.get("lambda").toString());
        this.factor = Math.pow(Math.E, -lambda);
        this.threashold = 1 / (1 - factor) * 0.5;
        this.shrinkNextRound = false;
        this.streamProfiles = new HashMap<String, StreamProfile<T>>();
        this.previousTimestamp = 0;
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLong(2);

        if (timestamp > this.previousTimestamp) {
            String alertMessage = "" + this.previousTimestamp + "\n";
            
            for (Map.Entry<String, StreamProfile<T>> streamProfileEntry : this.streamProfiles.entrySet()) {
                StreamProfile<T> streamProfile = streamProfileEntry.getValue();
                if (this.shrinkNextRound == true) {
                    streamProfile.streamAnomalyScore = 0;
                }
                
                this.collector.emit(new Values(streamProfileEntry.getKey(), 
                        streamProfile.streamAnomalyScore, this.previousTimestamp, 
                        streamProfile.currentDataInstance, 
                        streamProfile.currentDataInstanceScore));
            }
            
            if (this.shrinkNextRound == true) {
                this.shrinkNextRound = false;
            }
            
            this.previousTimestamp = timestamp;
        }

        String id = input.getString(0);
        StreamProfile<T> profile = this.streamProfiles.get(id);
        double dataInstanceAnomalyScore = input.getDouble(1);
        
        if (profile == null) {
            profile = new StreamProfile<T>(id, (T)input.getValue(3),
                    dataInstanceAnomalyScore, input.getDouble(1));
            
            this.streamProfiles.put(id, profile);
        } else {
            //	update stream score
            profile.streamAnomalyScore = profile.streamAnomalyScore * factor + dataInstanceAnomalyScore;
            profile.currentDataInstance = (T)input.getValue(3);
            profile.currentDataInstanceScore = dataInstanceAnomalyScore;
            if (profile.streamAnomalyScore > this.threashold) {
                this.shrinkNextRound = true;
            }
            this.streamProfiles.put(id, profile);
        }
        
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID_FIELD, STREAM_ANOMALY_SCORE_FIELD, TIMESTAMP_FIELD,
                OBSERVATION_FIELD, CUR_DATAINST_SCORE_FIELD));		
    }

    private void print() {
        for (int i = 0; i < 15; ++i) {
            System.out.println();
        }

        for (Map.Entry<String, StreamProfile<T>> entry : streamProfiles.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue().streamAnomalyScore);
        }

        for (int i = 0; i < 15; ++i) {
            System.out.println();
        }
    }

    /**
     * Keeps the profile of the stream.
     * @author yexijiang
     *
     * @param <T>
     */
    class StreamProfile<T> {
        String id;
        double streamAnomalyScore;
        T currentDataInstance;
        double currentDataInstanceScore;

        public StreamProfile(String id, T dataInstanceScore, double initialAnomalyScore, double currentDataInstanceScore) {
            this.id = id;
            this.streamAnomalyScore = initialAnomalyScore;
            this.currentDataInstance = dataInstanceScore;
            this.currentDataInstanceScore = currentDataInstanceScore;
        }
    }
}
