package storm.applications.bolt;

import java.util.HashMap;
import java.util.Map;

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
public class DataStreamAnomalyScoreBolt<T> extends AbstractBolt {
    private Map<String, StreamProfile<T>> streamProfiles;
    private double lambda;
    private double factor;
    private double threashold;
    private boolean shrinkNextRound;
    private long previousTimestamp;

    @Override
    public void initialize() {
        this.lambda = config.getDouble(Conf.ANOMALY_SCORER_LAMBDA);
        this.factor = Math.pow(Math.E, -lambda);
        this.threashold = 1 / (1 - factor) * 0.5;
        this.shrinkNextRound = false;
        this.streamProfiles = new HashMap<>();
        this.previousTimestamp = 0;
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(Field.TIMESTAMP);

        if (timestamp > previousTimestamp) {            
            for (Map.Entry<String, StreamProfile<T>> streamProfileEntry : streamProfiles.entrySet()) {
                StreamProfile<T> streamProfile = streamProfileEntry.getValue();
                if (shrinkNextRound == true) {
                    streamProfile.streamAnomalyScore = 0;
                }
                
                collector.emit(new Values(streamProfileEntry.getKey(), 
                        streamProfile.streamAnomalyScore, previousTimestamp, 
                        streamProfile.currentDataInstance, 
                        streamProfile.currentDataInstanceScore));
            }
            
            if (shrinkNextRound == true) {
                shrinkNextRound = false;
            }
            
            previousTimestamp = timestamp;
        }

        String id = input.getString(0);
        StreamProfile<T> profile = streamProfiles.get(id);
        double dataInstanceAnomalyScore = input.getDouble(1);
        
        if (profile == null) {
            profile = new StreamProfile<>(id, (T)input.getValue(3),
                    dataInstanceAnomalyScore, input.getDouble(1));
            
            streamProfiles.put(id, profile);
        } else {
            //	update stream score
            profile.streamAnomalyScore = profile.streamAnomalyScore * factor + dataInstanceAnomalyScore;
            profile.currentDataInstance = (T)input.getValue(3);
            profile.currentDataInstanceScore = dataInstanceAnomalyScore;
            if (profile.streamAnomalyScore > threashold) {
                shrinkNextRound = true;
            }
            streamProfiles.put(id, profile);
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.STREAM_ANOMALY_SCORE, Field.TIMESTAMP, Field.OBSERVATION, Field.CUR_DATAINST_SCORE);		
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
