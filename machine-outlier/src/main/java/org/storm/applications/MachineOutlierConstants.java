package org.storm.applications;

public class MachineOutlierConstants {
    public static final String METADATA_SPOUT = "metadataSpout";
    public static final String SCORER_BOLT = "scorerBolt";
    public static final String ANOMALY_SCORER_BOLT = "anomalyScorerBolt";
    public static final String ALERT_TRIGGER_BOLT = "alertTriggerBolt";
    public static final String FILE_SINK = "fileSink";
    
    public static final String ID_FIELD = "id";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String IP_FIELD = "ip";
    public static final String ENTITY_ID_FIELD = "entityID";
    public static final String METADATA_FIELD = "metadata";
    public static final String DATAINST_ANOMALY_SCORE_FIELD = "dataInstanceAnomalyScore";
    public static final String DATAINST_SCORE_FIELD = "dataInstanceScore";
    public static final String CUR_DATAINST_SCORE_FIELD = "curDataInstanceScore";
    public static final String STREAM_ANOMALY_SCORE_FIELD = "streamAnomalyScore";
    public static final String OBSERVATION_FIELD = "observation";
    public static final String ANOMALY_STREAM_FIELD = "anomalyStream";
    public static final String IS_ABNORMAL_FIELD = "isAbnormal";
}
