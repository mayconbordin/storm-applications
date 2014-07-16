package storm.applications.constants;

public interface MachineOutlierConstants extends BaseConstants {
    String PREFIX = "mo";
    
    interface Conf extends BaseConf {
        String GENERATOR_NUM_MACHINES = "mo.generator.num_machines";
        
        String SCORER_THREADS               = "mo.scorer.threads";
        String SCORER_DATA_TYPE             = "mo.scorer.data_type";
        String ANOMALY_SCORER_THREADS       = "mo.anomaly_scorer.threads";
        String ANOMALY_SCORER_WINDOW_LENGTH = "mo.anomaly_scorer.window_length";
        String ANOMALY_SCORER_LAMBDA        = "mo.anomaly_scorer.lambda";
        String ALERT_TRIGGER_THREADS        = "mo.alert_trigger.threads";
        String ALERT_TRIGGER_TOPK           = "mo.alert_trigger.topk";
    }
    
    interface Component extends BaseComponent {
        String SCORER = "scorerBolt";
        String ANOMALY_SCORER = "anomalyScorerBolt";
        String ALERT_TRIGGER = "alertTriggerBolt";
    }
    
    interface Field {
        String ID = "id";
        String TIMESTAMP = "timestamp";
        String IP = "ip";
        String ENTITY_ID = "entityID";
        String METADATA = "metadata";
        String DATAINST_ANOMALY_SCORE = "dataInstanceAnomalyScore";
        String DATAINST_SCORE = "dataInstanceScore";
        String CUR_DATAINST_SCORE = "curDataInstanceScore";
        String STREAM_ANOMALY_SCORE = "streamAnomalyScore";
        String OBSERVATION = "observation";
        String ANOMALY_STREAM = "anomalyStream";
        String IS_ABNORMAL = "isAbnormal";
    }
    
    
}
