package org.storm.applications;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAMConstants {
    public static final String SPOUT_GENERATOR = "generator";
    public static final String SPOUT_FILE = "file";
    
    public static final String CDR_SPOUT = "CdrSpout";
    public static final String VARIATION_DETECTOR_BOLT = "VariationDetectorBolt";
    public static final String RCR_BOLT = "RCRFilterBolt";
    public static final String ECR_BOLT = "ECRFilterBolt";
    public static final String ENCR_BOLT = "ENCRFilterBolt";
    public static final String ECR24_BOLT = "ECR24FilterBolt";
    public static final String CT24_BOLT = "CT24FilterBolt";
    public static final String FOFIR_BOLT = "FoFiRModuleBolt";
    public static final String URL_BOLT = "URLModuleBolt";
    public static final String ACD_BOLT = "ACDModuleBolt";
    public static final String GLOBAL_ACD_BOLT = "GlobalACDModuleBolt";
    public static final String SCORER_BOLT = "ScorerBolt";
    public static final String FILE_SINK = "FileSink";
    
    public static final String BACKUP_STREAM = "BackupStream";
    
    //public static class Field {
    public static final String CALLING_NUM_FIELD = "callingNumber";
    public static final String CALLED_NUM_FIELD = "calledNumber";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String SCORE_FIELD = "score";
    public static final String RECORD_FIELD = "record";
    public static final String AVERAGE_FIELD = "average";
    public static final String CALLTIME_FIELD = "calltime";
    public static final String NEW_CALLEE_FIELD = "newCallee";
    public static final String RATE_FIELD = "rate";
    public static final String ANSWER_TIME_FIELD = "answerTime";
    //}
}
