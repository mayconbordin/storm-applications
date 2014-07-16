package storm.applications.constants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface VoIPSTREAMConstants extends BaseConstants {
    String PREFIX = "vs";
    
    interface Conf extends BaseConf {
        String GENERATOR_POPULATION = "vs.generator.population";
        String GENERATOR_ERROR_PROB = "vs.generator.error_prob";
        
        String FILTER_NUM_ELEMENTS = "vs.%s.num_elements";
        String FILTER_BUCKETS_PEL = "vs.%s.buckets_per_element";
        String FILTER_BUCKETS_PWR = "vs.%s.buckets_per_word";
        String FILTER_BETA = "vs.%s.beta";
        
        String SCORE_THRESHOLD_MIN = "vs.%s.threshold.min";
        String SCORE_THRESHOLD_MAX = "vs.%s.threshold.max";
        
        String ACD_DECAY_FACTOR = "vs.acd.decay_factor";
        
        String FOFIR_WEIGHT = "vs.fofir.weight";
        String URL_WEIGHT = "vs.url.weight";
        String ACD_WEIGHT = "vs.acd.weight";
        
        String VAR_DETECT_APROX_SIZE = "vs.variation.aprox_size";
        String VAR_DETECT_ERROR_RATE = "vs.variation.error_rate";
        
        String VAR_DETECT_THREADS = "voipstream.vardetect.threads";
        String ECR_THREADS = "voipstream.ecr.threads";
        String RCR_THREADS = "voipstream.rcr.threads";
        String ENCR_THREADS = "voipstream.encr.threads";
        String ECR24_THREADS = "voipstream.ecr24.threads";
        String CT24_THREADS = "voipstream.ct24.threads";
        String FOFIR_THREADS = "voipstream.fofir.threads";
        String URL_THREADS = "voipstream.url.threads";
        String ACD_THREADS = "voipstream.acd.threads";
        String SCORER_THREADS = "voipstream.scorer.threads";
    }
    
    interface Component extends BaseComponent {
        String VARIATION_DETECTOR = "VariationDetectorBolt";
        String RCR = "RCRFilterBolt";
        String ECR = "ECRFilterBolt";
        String ENCR = "ENCRFilterBolt";
        String ECR24 = "ECR24FilterBolt";
        String CT24 = "CT24FilterBolt";
        String FOFIR = "FoFiRModuleBolt";
        String URL = "URLModuleBolt";
        String ACD = "ACDModuleBolt";
        String GLOBAL_ACD = "GlobalACDModuleBolt";
        String SCORER = "ScorerBolt";
    }
    
    interface Field {
        String CALLING_NUM = "callingNumber";
        String CALLED_NUM = "calledNumber";
        String TIMESTAMP = "timestamp";
        String SCORE = "score";
        String RECORD = "record";
        String AVERAGE = "average";
        String CALLTIME = "calltime";
        String NEW_CALLEE = "newCallee";
        String RATE = "rate";
        String ANSWER_TIME = "answerTime";
    }
    
    interface Stream extends BaseStream {
        String BACKUP = "backupStream";
    }
}
