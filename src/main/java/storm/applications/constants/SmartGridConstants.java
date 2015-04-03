package storm.applications.constants;

/**
 *
 * @author mayconbordin
 */
public interface SmartGridConstants extends BaseConstants {
    String PREFIX = "sg";

    interface Conf extends BaseConf {
        String SLICE_LENGTH = "sg.slice.length";
        
        String SLIDING_WINDOW_THREADS   = "sg.sliding_window.threads";
        String GLOBAL_MEDIAN_THREADS    = "sg.global_median.threads";
        String PLUG_MEDIAN_THREADS      = "sg.plug_median.threads";
        String HOUSE_LOAD_THREADS       = "sg.house_load.threads";
        String PLUG_LOAD_THREADS        = "sg.plug_load.threads";
        String OUTLIER_DETECTOR_THREADS = "sg.outlier_detector.threads";
        
        String HOUSE_LOAD_FREQUENCY     = "sg.house_load.frequency";
        String PLUG_LOAD_FREQUENCY      = "sg.plug_load.frequency";
        
        
        // generator configs
        String GENERATOR_INTERVAL_SECONDS = "sg.generator.interval_seconds";
        String GENERATOR_NUM_HOUSES       = "sg.generator.houses.num";
        String GENERATOR_HOUSEHOLDS_MIN   = "sg.generator.households.min";
        String GENERATOR_HOUSEHOLDS_MAX   = "sg.generator.households.max";
        String GENERATOR_PLUGS_MIN        = "sg.generator.plugs.min";
        String GENERATOR_PLUGS_MAX        = "sg.generator.plugs.max";
        String GENERATOR_LOADS            = "sg.generator.load.list";
        String GENERATOR_LOAD_OSCILLATION = "sg.generator.load.oscillation";
        String GENERATOR_PROBABILITY_ON   = "sg.generator.on.probability";
        String GENERATOR_ON_LENGTHS       = "sg.generator.on.lengths";
    }
    
    interface Component extends BaseComponent {
        String SLIDING_WINDOW   = "slidingWindow";
        String GLOBAL_MEDIAN    = "globalMedianCalculator";
        String PLUG_MEDIAN      = "plugMedianCalculator";
        String HOUSE_LOAD       = "houseLoadPredictor";
        String PLUG_LOAD        = "plugLoadPredictor";
        String OUTLIER_DETECTOR = "outlierDetector";
        String OUTLIER_SINK     = "outlierSink";
        String PREDICTION_SINK  = "predictionSink";
    }
    
    interface Stream extends BaseStream {
        
    }
    
    interface Field {
        String ID           = "id";
        String TIMESTAMP    = "timestamp";
        String VALUE        = "value";
        String PROPERTY     = "property";
        String PLUG_ID      = "plugId";
        String HOUSEHOLD_ID = "householdId";
        String HOUSE_ID     = "houseId";
        String GLOBAL_MEDIAN_LOAD = "globalMedianLoad";
        String SLIDING_WINDOW_ACTION = "slidingWindowAction";
        String PLUG_SPECIFIC_KEY = "plugSpecificKey";
        String PER_PLUG_MEDIAN = "perPlugMedian";
        
        String SLIDING_WINDOW_START = "slidingWindowStart";
        String SLIDING_WINDOW_END   = "slidingWindowEnd";
        String OUTLIER_PERCENTAGE   = "outlierPercentage";
        
        String PREDICTED_LOAD = "predictedLoad";
    }
    
    interface Measurement {
        int WORK = 0;
        int LOAD = 1;
    }
    
    interface SlidingWindowAction {
        int ADD = 1;
        int REMOVE = -1;
    }
}
