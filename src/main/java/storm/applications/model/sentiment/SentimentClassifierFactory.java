package storm.applications.model.sentiment;

import storm.applications.util.config.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SentimentClassifierFactory {
    public static final String LINGPIPE = "lingpipe";
    public static final String BASIC    = "basic";
    
    public static SentimentClassifier create(String classifierName, Configuration config) {
        SentimentClassifier classifier;
        
        switch (classifierName) {
            case BASIC:
                classifier = new BasicClassifier();
                break;
            case LINGPIPE:
                classifier = new LingPipeClassifier();
                break;
            default:
                throw new IllegalArgumentException("There is not sentiment classifier named " + classifierName);
        }
        
        classifier.initialize(config);
        
        return classifier;
    }
}
