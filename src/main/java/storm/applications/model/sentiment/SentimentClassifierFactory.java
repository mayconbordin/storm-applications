package storm.applications.model.sentiment;

import storm.applications.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public class SentimentClassifierFactory {
    public static final String LINGPIPE = "lingpipe";
    public static final String BASIC    = "basic";
    
    public static SentimentClassifier create(String classifierName, Configuration config) {
        SentimentClassifier classifier;
        
        if (classifierName.equals(BASIC)) {
            classifier = new BasicClassifier();
        } else if (classifierName.equals(LINGPIPE)) {
            classifier = new LingPipeClassifier();
        } else {
            throw new IllegalArgumentException("There is not sentiment classifier named " + classifierName);
        }
        
        classifier.initialize(config);
        
        return classifier;
    }
}
