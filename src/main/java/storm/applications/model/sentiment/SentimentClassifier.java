package storm.applications.model.sentiment;

import storm.applications.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
