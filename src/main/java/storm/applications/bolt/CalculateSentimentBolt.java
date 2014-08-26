package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.SentimentAnalysisConstants.Conf;
import storm.applications.constants.SentimentAnalysisConstants.Field;
import storm.applications.model.sentiment.SentimentClassifier;
import storm.applications.model.sentiment.SentimentClassifierFactory;
import storm.applications.model.sentiment.SentimentResult;

/**
 * Breaks each tweet into words and calculates the sentiment of each tweet and associates the sentiment value to the State
 * and logs the same to the console and also logs to the file.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class CalculateSentimentBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSentimentBolt.class);

    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
                                                                             .withLocale(Locale.ENGLISH);
    
    private SentimentClassifier classifier;

    @Override
    public void initialize() {
        String classifierType = config.getString(Conf.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.TEXT, Field.TIMESTAMP, Field.SENTIMENT, Field.SCORE);
    }

    @Override
    public void execute(Tuple input) {
        Map tweet = (Map) input.getValueByField(Field.TWEET);
        
        if (!tweet.containsKey("id_str") || !tweet.containsKey("text") || !tweet.containsKey("created_at"))
            return;
        
        String tweetId     = (String) tweet.get("id_str");
        String text        = (String) tweet.get("text");
        DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));
        
        SentimentResult result = classifier.classify(text);
        
        collector.emit(input, new Values(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore()));
        collector.ack(input);
    }
}