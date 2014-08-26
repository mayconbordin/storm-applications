package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.mutable.MutableInt;
import storm.applications.constants.SpamFilterConstants.*;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TokenizerBolt extends AbstractBolt {
    private static final String splitregex = "\\W";
    private static final Pattern wordregex = Pattern.compile("\\w+");
    
    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        streams.put(Stream.TRAINING, new Fields(Field.WORD, Field.COUNT, Field.IS_SPAM));
        streams.put(Stream.TRAINING_SUM, new Fields(Field.SPAM_TOTAL, Field.HAM_TOTAL));
        streams.put(Stream.ANALYSIS, new Fields(Field.ID, Field.WORD, Field.NUM_WORDS));
        return streams;
    }

    @Override
    public void execute(Tuple input) {
        String content = input.getStringByField(Field.MESSAGE);

        if (input.getSourceComponent().equals(Component.TRAINING_SPOUT)) {
            boolean isSpam = input.getBooleanByField(Field.IS_SPAM);
            
            Map<String, MutableInt> words = tokenize(content);
            int spamTotal = 0, hamTotal = 0;

            for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue().toInteger();
                
                if (isSpam) {
                    spamTotal += count;
                } else {
                    hamTotal += count;
                }
                
                collector.emit(Stream.TRAINING, input, new Values(word, count, isSpam));
            }
            
            collector.emit(Stream.TRAINING_SUM, input, new Values(spamTotal, hamTotal));
        }
        
        else if (input.getSourceComponent().equals(Component.ANALYSIS_SPOUT)) {
            String id = input.getStringByField(Field.ID);
            
            Map<String, MutableInt> words = tokenize(content);
            
            for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
                collector.emit(Stream.ANALYSIS, input, new Values(id, entry.getKey(), words.size()));
            }
        }
        
        collector.ack(input);
    }
    
    private Map<String, MutableInt> tokenize(String content) {
        String[] tokens = content.split(splitregex);
        Map<String, MutableInt> words = new HashMap<>();

        for (String token : tokens) {
            String word = token.toLowerCase();
            Matcher m = wordregex.matcher(word);

            if (m.matches()) {
                MutableInt count = words.get(word);
                if (count == null) {
                    words.put(word, new MutableInt());
                } else {
                    count.increment();
                }
            }
        }
        
        return words;
    }
    
}
