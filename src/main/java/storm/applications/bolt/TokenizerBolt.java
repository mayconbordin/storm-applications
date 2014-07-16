package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
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
    // How to split the String into  tokens
    private static final String splitregex = "\\W";
    
    // Regex to eliminate junk (although we really should welcome the junk)
    private static final Pattern wordregex = Pattern.compile("\\w+");
    
    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        streams.put(Stream.TRAINING, new Fields(Field.WORD, Field.COUNT, Field.IS_SPAM));
        streams.put(Stream.TRAINING_SUM, new Fields(Field.SPAM_TOTAL, Field.SPAM_TOTAL));
        streams.put(Stream.ANALYSIS, new Fields(Field.ID, Field.WORD, Field.NUM_WORDS));
        return streams;
    }

    @Override
    public void initialize() {
    }

    @Override
    public void execute(Tuple input) {
        String content = input.getStringByField(Field.MESSAGE);

        if (input.getSourceStreamId().equals(Stream.TRAINING)) {
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
                
                collector.emit(Stream.TRAINING, new Values(word, count, isSpam));
            }
            
            collector.emit(Stream.TRAINING_SUM, new Values(spamTotal, hamTotal));
        }
        
        else if (input.getSourceStreamId().equals(Stream.ANALYSIS)) {
            String id = input.getStringByField(Field.ID);
            
            Map<String, MutableInt> words = tokenize(content);
            
            for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
                collector.emit(Stream.ANALYSIS, new Values(id, entry.getKey(), words.size()));
            }
        }
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
