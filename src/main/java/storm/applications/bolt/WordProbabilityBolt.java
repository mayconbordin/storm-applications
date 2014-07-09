package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import storm.applications.constants.SpamFilterConstants.*;
import storm.applications.model.spam.Word;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class WordProbabilityBolt extends BaseRichBolt {
    private OutputCollector collector; 
    private Map<String, Word> words;
    private int spamTotal = 0;
    private int hamTotal = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Stream.ANALYSIS, new Fields(Field.ID, Field.WORD, Field.NUM_WORDS));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals(Stream.TRAINING)) {
            String word = input.getStringByField(Field.WORD);
            int count = input.getIntegerByField(Field.COUNT);
            boolean isSpam = input.getBooleanByField(Field.IS_SPAM);
            
            Word w = words.get(word);
            
            if (w == null) {
                w = new Word(word);
                words.put(word, w);
            }

            if (isSpam) {
                w.countBad(count);
            } else {
                w.countGood(count);
            }
        }
        
        else if (input.getSourceStreamId().equals(Stream.TRAINING_SUM)) {
            int spamCount = input.getIntegerByField(Field.SPAM_TOTAL);
            int hamCount  = input.getIntegerByField(Field.HAM_TOTAL);
            
            spamTotal += spamCount;
            hamTotal  += hamCount;
            
            for (Word word : words.values()) {
                word.calcProbs(spamTotal, hamTotal);
            }
        }
        
        else if (input.getSourceStreamId().equals(Stream.ANALYSIS)) {
            String id = input.getStringByField(Field.ID);
            String word = input.getStringByField(Field.WORD);
            int numWords = input.getIntegerByField(Field.NUM_WORDS);
            
            Word w = words.get(word);

            if (w == null) {
                w = new Word(word);
                w.setPSpam(0.4f);
            }
            
            collector.emit(Stream.ANALYSIS, new Values(id, w, numWords));
        }
    }
    
}
