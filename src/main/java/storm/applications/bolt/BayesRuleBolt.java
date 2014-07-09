package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.applications.constants.SpamFilterConstants.Field;
import storm.applications.model.spam.Word;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BayesRuleBolt extends BaseRichBolt {
    private static final float SPAM_PROB = 0.9f;
    
    private OutputCollector collector;
    private Map<String, AnalysisSummary> analysisSummary;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Field.ID, Field.SPAM_PROB, Field.IS_SPAM));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        analysisSummary = new HashMap<String, AnalysisSummary>();
    }

    public void execute(Tuple input) {
        String id    = input.getStringByField(Field.ID);
        Word word    = (Word) input.getValueByField(Field.WORD);
        int numWords = input.getIntegerByField(Field.NUM_WORDS);
        
        AnalysisSummary summary = analysisSummary.get(id);
        
        if (summary == null) {
            summary = new AnalysisSummary();
            analysisSummary.put(id, summary);
        }
        
        summary.uniqueWords++;
        
        updateSummary(summary, word);
        
        if (summary.uniqueWords >= numWords) {
            // calculate bayes
            float pspam = bayes(summary);
            
            collector.emit(new Values(id, pspam, (pspam > SPAM_PROB)));
            analysisSummary.remove(id);
        }
    }
    
    private float bayes(AnalysisSummary summary) {
        // Apply Bayes' rule (via Graham)
        float pposproduct = 1.0f;
        float pnegproduct = 1.0f;
        
        // For every word, multiply Spam probabilities ("Pspam") together
        // (As well as 1 - Pspam)
        for (Word w : summary) {
            pposproduct *= w.getPSpam();
            pnegproduct *= (1.0f - w.getPSpam());
        }

        // Apply formula
        return pposproduct / (pposproduct + pnegproduct);
    }
    
    private void updateSummary(AnalysisSummary summary, Word word) {
        int limit = 15;
        
        // If this list is empty, then add this word in!
        if (summary.isEmpty()) {
            summary.add(word);
        }
        
        // Otherwise, add it in sorted order by interesting level
        else {
            for (int j = 0; j < summary.size(); j++) {
                // For every word in the list already
                Word nw = summary.get(j);

                // If it's more interesting stick it in the list
                if (word.interesting() > nw.interesting()) {
                    summary.add(j, word);
                    break;
                }
                
                // If we get to the end, just tack it on there
                else if (j == summary.size()-1) {
                    summary.add(word);
                }
            }
        }

        // If the list is bigger than the limit, delete entries
        // at the end (the more "interesting" ones are at the 
        // start of the list
        while (summary.size() > limit)
            summary.remove(summary.size()-1);
    }
    
    private static class AnalysisSummary extends ArrayList<Word> {
        public int uniqueWords = 0;
    }
}
