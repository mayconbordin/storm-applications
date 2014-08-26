package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.SpamFilterConstants.Conf;
import storm.applications.constants.SpamFilterConstants.Field;
import storm.applications.model.spam.Word;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BayesRuleBolt extends AbstractBolt {
    private double spamProbability;
    
    private Map<String, AnalysisSummary> analysisSummary;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.SPAM_PROB, Field.IS_SPAM);
    }

    @Override
    public void initialize() {
        spamProbability = config.getDouble(Conf.BAYES_RULE_SPAM_PROB, 0.9d);
        analysisSummary = new HashMap<>();
    }

    @Override
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
            
            collector.emit(new Values(id, pspam, (pspam > spamProbability)));
            analysisSummary.remove(id);
        }
        
        collector.ack(input);
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

                // If it's the same word, don't bother
                if (word.getWord().equals(nw.getWord())) {
                    break;
                    // If it's more interesting stick it in the list
                } else if (word.interesting() > nw.interesting()) {
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
