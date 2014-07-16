package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.applications.constants.WordCountConstants.Field;

public class SplitSentenceBolt extends AbstractBolt {
    private static final String splitregex = "\\W";
    
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple input) {
        String[] words = input.getString(0).split(splitregex);
        
        for (String word : words) {
            collector.emit(new Values(word));
        }
    }
    
}
