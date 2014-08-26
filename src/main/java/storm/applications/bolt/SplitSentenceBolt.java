package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang3.StringUtils;
import storm.applications.constants.WordCountConstants.Field;

public class SplitSentenceBolt extends AbstractBolt {
    private static final String splitregex = "\\W";
    
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.CREATED_AT);
    }

    @Override
    public void execute(Tuple input) {
        long createdAt = input.getLongByField(Field.CREATED_AT);
        String[] words = input.getString(0).split(splitregex);
        
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word, createdAt));
        }
        
        collector.ack(input);
    }
    
}
