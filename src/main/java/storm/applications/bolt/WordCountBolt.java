package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.MutableLong;
import java.util.HashMap;
import java.util.Map;
import storm.applications.constants.WordCountConstants.Field;

public class WordCountBolt extends AbstractBolt {
    private final Map<String, MutableLong> counts = new HashMap<>();

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        MutableLong count = counts.get(word);
        
        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();
        
        collector.emit(new Values(word, count.get()));
    }
    
}
