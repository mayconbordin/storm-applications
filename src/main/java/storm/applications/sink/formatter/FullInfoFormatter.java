package storm.applications.sink.formatter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FullInfoFormatter extends Formatter {
    private static final String TEMPLATE = "source: %s:%d, stream: %s, id: %s, values: [%s]";

    @Override
    public String format(Tuple tuple) {
        Fields schema = context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId());

        String values = "";
        for (int i=0; i<tuple.size(); i++) {
            if (i != 0) values += ", ";
            values += String.format("%s=%s", schema.get(i), tuple.getValue(i));
        }
        
        return String.format(TEMPLATE, tuple.getSourceComponent(), tuple.getSourceTask(),
                tuple.getSourceStreamId(), tuple.getMessageId().toString(), values);
    }
    
}
