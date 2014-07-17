package storm.applications.sink.formatter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BasicFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        Fields schema = context.getComponentOutputFields(tuple.getSourceComponent(), tuple.getSourceStreamId());
        
        String line = "";
            
        for (int i=0; i<tuple.size(); i++) {
            if (i != 0) line += ", ";
            line += String.format("%s=%s", schema.get(i), tuple.getValue(i));
        }
        
        return line;
    }
    
}
