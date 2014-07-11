package storm.applications.spout.parser;

import java.util.List;
import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TransactionParser extends Parser {

    @Override
    public List<StreamValues> parse(String input) {
        String[] items = input.split(",", 2);
        return list(new StreamValues(items[0], items[1]));
    }
    
}
