package storm.applications.parser;

import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TransactionParser extends Parser {

    @Override
    public StreamValues parse(String input) {
        String[] items = input.split(",", 2);
        return new StreamValues(items[0], items[1]);
    }
    
}
