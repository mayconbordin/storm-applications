package storm.applications.spout.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Parser {
    protected Map config;

    public void initialize(Map config) {
        this.config = config;
    }

    public abstract List<StreamValues> parse(String input);
    
    protected List<StreamValues> list(StreamValues...values) {
        return Arrays.asList(values);
    }
}