package storm.applications.parser;

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

    public abstract StreamValues parse(String input);
}