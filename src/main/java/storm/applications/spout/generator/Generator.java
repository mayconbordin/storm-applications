package storm.applications.spout.generator;

import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Generator {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }
    
    public abstract StreamValues generate();
}
