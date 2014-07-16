package storm.applications.spout.generator;

import java.util.Map;
import storm.applications.util.Configuration;
import storm.applications.util.StreamValues;

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
