package storm.applications.spout.generator;

import java.util.Map;
import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Generator {
    protected Map config;

    public void initialize(Map config) {
        this.config = config;
    }
    
    public abstract StreamValues generate();
}
