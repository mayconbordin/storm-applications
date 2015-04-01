package storm.applications.util.geoip;

import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.util.config.Configuration;



/**
 *
 * @author mayconbordin
 */
public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";
    
    public static IPLocation create(String name, Configuration config) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location(config.getString(BaseConf.GEOIP2_DB));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
