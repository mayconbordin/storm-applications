package storm.applications.util.geoip;

import java.util.Map;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.util.ConfigUtility;



/**
 *
 * @author mayconbordin
 */
public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";
    
    public static IPLocation create(String name, Map config) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location(ConfigUtility.getString(config, BaseConf.GEOIP2_DB));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
