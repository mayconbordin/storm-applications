package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import static storm.applications.constants.ClickAnalyticsConstants.*;
import storm.applications.util.geoip.IPLocation;
import storm.applications.util.geoip.IPLocationFactory;
import storm.applications.util.geoip.Location;

/**
 * User: domenicosolazzo
 */
public class GeographyBolt extends AbstractBolt {
    private IPLocation resolver;

    @Override
    public void initialize() {
        String ipResolver = config.getString(BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
    }

    @Override
    public void execute(Tuple tuple) {
        String ip = tuple.getStringByField(Field.IP);
        
        Location location = resolver.resolve(ip);
        
        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();

            collector.emit(new Values(country, city));
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY);
    }
}
