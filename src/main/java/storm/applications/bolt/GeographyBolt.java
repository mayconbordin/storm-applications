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
        String ipResolver = config.getString(Conf.GEOGRAPHY_IP_RESOLVER);
        resolver = IPLocationFactory.create(ipResolver, config);
    }

    @Override
    public void execute(Tuple tuple) {
        String ip = tuple.getStringByField(Field.IP);
        
        Location l = resolver.resolve(ip);
        String city = l.getCity();
        String country = l.getCountryName();
        
        collector.emit(new Values(country, city));
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY);
    }
}
