package storm.applications.constants;

public interface ClickAnalyticsConstants extends BaseConstants {
    interface Conf extends BaseConf {
        String REPEATS_THREADS       = "ca.repeats.threads";
        String GEOGRAPHY_THREADS     = "ca.geography.threads";
        String GEOGRAPHY_IP_RESOLVER = "ca.geography.ip_resolver";
        String TOTAL_STATS_THREADS   = "ca.total_stats.threads";
        String GEO_STATS_THREADS     = "ca.geo_stats.threads";
    }
    
    interface Field {
        String IP = "ip";
        String URL = "url";
        String CLIENT_KEY = "clientKey";
        String COUNTRY = "country";
        String COUNTRY_NAME = "country_name";
        String CITY = "city";
        String UNIQUE = "unique";
        String COUNTRY_TOTAL = "countryTotal";
        String CITY_TOTAL = "cityTotal";
        String TOTAL_COUNT = "totalCount";
        String TOTAL_UNIQUE = "totalUnique";
    }
    
    interface Component extends BaseComponent {
        String REPEATS = "repeatsBolt";
        String GEOGRAPHY = "geographyBolt";
        String TOTAL_STATS = "totalStats";
        String GEO_STATS = "geoStats";
    }
}
