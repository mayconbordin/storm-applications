package storm.applications.constants;

public interface LogProcessingConstants extends BaseConstants {
    String PREFIX = "lp";
    
    interface Field {
        String IP = "ip";
        String TIMESTAMP = "timestamp";
        String TIMESTAMP_MINUTES = "timestampMinutes";
        String REQUEST = "request";
        String RESPONSE = "response";
        String BYTE_SIZE = "byteSize";
        String COUNT = "count";
        String COUNTRY = "country";
        String COUNTRY_NAME = "country_name";
        String CITY = "city";
        String COUNTRY_TOTAL = "countryTotal";
        String CITY_TOTAL = "cityTotal";
    }
    
    interface Conf extends BaseConf {
        String VOLUME_COUNTER_WINDOW  = "lp.volume_counter.window";
        String VOLUME_COUNTER_THREADS = "lp.volume_counter.threads";
        String STATUS_COUNTER_THREADS = "lp.status_counter.threads";
        String GEO_FINDER_THREADS     = "lp.geo_finder.threads";
        String GEO_STATS_THREADS      = "lp.geo_stats.threads";
    }
    
    interface Component extends BaseComponent {
        String VOLUME_COUNTER = "volumeCounterOneMin";
        String VOLUME_SINK = "countSink";
        String STATUS_COUNTER = "statusCounter";
        String STATUS_SINK = "statusSink";
        String GEO_FINDER = "geoFinder";
        String GEO_STATS = "geoStats";
        String GEO_SINK = "geoSink";
    }
}
