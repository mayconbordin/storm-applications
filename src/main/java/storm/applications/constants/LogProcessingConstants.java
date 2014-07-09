package storm.applications.constants;

public final class LogProcessingConstants {
    public static final class Field {
        public static final String LOG_ENTRY = "LogEntry";
        public static final String LOG_IP = "ip";
        public static final String LOG_STATUS_CODE = "statusCode";
        public static final String STATUS_CODE_COUNT = "stautsCodeCount";
        public static final String LOG_TIMESTAMP = "timestamp";
        public static final String LOG_INCREMENT = "IncrementAmount";
        public static final String LOG_COLUMN = "column";
        public static final String COUNTRY = "country";
        public static final String COUNTRY_NAME = "country_name";
        public static final String CITY = "city";
        public static final String COUNTRY_TOTAL = "countryTotal";
        public static final String CITY_TOTAL = "cityTotal";
    }
    
    public static final class Conf {
        public static final String KAFKA_HOSTS = "kafka.hosts";
        public static final String KAFKA_PARTITIONS = "kafka.broker.partitions";
        public static final String KAFKA_TOPIC = "kafka.topic";
        public static final String KAFKA_ZOOKEEPER_PATH = "kafka.zookeeper.path";
        public static final String KAFKA_CONSUMER_ID = "kafka.consumer.id";

        public static final String CASSANDRA_HOST = "cassandra.host";
        public static final String CASSANDRA_KEYSPACE = "cassandra.keyspace";
        public static final String CASSANDRA_COUNT_CF_NAME = "cassandra.count.cf.name";
        public static final String CASSANDRA_STATUS_CF_NAME = "cassandra.status.cf.name";
        public static final String CASSANDRA_COUNTRY_CF_NAME = "cassandra.country.cf.name";
        
        public static final String ZOOKEEPER_HOST = "zookeeper.host";
        public static final String ZOOKEEPER_PORT = "zookeeper.port";
    }
    
    public static final class Component {
        public static final String SPOUT = "spout";
        public static final String PARSER = "parser";
        public static final String VOLUME_COUNT = "volumeCounterOneMin";
        public static final String COUNT_PERSIST = "countPersistor";
        public static final String IP_STAT_PARSER = "ipStatusParser";
        public static final String STAT_COUNT = "statusCounter";
        public static final String STAT_COUNT_PERSIST = "statusCountePersistor";
        public static final String GEO_FINDER = "geoLocationFinder";
        public static final String COUNTRY_STATS = "countryStats";
        public static final String COUNTRY_STATS_PERSIST = "countryStatsPersistor";
        public static final String PRINTER = "printerBolt";
    }
}
