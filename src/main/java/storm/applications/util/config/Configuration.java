package storm.applications.util.config;

import backtype.storm.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.math.NumberUtils;
import storm.applications.util.data.DataTypeUtils;

public class Configuration extends Config {
    public static final String METRICS_ENABLED        = "metrics.enabled";
    public static final String METRICS_REPORTER       = "metrics.reporter";
    public static final String METRICS_INTERVAL_VALUE = "metrics.interval.value";
    public static final String METRICS_INTERVAL_UNIT  = "metrics.interval.unit";
    public static final String METRICS_OUTPUT         = "metrics.output";
    
    public String getString(String key) {
        String val = null;
        Object obj = get(key);
        if (null != obj) {
            if (obj instanceof String){
                val = (String)obj;
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public String getString(String key, String def) {
        String val = null;
        try {
            val = getString(key);
        } catch (IllegalArgumentException ex) {
            val = def;
        }
        return val;
    }

    public int getInt(String key) {
        int val = 0;
        Object obj = get(key);
        
        if (null != obj) {
        if (obj instanceof Integer) {
            val = (Integer)obj;
        } else if (obj instanceof Number) {
            val = ((Number)obj).intValue();
        } else if (obj instanceof String) {
                try {
                    val = Integer.parseInt((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Value for configuration key " + key + " cannot be parsed to an Integer", ex);
                }
            } else {
                throw new IllegalArgumentException("Integer value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        
        return val;
    }

    public long getLong(String key) {
        long val = 0;
        Object obj = get(key);
        if (null != obj) {
            if (obj instanceof Long) {
                val = (Long)obj;
            } else if (obj instanceof String) {
                try {
                    val = Long.parseLong((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found  in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public int getInt(String key, int def) {
        int val = 0;
        try {
            val = getInt(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public long getLong(String key, long def) {
        long val = 0;
        try {
            val = getLong(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public double getDouble(String key) {
        double  val = 0;
        Object obj = get(key);
        if (null != obj) {
            if (obj instanceof Double) {
                val = (Double)obj;
            } else if (obj instanceof String) {
                try {
                    val = Double.parseDouble((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public double getDouble(String key, double def) {
        double val = 0;
        try {
            val = getDouble(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public boolean getBoolean(String key) {
        boolean val = false;
        Object obj = get(key);
        if (null != obj) {
            if (obj instanceof Boolean) {
                val = (Boolean)obj;
            } else if (obj instanceof String) {
                val = Boolean.parseBoolean((String)obj);
            } else {
                throw new IllegalArgumentException("Boolean value not found  in configuration  for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public boolean getBoolean(String key, boolean def) {
        boolean val = false;
        try {
            val = getBoolean(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }
    
    public int[] getIntArray(String key, int[] def) {
        return getIntArray(key, ",", def);
    }
    
    public int[] getIntArray(String key, String separator, int[] def) {
        int[] values = null;
        
        try {
            values = getIntArray(key, separator);
        } catch (IllegalArgumentException ex) {
            values = def;
        }

        return values;
    }
    
    public int[] getIntArray(String key, String separator) {
        String value   = getString(key);
        String[] items = value.split(separator);
        int[] values   = new int[items.length];

        for (int i=0; i<items.length; i++) {
            try {
                values[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Value for configuration key "
                        + key + " cannot be parsed to an Integer array", ex);
            }
        }
            
        return values;
    }

    public boolean exists(String key) {
        return containsKey(key);
    }
    
    public static Configuration fromMap(Map map) {
        Configuration config = new Configuration();
        
        for (Object k : map.keySet()) {
            String key   = (String) k;
            Object value = map.get(key);
            
            if (value instanceof String) {
                String str = (String) value;
                
                if (DataTypeUtils.isInteger(str)) {
                    config.put(key, Integer.parseInt(str));
                } else if (NumberUtils.isNumber(str)) {
                    config.put(key, Double.parseDouble(str));
                } else if (value.equals("true") || value.equals("false")) {
                    config.put(key, Boolean.parseBoolean(str));
                } else {
                    config.put(key, value);
                }
            } else {
                config.put(key, value);
            }
        }
        
        return config;
    }
    
    public static Configuration fromProperties(Properties properties) {
        Configuration config = new Configuration();

        for (String key : properties.stringPropertyNames()) {
            config.put(key, parseString(properties.getProperty(key)));
        }
        
        return config;
    }
    
    public static Configuration fromStr(String str) {
        Map<String, String> map = strToMap(str);
        Configuration config = new Configuration();
        
        for (String key : map.keySet()) {
            config.put(key, parseString(map.get(key)));
        }
        
        return config;
    }
    
    public static Map<String, String> strToMap(String str) {
        Map<String, String> map = new HashMap<>();
        String[] arguments = str.split(",");
        
        for (String arg : arguments) {
            String[] kv = arg.split("=");
            map.put(kv[0].trim(), kv[1].trim());
        }
        
        return map;
    }
    
    private static Object parseString(String value) {
        if (DataTypeUtils.isInteger(value)) {
            return Integer.parseInt(value);
        } else if (NumberUtils.isNumber(value)) {
            return Double.parseDouble(value);
        } else if (value.equals("true") || value.equals("false")) {
            return Boolean.parseBoolean(value);
        }
        
        return value;
    }
}
