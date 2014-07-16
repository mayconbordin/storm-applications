package storm.applications.util;

import backtype.storm.Config;
import java.util.Map;

public class Configuration extends Config {

    public Configuration(Map map) {
        putAll(map);
    }
    
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
            } else if (obj instanceof String) {
                try {
                    val = Integer.parseInt((String)obj);
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

    public boolean exists(String key) {
        return containsKey(key);
    }
}
