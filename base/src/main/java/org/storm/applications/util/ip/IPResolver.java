package org.storm.applications.util.ip;

import org.json.simple.JSONObject;

/**
 * User: domenicosolazzo
 */
public interface IPResolver {
    public JSONObject resolveIP(String ip);
}
