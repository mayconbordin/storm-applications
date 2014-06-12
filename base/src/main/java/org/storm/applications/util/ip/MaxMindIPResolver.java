/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.storm.applications.util.ip;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MaxMindIPResolver implements IPResolver {
    private static final Logger LOG = LoggerFactory.getLogger(MaxMindIPResolver.class);
    
    private String dbPath;
    private LookupService lookupService;

    public MaxMindIPResolver(String dbPath) throws IOException {
        this.dbPath = dbPath;
        lookupService = new LookupService(dbPath, LookupService.GEOIP_STANDARD);
    }

    public JSONObject resolveIP(String ip) {
        Location loc = lookupService.getLocation(ip);
        
        JSONObject obj = new JSONObject();
        obj.put("country_name", loc.countryName);
        obj.put("country_code", loc.countryCode);
        obj.put("city", loc.city);
        obj.put("ip", ip);
        
        return obj;
    }
    
}
