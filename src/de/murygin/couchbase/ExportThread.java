/*******************************************************************************
 * Copyright (c) 2013 Daniel Murygin.
 *
 * This program is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public License 
 * as published by the Free Software Foundation, either version 3 
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,    
 * but WITHOUT ANY WARRANTY; without even the implied warranty 
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
 * See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. 
 * If not, see <http://www.gnu.org/licenses/>.
 * 
 * Contributors:
 *     Daniel Murygin <dm[at]sernet[dot]de> - initial API and implementation
 ******************************************************************************/
package de.murygin.couchbase;

import org.apache.log4j.Logger;

import com.couchbase.client.CouchbaseClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 *
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class ExportThread extends Thread {
    
    private static final Logger LOG = Logger.getLogger(ExportThread.class);
    
    Client jerseyClient = null;
    CouchbaseClient couchbaseClient = null;
    String username = null;
    
    
    public ExportThread(Client jerseyClient, CouchbaseClient cb, String username) {
        super();
        this.jerseyClient = jerseyClient;
        this.couchbaseClient = cb;
        this.username = username;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        saveRecentTracksOfFriends(username);
    }
    
    private void saveRecentTracksOfFriends(String username) {                
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading tracks of user: " + username + "from lastfm...");
        }
        String jsonResponse = getRecentTracksAsJson(username);         
        if (LOG.isDebugEnabled()) {
            LOG.debug("lastfm JSON response: " + jsonResponse);
        } 
        saveJsonInCouchbase(username, jsonResponse);   
        if (LOG.isInfoEnabled()) {
            LOG.info("Recent tracks of user " + username + " saved in couchbase.");
        }
    }
    
    private String getRecentTracksAsJson(String username) {
        StringBuilder sb = new StringBuilder();
        sb.append("http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=");
        sb.append(username);
        sb.append("&api_key=");
        sb.append(LastfmExporter.key);
        sb.append("&format=json");
        
        String url = sb.toString();
        
        WebResource webResource = jerseyClient.resource(url);
        String jsonResponse = webResource.get(String.class);
        return jsonResponse;
    }

    private void saveJsonInCouchbase(String username, String jsonResponse) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("lastfm:recenttracks:");
            sb.append(username);           
            String key = sb.toString();            
            couchbaseClient.set(key, 0, jsonResponse);
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
        }
    }
}
