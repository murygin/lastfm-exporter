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
 *     Daniel Murygin <daniel.murygin[at]gmail[dot]com>
 ******************************************************************************/
package de.murygin.couchbase;

import org.apache.log4j.Logger;

import com.couchbase.client.CouchbaseClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * This thread loads information about one artist from Last.fm
 * ans saves JSON response in a Couchbase bucket.
 * 
 * {@link LastfmExporter} uses multiple ArtistExportThreads
 * to export data from Last.fm
 * 
 * Last.fm API - http://www.lastfm.de/api
 * Couchbase - http://www.couchbase.com/
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class ArtistExportThread extends Thread {
    
    private static final Logger LOG = Logger.getLogger(ArtistExportThread.class);
    
    Client jerseyClient = null;
    CouchbaseClient couchbaseClient = null;
    String artistName = null;
    
    
    public ArtistExportThread(Client jerseyClient, CouchbaseClient cb, String username) {
        super();
        this.jerseyClient = jerseyClient;
        this.couchbaseClient = cb;
        this.artistName = username;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        exportArtists(artistName);
    }
    
    private void exportArtists(String artistName) {                
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading info of artist: " + artistName + " from lastfm...");
        }
        String jsonResponse = getArtistInfoAsJson(artistName);         
        if (LOG.isDebugEnabled()) {
            LOG.debug("lastfm JSON response: " + jsonResponse);
        } 
        saveJsonInCouchbase(artistName, jsonResponse);   
        if (LOG.isInfoEnabled()) {
            LOG.info("Info of " + artistName + " saved in couchbase.");
        }
    }
    
    private String getArtistInfoAsJson(String artistName) {
        String url = buildWebserviceUrl(artistName);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Webservice URL: " + url);
        }
        
        WebResource webResource = jerseyClient.resource(url);
        String jsonResponse = webResource.get(String.class);
        return jsonResponse;
    }

    private void saveJsonInCouchbase(String artistName, String jsonResponse) {
        try {
            String key = buildDocumentKey(artistName);            
            couchbaseClient.set(key, 0, jsonResponse);
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
        }
    }
    
    private String buildWebserviceUrl(String artistName) {
        artistName = artistName.replaceAll(" ", "%20");
        StringBuilder sb = new StringBuilder();
        sb.append("http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=");
        sb.append(artistName);
        sb.append("&api_key=");
        sb.append(LastfmExporter.key);
        sb.append("&format=json");     
        return sb.toString();
    }
    
    private String buildDocumentKey(String artistName) {
        StringBuilder sb = new StringBuilder();
        sb.append("lastfm:artist:");
        sb.append(artistName);
        return sb.toString();
    }
}
