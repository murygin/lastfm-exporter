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

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.couchbase.client.CouchbaseClient;
import com.sun.jersey.api.client.Client;

import de.umass.lastfm.Artist;

/**
 * LastfmExporter exports information about artists from Last.fm 
 * and saves JSON responses in a Couchbase bucket.
 * 
 * Last.fm API - http://www.lastfm.de/api
 * Couchbase - http://www.couchbase.com/
 *
 * @author Daniel Murygin <dm[at]sernet[dot]de>
 */
public class LastfmExporter {

    private static final Logger LOG = Logger.getLogger(LastfmExporter.class);
    
    public static String key = "040f7b24f14afd8cd5b6385fceba24f7"; //this is the key used in the last.fm API examples online.
   
    private static String artistNameToStart = "Eminem";
    
    private static int maxNumberOfThreads = 10;
    
    private ExecutorService taskExecutor;
    
    Client jerseyClient;
    CouchbaseClient cb = null;
    
    public static Set<String> processedUser = new HashSet<String>();
    public static Set<String> processedArtists= new HashSet<String>();

    public LastfmExporter() {
        super();
        jerseyClient = Client.create();
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create("http://127.0.0.1:8091/pools"));

        try {
            cb = new CouchbaseClient(uris, "lastfm", "");
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
        }
        
        taskExecutor = Executors.newFixedThreadPool(maxNumberOfThreads);
    }

    public static void main(String[] args) {   
        LastfmExporter loader = new LastfmExporter();    
        loader.saveArtistInfo(artistNameToStart);    
    }
    
    private void saveArtistInfo(String artistName) {
        if(processedArtists.contains(artistName)) {
            return;
        } else {
            processedArtists.add(artistName);
        }
        
        Collection<Artist> artistCollection = Artist.getSimilar(artistName, key);
        
        if (LOG.isInfoEnabled()) {
            LOG.info("");
            LOG.info("");
            LOG.info("Processing artist " + artistName + ", number of similar:  " + artistCollection.size());
        }
        
        // export artist
        ArtistExportThread thread = new ArtistExportThread(jerseyClient, cb, artistName);
        taskExecutor.execute(thread);
        
       // export similar artists
        for (Artist artist : artistCollection) {
            String currentArtist = artist.getName(); 
            
            ArtistExportThread similarArtistThread = new ArtistExportThread(jerseyClient, cb, currentArtist);
            taskExecutor.execute(similarArtistThread);
         
        }
        for (Artist artist : artistCollection) {
            saveArtistInfo(artist.getName());
        }
    }

}
