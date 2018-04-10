package org.dataconservancy.pass.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

public class ElasticSearchIndexerTest {
    private MockWebServer server;
    private ElasticSearchIndexer indexer;
    private HttpUrl es_index_url;

    @Before
    public void setup() throws IOException {
        server = new MockWebServer();
        es_index_url = server.url("/es/test/");
        indexer = new ElasticSearchIndexer(es_index_url.toString(), "admin", "admin");
    }

    @After
    public void cleanup() throws IOException {
        server.shutdown();
    }

    @Test
    public void testCreateMessage() throws Exception {
        // Mock message about created Fedora resource

        String fedora_res_uri = server.url("/fcrepo/cow/moo").toString();

        JSONObject res_json = new JSONObject();
        res_json.put("@id", fedora_res_uri);
        res_json.put("@context",
                "https://raw.githubusercontent.com/OA-PASS/ember-fedora-adapter/master/tests/dummy/public/farm.jsonld");
        res_json.put("healthy", true);
        res_json.put("name", "moo");

        // GET for Fedora resource
        server.enqueue(new MockResponse().setBody(res_json.toString()));

        // POST to Elasticsearch
        server.enqueue(new MockResponse().setBody("{}"));

        FedoraMessage m = new FedoraMessage();
        m.setAction(FedoraAction.CREATED);
        m.setResourceURI(fedora_res_uri);

        indexer.handle(m);

        // Check the requests

        RecordedRequest get = server.takeRequest();

        assertEquals("GET", get.getMethod());
        assertNotNull(get.getHeader("Authorization"));
        assertEquals("application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"", get.getHeader("Accept"));
        assertEquals(fedora_res_uri, get.getRequestUrl().toString());

        RecordedRequest post = server.takeRequest();

        assertEquals("POST", post.getMethod());
        
        JSONObject payload = new JSONObject(post.getBody().readUtf8());
        
        assertEquals(res_json.get("@id"), payload.get("@id"));
        assertEquals("application/json; charset=utf-8", post.getHeader("Content-Type"));
        assertTrue(post.getRequestUrl().toString().startsWith(es_index_url.toString()));
    }
    
    @Test
    public void testModifyMessage() throws Exception {
        // Mock message about modified Fedora resource

        String fedora_res_uri = server.url("/fcrepo/cow/moo").toString();

        JSONObject res_json = new JSONObject();
        res_json.put("@id", fedora_res_uri);
        res_json.put("@context",
                "https://raw.githubusercontent.com/OA-PASS/ember-fedora-adapter/master/tests/dummy/public/farm.jsonld");
        res_json.put("healthy", true);
        res_json.put("name", "moo");

        // GET for Fedora resource
        server.enqueue(new MockResponse().setBody(res_json.toString()));

        // POST to Elasticsearch
        server.enqueue(new MockResponse().setBody("{}"));

        FedoraMessage m = new FedoraMessage();
        m.setAction(FedoraAction.MODIFIED);
        m.setResourceURI(fedora_res_uri);

        indexer.handle(m);

        // Check the requests

        RecordedRequest get = server.takeRequest();

        assertEquals("GET", get.getMethod());
        assertNotNull(get.getHeader("Authorization"));
        assertEquals("application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"", get.getHeader("Accept"));
        assertEquals(fedora_res_uri, get.getRequestUrl().toString());

        RecordedRequest post = server.takeRequest();

        assertEquals("POST", post.getMethod());
        
        JSONObject payload = new JSONObject(post.getBody().readUtf8());
        
        assertEquals(res_json.get("@id"), payload.get("@id"));
        assertEquals("application/json; charset=utf-8", post.getHeader("Content-Type"));
        assertTrue(post.getRequestUrl().toString().startsWith(es_index_url.toString()));
    }
    
    @Test
    public void testDeleteMessage() throws Exception {
        // Mock message about deleted Fedora resource

        String fedora_res_uri = server.url("/fcrepo/cow/moo").toString();

        // DELETE to Elasticsearch
        server.enqueue(new MockResponse().setBody("{}"));

        FedoraMessage m = new FedoraMessage();
        m.setAction(FedoraAction.DELETED);
        m.setResourceURI(fedora_res_uri);

        indexer.handle(m);

        // Check the requests

        RecordedRequest delete = server.takeRequest();

        assertEquals("DELETE", delete.getMethod());
        
        assertTrue(delete.getRequestUrl().toString().startsWith(es_index_url.toString()));
    }
}
