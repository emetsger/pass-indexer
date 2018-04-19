package org.dataconservancy.pass.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


/**
 * Maintain documents corresponding to the JSON-LD representation of Fedora resources.
 * The compact JSON-LD representation must be simple enough for Elasticsearch to understand.
 * Documents are updated when Fedora resources are created or modified or deleted.
 * The document id is the path of the Fedora URI encoded as base64 safe for URLs.
 * 
 * The mapping in the index configuration is used to check JSON documents retrieved from Fedora 
 * before indexing. Properties which do not have a mapping or otherwise cannot be indexed are
 * logged and ignored.
 * 
 * Properties which contain Fedora URIs have a custom matching with causes them to be indexed as
 * Fedora resource paths. This allows a client to search using different URIs which map to the same
 * Fedora resource.
 */
public class ElasticSearchIndexer {
    public static final String FEDORA_ACCEPT_HEADER = "application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"";
    public static final String FEDORA_PREFER_HEADER = "return=representation; omit=\"http://fedora.info/definitions/v4/repository#ServerManaged\"";
   
    private static final String ES_INDEX_CONFIG = "/esindex.json";
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchIndexer.class);

    private final OkHttpClient client;
    private final String fedora_cred;
    private final String es_index_url;
    private final Set<String> supported_fields;

    /**
     * If the given Elasticsearch does not exist, create it using the built in configuration.
     * Otherwise the configuration is retrieved from the index.
     * 
     * @param es_index_url 
     * @param fedora_user
     * @param fedora_pass
     * @param fedora_uri_pattern - Must match the part of the Fedura URI which is the the resource path.
     * @throws IOException
     */
    public ElasticSearchIndexer(String es_index_url, String fedora_user, String fedora_pass) throws IOException {
        this.client = new OkHttpClient();
        this.es_index_url = es_index_url.endsWith("/") ? es_index_url : es_index_url + "/";
        this.fedora_cred = Credentials.basic(fedora_user, fedora_pass);
        
        JSONObject config = get_existing_index_configuration();

        if (config == null) {
            LOG.info("Index does not exist. Creating " + es_index_url);
            config = load_builtin_index_configuration();
            create_index(config);
        } else {
            LOG.info("Found existing index " + es_index_url);
        }
        
        // Determine the available fields in the index from the configuration and which fields support completion.
        
        // The mappings key is either toplevel or inside an object representing the index.
        if (!config.has("mappings")) {
            Set<String> keys = config.keySet();
            
            if (keys.size() == 1) {
                config = config.getJSONObject(keys.iterator().next());
            }
        }
        
        JSONObject props = config.getJSONObject("mappings").getJSONObject("_doc").getJSONObject("properties");
        this.supported_fields = new HashSet<>(props.keySet());
    }
   
    // Create index es_index_url with the given configuration
    private void create_index(JSONObject config) throws IOException {
        RequestBody body = RequestBody.create(JSON, config.toString());
        Request put = new Request.Builder().url(es_index_url).put(body).build();
        Response response = client.newCall(put).execute();
        String result = response.body().string();

        if (response.isSuccessful()) {
            LOG.info("Created index: " + es_index_url + "\n" + result);
        } else {
            String msg = "Failed to create index: " + es_index_url + "\n" + result;
            LOG.error(msg);
            throw new IOException(msg);
        }
    }
    
    // Return the index configuration built in to the module.
    private JSONObject load_builtin_index_configuration() throws IOException {
        try (InputStream is = this.getClass().getResourceAsStream(ES_INDEX_CONFIG)) {
            return new JSONObject(new JSONTokener(is));
        }
    }
    
    // Return the current index configuration or null if it does not exist.
    private JSONObject get_existing_index_configuration() throws IOException {
        Request get = new Request.Builder().url(es_index_url).build();
        Response response = client.newCall(get).execute();

        if (response.code() == 404) {
            return null;
        }
        
        if (!response.isSuccessful()) {
            String msg = "Failed to retrieve index config: " + es_index_url + " " + response.code();
            LOG.error(msg);
            throw new IOException(msg);
        }

        return new JSONObject(response.body().string());
    }
    
    // Return compact JSON-LD representation of Fedora resource without server triples
    private String get_fedora_resource(String uri) throws IOException {
        Request get = new Request.Builder().url(uri).header("Authorization", fedora_cred)
                .header("Accept", FEDORA_ACCEPT_HEADER).header("Prefer", FEDORA_PREFER_HEADER).build();

        Response response = client.newCall(get).execute();

        if (!response.isSuccessful()) {
            String msg = "Failed to retrieve Fedora resource: " + uri + " " + response.code();
            LOG.error(msg);
            throw new IOException(msg);
        }

        return response.body().string();
    }
    
    // Return URL safe base64 encoding of string.
    private String base64_encode(String s) {
        return Base64.getUrlEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    // Return URL safe document id.
    private String get_document_id(String fedora_uri) throws IOException {
        return base64_encode(new URL(fedora_uri).getPath());
    }

    private String get_create_document_url(String doc_id) throws IOException {
        return es_index_url + "_doc/" + doc_id + "?pretty";
    }

    // Do any normalization necessary before indexing.
    // Remove properties not in the index mapping
    // For each NAME_suggest, add NAME to NAME_suggest.
    private String normalize_document(String json) {
        JSONObject o = new JSONObject(json);

        // TODO For the moment remove inline @context which Elasticsearch cannot handle
        o.remove("@context");
        
        // Ignore and warn about keys not in the configuration or with object values
        for (Iterator<String> iter = o.keys(); iter.hasNext(); ) {
            String key = iter.next();
            Object value = o.get(key);
            
            if (!supported_fields.contains(key)) {
                LOG.warn("Unexpected property ignored: " + key + ", " + value);
                iter.remove();
            }
            
            if (JSONObject.class.isInstance(value)) {
                LOG.warn("Property with object value ignored: " + key + ", " + value);
                iter.remove();
            }
        }
        
        return o.toString();
    }

    // Create or update the document corresponding to a Fedora resource in Elasticsearch.
    // For simplicity the document id is the base64 encoded Fedora URI.
    private void update_document(String fedora_uri) throws IOException {
        LOG.debug("Updating document for Fedora resource: " + fedora_uri);

        String doc = normalize_document(get_fedora_resource(fedora_uri));
        String doc_id = get_document_id(fedora_uri);
        String doc_url = get_create_document_url(doc_id);

        RequestBody body = RequestBody.create(JSON, doc);
        Request post = new Request.Builder().url(doc_url).post(body).build();
        Response response = client.newCall(post).execute();
        String result = response.body().string();

        if (response.isSuccessful()) {
            LOG.debug("Update success: " + response);
        } else {
            String msg = "Update failure: " + result; 
            LOG.error(msg);
            throw new IOException(msg);
        }
    }

    private void delete_document(String fedora_uri) throws IOException {
        LOG.debug("Deleting document for Fedora resource: " + fedora_uri);

        String doc_id = get_document_id(fedora_uri);
        String doc_url = get_create_document_url(doc_id);

        Request delete = new Request.Builder().url(doc_url).delete().build();
        Response response = client.newCall(delete).execute();
        String result = response.body().string();

        if (response.isSuccessful()) {
            LOG.debug("Delete success: " + response);
        } else {
            LOG.error("Delete failed: " + result);
        }
    }

    public void handle(FedoraMessage m) throws IOException {
        LOG.debug("Handling Fedora message: " + m);

        switch (m.getAction()) {
        case CREATED:
        case MODIFIED:
            update_document(m.getResourceURI());
            break;
        case DELETED:
            delete_document(m.getResourceURI());
            break;
        default:
            break;
        }
    }
}
