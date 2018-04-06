package pass.indexer;

import java.io.IOException;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squareup.okhttp.Credentials;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

// TODO Logging.
// TODO Auto-complete
// TODO Schema

/**
 * Update an Elasticsearch index in response to an action on a Fedora resource.
 */
public class ElasticsearchIndexer {
    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final String JSON_LD_COMPACT = "application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"";
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexer.class);

    private final OkHttpClient client;
    private final String fedora_cred;
    private final String es_index_url;

    public ElasticsearchIndexer(String es_index_url, String fedora_user, String fedora_pass) {
        this.client = new OkHttpClient();
        this.es_index_url = es_index_url;
        this.fedora_cred = Credentials.basic(fedora_user, fedora_pass);
    }

    // Return compact JSON-LD representation of Fedora resource
    private String get_fedora_resource(String uri) throws IOException {
        Request get = new Request.Builder().url(uri).header("Authorization", fedora_cred)
                .header("Accept", JSON_LD_COMPACT).build();

        Response response = client.newCall(get).execute(); 
        
        if (!response.isSuccessful()) {
            LOG.error("Failed to retrieve Fedora resource: " + uri + " " + response.code());
        }
        
        return response.body().string();
    }

    // POST JSON-LD to Elasticsearch
    void elasticsearch_add_document(String json) throws IOException {
        // TODO Hack to remove inline @context which Elasticsearch cannot handle
        JSONObject o = new JSONObject(json);
        o.remove("@context");
        json = o.toString();
        
        LOG.debug("Adding document: " + json);

        RequestBody body = RequestBody.create(JSON, json);
        Request post = new Request.Builder().url(es_index_url).post(body).build();
        Response response = client.newCall(post).execute();
        String result = response.body().string();

        if (response.isSuccessful()) {
            LOG.debug("Indexing success: " + response);
        } else {
            LOG.error("Indexing failed: " + result);
        }
    }

    public void handle(FedoraMessage m) throws IOException {
        LOG.debug("Handling: " + m);
        
        switch (m.getAction()) {
        case CREATED:
            elasticsearch_add_document(get_fedora_resource(m.getResourceURI()));
            break;
        case DELETED:
            // TODO
            break;
        case MODIFIED:
            // TODO
            break;
        default:
            break;
        }
    }
}
