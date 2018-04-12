package org.dataconservancy.pass.indexer;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


// TODO Auto-complete
// TODO Schema
// TODO Always throw exception on failure? Needed to inform JMS what happened?
// TODO Issues with public vs private Fedora URI

/**
 * Maintain documents corresponding to the JSON-LD representation of Fedora resources.
 * The compact JSON-LD representation must be simple enough for Elasticsearch to understand.
 * Documents are updated when Fedora resources are created or modified or deleted.
 * The document id is the path of the Fedora URI encoded as base64 safe for URLs.
 */
public class ElasticSearchIndexer {
    public static final String FEDORA_ACCEPT_HEADER = "application/ld+json; profile=\"http://www.w3.org/ns/json-ld#compacted\"";
    public static final String FEDORA_PREFER_HEADER = "return=representation; omit=\"http://fedora.info/definitions/v4/repository#ServerManaged\"";
    
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchIndexer.class);

    private final OkHttpClient client;
    private final String fedora_cred;
    private final String es_index_url;

    public ElasticSearchIndexer(String es_index_url, String fedora_user, String fedora_pass) {
        this.client = new OkHttpClient();
        this.es_index_url = es_index_url;
        this.fedora_cred = Credentials.basic(fedora_user, fedora_pass);
    }

    // Return compact JSON-LD representation of Fedora resource without server triples
    private String get_fedora_resource(String uri) throws IOException {
        Request get = new Request.Builder().url(uri).header("Authorization", fedora_cred)
                .header("Accept", FEDORA_ACCEPT_HEADER).header("Prefer", FEDORA_PREFER_HEADER).build();

        Response response = client.newCall(get).execute();

        if (!response.isSuccessful()) {
            LOG.error("Failed to retrieve Fedora resource: " + uri + " " + response.code());
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
    private String normalize_document(String json) {
        JSONObject o = new JSONObject(json);

        // TODO For the moment remove inline @context which Elasticsearch cannot
        // handle
        o.remove("@context");

        return o.toString();
    }

    // Create or update the document corresponding to a Fedora resource in
    // Elasticsearch.
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
            LOG.error("Update failure: " + result);
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
