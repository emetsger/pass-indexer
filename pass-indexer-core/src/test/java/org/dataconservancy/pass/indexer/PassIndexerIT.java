package org.dataconservancy.pass.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * IT that depends on docker setup in docker-compose.yml.
 */
public class PassIndexerIT implements IndexerConstants {
	private static final String fedora_base_uri = "http://localhost:8080/fcrepo/rest/";
	private static final String es_uri = "http://localhost:9200/_search";
	private static final MediaType JSON_LD = MediaType.parse("application/ld+json; charset=utf-8");
	private static final int WAIT_TIME = 10 * 1000;

	private static FedoraIndexerService serv;
	private static OkHttpClient client;
	private static String fedora_cred;

	@BeforeClass
	public static void setup() throws Exception {
		serv = new FedoraIndexerService();

		serv.setJmsConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:61616"));
		serv.setJmsQueue("fedora");
		serv.setElasticsearchIndexUrl("http://localhost:9200/pass/");
		serv.setFedoraUser("admin");
		serv.setFedoraPass("moo");
		serv.setAllowedTypePrefix("http://example.org/pass/");

		serv.start();

		client = new OkHttpClient();

		fedora_cred = Credentials.basic("admin", "moo");
	}

	@AfterClass
	public static void cleanup() {
		serv.close();
	}

	// Create a Fedora resource and return the assigned URI.
	private String post_fedora_resource(String container_name, JSONObject content) throws Exception {
		String uri = fedora_base_uri + container_name;

		RequestBody body = RequestBody.create(JSON_LD, content.toString());

		Request post = new Request.Builder().url(uri).header("Authorization", fedora_cred).post(body).build();

		try (Response response = client.newCall(post).execute()) {
			if (response.isSuccessful()) {
				return response.header("Location");
			} else {
				throw new IOException("Failed to create: " + uri + ", response: " + response.body().string());
			}
		}
	}

	// Update a Fedora resource
	private void put_fedora_resource(String uri, JSONObject content) throws Exception {
		RequestBody body = RequestBody.create(JSON_LD, content.toString());

		Request put = new Request.Builder().url(uri).header("Authorization", fedora_cred)
				.header("Prefer", FEDORA_PREFER_HEADER).put(body).build();

		try (Response response = client.newCall(put).execute()) {
			if (!response.isSuccessful()) {
				throw new IOException("Failed to update: " + uri + ", response: " + response.body().string());
			}
		}
	}

	// Delete a Fedora resource, leaving a tombstone.
	private void delete_fedora_resource(String uri) throws Exception {
		Request delete = new Request.Builder().url(uri).header("Authorization", fedora_cred).delete().build();

		try (Response response = client.newCall(delete).execute()) {
			if (!response.isSuccessful()) {
				throw new IOException("Failed to delete: " + uri + ", response: " + response.body().string());
			}
		}
	}

	// Execute an Elasticsearch query and return the result
	private JSONObject execute_es_query(JSONObject query) throws Exception {
		RequestBody body = RequestBody.create(JSON, query.toString());

		Request post = new Request.Builder().url(es_uri).post(body).build();

		try (Response response = client.newCall(post).execute()) {
			if (response.isSuccessful()) {
				return new JSONObject(response.body().string());
			} else {
				throw new IOException("Failed to execute query: " + query + ", response: " + response.body().string());
			}
		}
	}

	private JSONObject create_pass_object(String type) {
		JSONObject result = new JSONObject();

		// Must set @id to ""
		result.put("@id", "");
		result.put("@context", "https://oa-pass.github.io/pass-data-model/src/main/resources/context-2.0.jsonld");
		result.put("@type", type);

		return result;
	};

	private JSONObject create_term_query(String field, String value) {
		JSONObject query = new JSONObject();
		JSONObject term_match = new JSONObject();
		JSONObject term_value = new JSONObject();

		term_value.put(field, value);
		term_match.put("term", term_value);
		query.put("query", term_match);

		return query;
	}

	private JSONObject execute_es_id_query(String uri) throws Exception {
		JSONObject result = execute_es_query(create_term_query("@id", uri));

		JSONArray hits = result.getJSONObject("hits").getJSONArray("hits");

		if (hits.length() != 0 && hits.length() != 1) {
			assertTrue("A fedora resource should have at most one Es document: " + uri, false);
		}

		return result;
	}

	private JSONObject get_indexed_fedora_resource(String uri) throws Exception {
		JSONObject result = execute_es_id_query(uri);

		JSONArray hits = result.getJSONObject("hits").getJSONArray("hits");

		if (hits.length() == 0) {
			assertTrue("No Elasticsearch document: " + uri, false);
		}

		return hits.getJSONObject(0).getJSONObject("_source");
	}

	private boolean is_fedora_resource_indexed(String uri) throws Exception {
		JSONObject result = execute_es_id_query(uri);
		JSONArray hits = result.getJSONObject("hits").getJSONArray("hits");

		return hits.length() == 1;
	}

	// Check that Elasticsearch document is created for a Fedora User resource.
	// When the Fedora resource is updated, so is the document.
	@Test
	public void testCreateAndModify() throws Exception {
		JSONObject user = create_pass_object("User");
		user.put("username", "cow1");
		user.put("email", "moo@example.org");

		String uri = post_fedora_resource("users", user);
		Thread.sleep(WAIT_TIME);

		// Check Es document
		{
			JSONObject result = get_indexed_fedora_resource(uri);

			user.put("@id", uri);

			user.keySet().forEach(key -> {
				assertEquals(result.get(key), user.get(key));
			});
		}

		// Update the Fedora resource
		user.put("displayName", "Bob");

		put_fedora_resource(uri, user);
		Thread.sleep(WAIT_TIME);

		// Check the Es document
		{
			JSONObject result = get_indexed_fedora_resource(uri);

			user.keySet().forEach(key -> {
				assertEquals(result.get(key), user.get(key));
			});
		}
	}

	// Create and then delete a policy object.
	// The Es document for the object is also deleted.
	@Test
	public void testDelete() throws Exception {
		
		JSONObject policy = create_pass_object("Policy");
		policy.put("title", "The best policy");

		String uri = post_fedora_resource("policies", policy);
		Thread.sleep(WAIT_TIME);
		
		assertTrue(is_fedora_resource_indexed(uri));
		
		delete_fedora_resource(uri);
		Thread.sleep(WAIT_TIME);

		assertFalse(is_fedora_resource_indexed(uri));
	}

	// Show that Fedora resources can be searched for by resource path.
	// See the Es mapping configuration for pass.
	@Test
	public void testSearchByFedoraURI() throws Exception {
		JSONObject policy = create_pass_object("Policy");
		policy.put("title", "The worst  policy");

		String uri = post_fedora_resource("policies", policy);
		Thread.sleep(WAIT_TIME);
		
		assertTrue(is_fedora_resource_indexed(uri));
		
		String marker = "/rest";
		int i = uri.indexOf(marker);
		String path = uri.substring(i + + marker.length());
		System.err.println(path);

		assertTrue(is_fedora_resource_indexed(path));
	}
}
