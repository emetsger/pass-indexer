package pass.indexer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Logging, exception handling
// TODO Batch up index requests?
// TODO Any reason to add a ExecutorService to the mix?

/**
 * Setup a handler that reads Fedora events from a JMS queue and updates an
 * Elasticsearch index in response.
 */
public class FedoraIndexerService {
    private static final Logger LOG = LoggerFactory.getLogger(FedoraIndexerService.class);
    
    private JmsClient jms_client;
    private String jms_broker_url;
    private String jms_queue;
    private Set<String> allowed_types;
    private String elasticsearch_index_url;
    private String fedora_user;
    private String fedora_pass;

    public void setJmsBrokerURL(String jms_broker_url) {
        this.jms_broker_url = jms_broker_url;
    }

    public void setJmsQueue(String jms_queue) {
        this.jms_queue = jms_queue;
    }

    public void setAllowedTypes(String... types) {
        this.allowed_types = new HashSet<>(Arrays.asList(types));
    }

    public void setElasticsearchIndexUrl(String elasticsearch_index_url) {
        this.elasticsearch_index_url = elasticsearch_index_url;
    }

    public void setFedoraUser(String fedora_user) {
        this.fedora_user = fedora_user;
    }

    public void setFedoraPass(String fedora_pass) {
        this.fedora_pass = fedora_pass;
    }
    
    private boolean should_handle(FedoraMessage fedora_msg) {
        for (String type : fedora_msg.getResourceTypes()) {
            if (allowed_types.contains(type)) {
                return true;
            }
        }

        return false;
    }

    public void start() {
        jms_client = new JmsClient(new ActiveMQConnectionFactory(jms_broker_url));

        ElasticsearchIndexer es = new ElasticsearchIndexer(elasticsearch_index_url, fedora_user, fedora_pass);

        jms_client.listen(jms_queue, msg -> {
            try {
                FedoraMessage fedora_msg = FedoraMessageConverter.convert(msg);
                
                boolean should_handle = should_handle(fedora_msg);
                
                LOG.debug("Fedora message:" + fedora_msg + "; handle: " + should_handle);

                if (should_handle) {
                    es.handle(fedora_msg);
                } 
            } catch (IOException | JMSException e) {
                throw new RuntimeException(e);
            }
        });
        
        LOG.info("Started listening on " + jms_broker_url + " queue " + jms_queue);
        LOG.info("Elasticsearch index: " + elasticsearch_index_url);
    }

    public void shutdown() {
        LOG.info("Shutting down JMS client");
        jms_client.close();
    }
}
