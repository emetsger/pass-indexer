package org.dataconservancy.pass.indexer;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Main {
    // Check environment variable and then property
    private static String get_config(final String key) {
        String value = System.getenv().get(key);
        
        if (value == null ) {
            value = System.getProperty(key);
        }
        
        if (value == null) {
            System.err.println("Required configuration property is missing: " + key);
            System.exit(1);
        }

        return value;
    }

    public static void main(String[] args) {
        try (FedoraIndexerService serv = new FedoraIndexerService()) {
            serv.setJmsConnectionFactory(new ActiveMQConnectionFactory(get_config("PI_FEDORA_JMS_BROKER")));
            serv.setJmsQueue(get_config("PI_FEDORA_JMS_QUEUE"));
            serv.setElasticsearchIndexUrl(get_config("PI_ES_INDEX"));
            serv.setFedoraUser(get_config("PI_FEDORA_USER"));
            serv.setFedoraPass(get_config("PI_FEDORA_PASS"));
            serv.setAllowedTypePrefix(get_config("PI_TYPE_PREFIX"));

            System.out.println("Starting Fedora indexing service.");

            serv.start();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                System.out.println("Fedora index service interrupted: " + e);
                System.exit(0);
            }
        }
    }
}
