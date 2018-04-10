package org.dataconservancy.pass.indexer;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class FedoraIT {

    @Test
    public void test() throws Exception {
        FedoraIndexerService serv = new FedoraIndexerService();
        
        serv.setJmsConnectionFactory(new ActiveMQConnectionFactory("tcp://localhost:61616"));
        serv.setJmsQueue("fedora");
        serv.setElasticsearchIndexUrl("http://localhost:9200/pass/");
        serv.setFedoraUser("admin");
        serv.setFedoraPass("moo");
        serv.setAllowedTypePrefix("http://example.org/pass/");
        
        serv.start();
        Thread.sleep(300 * 1000);
        serv.close();       
    }
}
