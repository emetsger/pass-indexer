package pass.indexer;

import org.junit.Test;

public class FedoraTest {

    @Test
    public void test() throws Exception {
        FedoraIndexerService serv = new FedoraIndexerService();
        
        serv.setJmsBrokerURL("tcp://localhost:61616");
        serv.setJmsQueue("fedora");
        serv.setElasticsearchIndexUrl("http://localhost:9200/pass/_doc/?pretty");
        serv.setFedoraUser("admin");
        serv.setFedoraPass("moo");
        serv.setAllowedTypes("http://example.org/pass/Grant", "http://example.org/pass/Submission");
        
        serv.start();
        Thread.sleep(30 * 1000);
        serv.shutdown();        
    }
}
