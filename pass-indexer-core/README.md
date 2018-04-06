# Simple JMS client

This module contains a simple JMS client for reading and writing to queues.

## Lifecycle
The primary class of interest is [JmsClient](rmap-loader-jms/src/main/java/info/rmapproject/loader/jms/JmsClient.java).  It can be initialized by providing a JMS ConnectionFactory.

    JmsClient client = new JMSClient(connectionFactory)
    
If created via the no-arg constructor, then `init()` needs to be called in order to create the appropriate JMS connections:

    JmsClient client = new JmsClient();
    clent.setConnectionFactory(connectionFactory);
    client.init();
    
To clean up resources and close connections, use `close()`.  It implements AutoCloseable, so can be used in a try-with-resources block

    try (JMSClient client = new JmsClient(connectionFactory) {
        // do lots of stuff
    }

JMSClient is _not_ guaranteed safe for use by multiple threads.  It _is_ intended to be long-lived, so a single JMSClient can be kept open indefinitely.

## Writing to queues

The loader/harvest framework provides a [HarvestRecord](rmap-loader-api/src/main/java/info/rmapproject/loader/HarvestRecord.java) abstraction for encapsulating a record (as bytes), plus simple metadata.  The JMS client provides a [HarvestRecordWriter](rmap-loader-jms/src/main/java/info/rmapproject/loader/jms/HarvestRecordWriter.java) for conveniently writing these to a queue.

First, a HarvestRecordWriter is initialized by giving it a JMSClient:

    HarvestRecordWriter writer = new HarvestRecordWriter(jmsClient);
    
Then, create a HarvestRecord, and populate it

    HarvestRecord record = new HarvestRecord();
    record.setBody(body.toByteArray());
    
Now, pick a queue and send

    writer.write("rmap.harvest.oai_dc.whatever.2015-10-10", record);
    
If it completes without exception, then the record is durably persisted to the queue.

## Reading from queues

Reading is performed by providing a callback that is invoked whenever a message is recieved for a given queue.  Behind the scenes, a listener thread awaits messages and asynchronously invokes callbacks.  The JMS Client provides a [HarvestRecordListener](rmap-loader-jms/src/main/java/info/rmapproject/loader/jms/HarvestRecordListener.java) that makes it convenient to use the [HarvestRecord](rmap-loader-api/src/main/java/info/rmapproject/loader/HarvestRecord.java) abstraction.

HarvestRecordListeners are initialized by invoking a static factory method `onHarvestRecord`, and providing a `Consumer<HarvestRecord>`. The HarvestRecordListener is then given to a JMSClient via its `listen(queue, listener)` method.   This works particularly well with lambdas as follows:

    import static info.rmapproject.loader.jms.HarvestRecordListener.onHarvestRecord
    ...
    
    jmsClient.listen("rmap.harvest.oai_dc.>", onHarvestRecord(receivedHarvestRecord -> {
        // Do domething with the HarvestRecord recievedHarvestRecord
    });
    
    // Since listening is asynchronous, jmsClient.listen() will return immediately.
    
If the callback throws an exception, the message will be placed back onto the queue.  That way, it's not lost.  This may not always be desirable, however.  A more cautious approach is to place any messages that cause an exception into an error queue.  The JMS Client provides a callback that can be invoked upon exception to make this a little easier:

    jmsClient.listen("rmap.harvest.oai_dc.>", onHarvestRecord(receivedHarvestRecord -> {
        // Do domething with the HarvestRecord recievedHarvestRecord
        // Let's say it throws an exception
    }).withExceptionHandler((message, exception) -> {
        // message is a raw JMS Message, exception is a Java Exception
        // To send to an error queue, it's easiest to directly invoke JmsClient not use the HarvestRecord abstraction
        
        LOG.info("Uh oh, got an exception", exception)
        jms.write("rmap.harvest.error.oai_dc.something", message);
    });
