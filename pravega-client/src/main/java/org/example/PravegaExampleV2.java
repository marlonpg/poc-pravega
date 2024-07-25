package org.example;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;

public class PravegaExampleV2 {
    private static final String SCOPE = "examples";
    private static final String STREAM_NAME = "helloStream";
    private static final String READER_GROUP = "readerGroup";
    private static final String READER_NAME = "helloReader-" + System.currentTimeMillis();
    private static final URI CONTROLLER_URI = URI.create("tcp://localhost:9090");

    public static void main(String[] args) {

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        //Create the scope and stream
        try (StreamManager streamManager = StreamManager.create(CONTROLLER_URI)) {
            streamManager.createScope(SCOPE);
            streamManager.createStream(SCOPE, STREAM_NAME, streamConfig);
        }

        //Write events to that Stream
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(CONTROLLER_URI).build());
             EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME, new UTF8StringSerializer(), EventWriterConfig.builder().build())) {
            for (int i = 1; i <= 5; i++) {
                writer.writeEvent("helloRoutingKey", String.valueOf(i));
                System.out.println("Writing event: " + i);
            }
        }

        //Create reader group
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, CONTROLLER_URI)) {
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(SCOPE, STREAM_NAME))
                    .disableAutomaticCheckpoints()
                    .build();
            readerGroupManager.createReaderGroup(READER_GROUP, readerGroupConfig);
        }

        //Read events from that Stream
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(CONTROLLER_URI).build());
             EventStreamReader<String> reader = clientFactory.createReader(READER_NAME,
                     READER_GROUP,
                     new UTF8StringSerializer(),
                     ReaderConfig.builder().build())) {
            for (int i = 1; i <= 5; i++) {
                EventRead<String> eventRead = reader.readNextEvent(1000);
                String event = eventRead.getEvent();
                System.out.println("Reading event: " + event);
            }
        }

    }
}
