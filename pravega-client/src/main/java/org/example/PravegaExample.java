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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;

public class PravegaExample {
    private static final String SCOPE = "exampleScope";
    private static final String STREAM_NAME = "exampleStream";
    private static final String READER_GROUP = "exampleReaderGroup";
    private static final String READER_NAME = "reader-" + System.currentTimeMillis();
    private static final URI CONTROLLER_URI = URI.create("tcp://127.0.0.1:9090");

    public static void main(String[] args) {
        // Create a scope
        try (StreamManager streamManager = StreamManager.create(CONTROLLER_URI)) {
            streamManager.createScope(SCOPE);

            // Create a stream
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            streamManager.createStream(SCOPE, STREAM_NAME, streamConfig);

            // Write to the stream
            ClientConfig clientConfig = ClientConfig.builder().controllerURI(CONTROLLER_URI).build();
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
            EventStreamWriter<Integer> writer = clientFactory.createEventWriter(
                    STREAM_NAME,
                    new JavaSerializer<>(),
                    EventWriterConfig.builder().build());

            for (int i = 1; i <= 5; i++) {
                writer.writeEvent(String.valueOf(i), i);
                System.out.println("Writing event: " + i);
            }
            writer.close();


            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig)) {
                ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                        .stream(Stream.of(SCOPE, STREAM_NAME))
                        .build();
                readerGroupManager.createReaderGroup(READER_GROUP, readerGroupConfig);

                // Reader
                EventStreamReader<String> reader = clientFactory.createReader(
                        READER_NAME,
                        READER_GROUP,
                        new UTF8StringSerializer(),
                        ReaderConfig.builder().build()
                );

                for (int i = 1; i <= 5; i++) {
                    EventRead<String> eventRead = reader.readNextEvent(1000);
                    String event = eventRead.getEvent();
                    System.out.println("Reading event: " + event);
                }
                reader.close();
            }
        }
    }
}
