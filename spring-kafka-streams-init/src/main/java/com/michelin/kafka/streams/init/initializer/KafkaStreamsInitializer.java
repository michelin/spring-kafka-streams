package com.michelin.kafka.streams.init.initializer;

import com.michelin.kafka.streams.init.model.TopologyExposeJsonModel;
import com.michelin.kafka.streams.init.properties.KafkaProperties;
import com.michelin.kafka.streams.init.services.ConvertTopology;
import com.michelin.kafka.streams.starter.commons.context.KafkaStreamsExecutionContext;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.function.Consumer;

@Slf4j
@Component
public class KafkaStreamsInitializer {
    @Autowired
    protected ConfigurableApplicationContext applicationContext;

    @Autowired
    protected KafkaProperties kafkaProperties;

    protected KafkaStreams kafkaStreams;

    protected Topology topology;

    protected void init(Consumer<StreamsBuilder> topologyFunction, String dlqTopicName) {
        initStreamExecutionContext(dlqTopicName);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topologyFunction.accept(streamsBuilder);

        topology = streamsBuilder.build();

        log.info("Description of the topology:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, KafkaStreamsExecutionContext.getProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    kafkaProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);
            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                        kafkaProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

                applicationContext.close();
            }
        });

        kafkaStreams.start();
    }

    private void initStreamExecutionContext(String dlqTopicName) {
        KafkaStreamsExecutionContext.setDlqTopicName(dlqTopicName);
        KafkaStreamsExecutionContext.registerProperties(kafkaProperties.asProperties());
        KafkaStreamsExecutionContext.setSerdesConfig(kafkaProperties.getProperties());
    }

    @GetMapping("expose-topology")
    public ResponseEntity<TopologyExposeJsonModel> exposeTopology() {
        if (topology != null) {
            return ResponseEntity.ok(ConvertTopology.convertTopologyForRest(kafkaProperties.getProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), topology));
        }
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    @GetMapping("dropthemic/{debounceParam}")
    public ResponseEntity<String> dropTheMic(@PathVariable() String debounceParam) {
        long debounce = 1L;

        if (!StringUtils.isBlank(debounceParam)) {
            debounce = Long.parseLong(debounceParam);
        }

        if (kafkaStreams != null) {
            log.info("Clean up and close in {}s", debounce);
            kafkaStreams.close();
            try {
                kafkaStreams.cleanUp();
            } catch (IllegalStateException ex) {
                log.warn("Error on state dir clean up", ex);
            }

            long shutdownDebounce = debounce * 1000L;
            var springClose = new Thread(() -> {
                try {
                    log.info("Shutdown scheduled in {}",shutdownDebounce);
                    Thread.sleep(shutdownDebounce);
                } catch (InterruptedException e) {
                    log.warn("Error while waiting for stop", e);
                    Thread.currentThread().interrupt();
                }

                if (applicationContext != null) {
                    applicationContext.close();
                } else{
                    log.warn("No Spring context set");
                }
            });

            springClose.start();
            return new ResponseEntity<>("See you space cowboy", HttpStatus.I_AM_A_TEAPOT);
        }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    @GetMapping("ready")
    public ResponseEntity<String> readinessProbe() {
        if (kafkaStreams != null) {
            log.debug("Current state of the Kafka Stream {}: {}",
                    KafkaStreamsExecutionContext.getProperties().getProperty(StreamsConfig.APPLICATION_ID_CONFIG),
                    kafkaStreams.state());

            if (kafkaStreams.state() == KafkaStreams.State.REBALANCING) {
                long startingThreadCount = kafkaStreams.metadataForLocalThreads()
                        .stream()
                        .filter(t -> StreamThread.State.STARTING.name().compareToIgnoreCase(t.threadState()) == 0 || StreamThread.State.CREATED.name().compareToIgnoreCase(t.threadState()) == 0)
                        .count();

                if (startingThreadCount == kafkaStreams.metadataForLocalThreads().size()) {
                    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
                }
            }

            return kafkaStreams.state().isRunningOrRebalancing() ?
                    ResponseEntity.ok().build() : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        return ResponseEntity.badRequest().build();
    }

    @GetMapping("liveness")
    public ResponseEntity<String> livenessProbe() {
        if (kafkaStreams != null) {
            return kafkaStreams.state() != KafkaStreams.State.NOT_RUNNING ? ResponseEntity.ok().build()
                    : ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

}
