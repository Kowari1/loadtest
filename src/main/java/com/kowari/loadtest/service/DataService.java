package com.kowari.loadtest.service;

import com.kowari.loadtest.kafka.consumer.KafkaMessageConsumer;
import com.kowari.loadtest.kafka.producer.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class DataService {

    private static final int BATCH_SIZE = 100;

    private final ConcurrentLinkedQueue<String> queue =
            new ConcurrentLinkedQueue<>();

    private final ExecutorService executor =
            Executors.newSingleThreadExecutor();

    private final AtomicBoolean workerRunning =
            new AtomicBoolean(false);

    private final KafkaProducer kafkaProducer;
    private final KafkaMessageConsumer kafkaConsumer;

    public DataService(
            KafkaProducer kafkaProducer,
            KafkaMessageConsumer kafkaConsumer) {

        this.kafkaProducer = kafkaProducer;
        this.kafkaConsumer = kafkaConsumer;
    }

    public void add(String id) {
        queue.add(id);

        if (queue.size() >= BATCH_SIZE) {
            startWorker();
        }
    }

    private void startWorker() {
        if (!workerRunning.compareAndSet(false, true)) {
            return;
        }

        executor.submit(this::processBatches);
    }

    private void processBatches() {
        try {
            while (queue.size() >= BATCH_SIZE) {

                List<String> batch = new ArrayList<>(BATCH_SIZE);
                var iterator = queue.iterator();

                for (int i = 0; i < BATCH_SIZE; i++) {
                    batch.add(iterator.next());
                }

                kafkaProducer.sendBatch(batch);

                for (int i = 0; i < BATCH_SIZE; i++) {
                    queue.poll();
                }
            }
        } finally {
            workerRunning.set(false);

            if (queue.size() >= BATCH_SIZE) {
                startWorker();
            }
        }
    }

    public List<String> getNewData() {
        return kafkaConsumer.readNewMessages();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
