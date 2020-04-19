package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import com.vladykin.replicamap.kafka.impl.worker.flush.FlushWorker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static com.vladykin.replicamap.kafka.impl.util.Utils.MIN_POLL_TIMEOUT_MS;
import static java.util.Collections.singleton;

/**
 * Convenience tools.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
@SuppressWarnings({"ArraysAsListWithZeroOrOneArgument", "SwitchStatementWithTooFewBranches"})
public class KReplicaMapTools {
    public static final String CMD_INIT_EXISTING = "initExisting";
    protected static final List<String> CMDS = Arrays.asList(CMD_INIT_EXISTING);

    protected final Iterator<String> args;

    public KReplicaMapTools(String[] args) {
        this.args = Arrays.asList(args).iterator();
    }

    protected String nextArg(String argName) {
        if (!args.hasNext())
            throw new IllegalArgumentException("Argument '" + argName + "' is not provided.");

        return args.next();
    }

    public String run() throws Exception {
        String cmd = nextArg("command");
        switch (cmd) {
            case CMD_INIT_EXISTING:
                return setupExistingDataTopic(
                    nextArg("bootstrapServers"),
                    nextArg("dataTopic"),
                    nextArg("opsTopic")
                );
            default:
                throw new IllegalArgumentException("Unsupported command: " + cmd + ", supported commands: " + CMDS);
        }
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0)
            System.out.println("Supported commands: " + CMDS);
        else {
            try {
                System.out.println("OK: " + new KReplicaMapTools(args).run());
            }
            catch (IllegalArgumentException e) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }
    }

    public String setupExistingDataTopic(String bootstrapServers, String dataTopic, String opsTopic) throws Exception {
        Map<String, Object> cfg = new HashMap<>();

        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        cfg.put("allow.auto.create.topics", false); // ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        Map<TopicPartition,ConsumerRecord<byte[],byte[]>> lastDataRecords = new HashMap<>();

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(cfg)) {
            Map<TopicPartition,Long> dataEndOffsets = Utils.endOffsets(consumer, dataTopic);

            if (Utils.endOffsets(consumer, opsTopic).values().stream().mapToLong(x -> x).sum() != 0)
                throw new IllegalArgumentException("Non-zero end offsets in ops topic: " + opsTopic);

            List<TopicPartition> dataParts = Utils.partitions(consumer, dataTopic);

            if (dataParts.size() != Utils.partitions(consumer, opsTopic).size())
                throw new IllegalArgumentException("Number of partitions does not match: " + dataTopic + ", " + opsTopic);

            for (TopicPartition dataPart : dataParts) {
                long endOffset = dataEndOffsets.get(dataPart);
                if (endOffset > 0L) {
                    ConsumerRecord<byte[],byte[]> lastDataRec = findLastDataRecord(consumer, dataPart, endOffset);
                    if (lastDataRec != null)
                        lastDataRecords.put(dataPart, lastDataRec);
                }
            }
        }

        if (lastDataRecords.isEmpty())
            throw new IllegalArgumentException("Data topic has no committed records: " + dataTopic);

        cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        List<Future<RecordMetadata>> futs = new ArrayList<>(lastDataRecords.size());

        try (Producer<byte[],byte[]> dataProducer =
                 new KafkaProducer<>(cfg, new ByteArraySerializer(), new ByteArraySerializer())
        ) {
            for (Map.Entry<TopicPartition,ConsumerRecord<byte[],byte[]>> entry : lastDataRecords.entrySet()) {
                TopicPartition dataPart = entry.getKey();
                ConsumerRecord<byte[],byte[]> lastDataRec = entry.getValue();

                // Write the last record once again with header.
                futs.add(dataProducer.send(new ProducerRecord<>(
                    dataTopic, dataPart.partition(),
                    lastDataRec.key(), lastDataRec.value(),
                    Utils.concat(lastDataRec.headers(), FlushWorker.newOpsOffsetHeader(-1L)))));
            }
            dataProducer.flush();

            for (Future<RecordMetadata> fut : futs)
                fut.get();
        }

        return "Found data topic partitions with data: " + lastDataRecords.keySet();
    }

    protected ConsumerRecord<byte[],byte[]> findLastDataRecord(
        Consumer<byte[], byte[]> consumer,
        TopicPartition dataPart,
        long endOffset
    ) {
        consumer.assign(singleton(dataPart));
        consumer.seek(dataPart, endOffset - 1);

        ConsumerRecord<byte[],byte[]> lastDataRec = null;
        boolean reachedZero = false;

        for (int step = 1;;) {
            ConsumerRecords<byte[],byte[]> recs = Utils.poll(consumer, MIN_POLL_TIMEOUT_MS);

            if (recs.isEmpty()) {
                endOffset = Utils.endOffset(consumer, dataPart); // With read_committed endOffset means LSO.

                if (consumer.position(dataPart) == endOffset) {
                    if (lastDataRec != null)
                        return lastDataRec;
                }

                step *= 2;
                long offset = endOffset - step;

                if (offset < 0)
                    offset = 0;

                if (offset == 0) {
                    if (reachedZero)
                        return null; // Nothing found: only uncommitted records.

                    reachedZero = true;
                }

                consumer.seek(dataPart, offset);
                continue;
            }

            for (ConsumerRecord<byte[],byte[]> rec : recs.records(dataPart))
                lastDataRec = rec;
        }
    }
}
