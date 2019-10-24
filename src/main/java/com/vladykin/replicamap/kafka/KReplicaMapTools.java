package com.vladykin.replicamap.kafka;

import com.vladykin.replicamap.kafka.impl.msg.OpMessage;
import com.vladykin.replicamap.kafka.impl.msg.OpMessageSerializer;
import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;

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

    public String run() {
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

    public static void main(String... args) {
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

    public String setupExistingDataTopic(String bootstrapServers, String dataTopic, String opsTopic) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        cfg.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "replicamap_tools_" + UUID.randomUUID());

        Map<TopicPartition,Long> dataOffsets;
        try (Consumer<Object, Object> consumer = new KafkaConsumer<>(cfg)) {
            dataOffsets = Utils.endOffsets(consumer, dataTopic);

            if (Utils.endOffsets(consumer, opsTopic).values().stream().mapToLong(x -> x).sum() != 0)
                throw new IllegalArgumentException("Non-zero end offsets in ops topic: " + opsTopic);
        }

        dataOffsets = new ConcurrentHashMap<>(dataOffsets);

        for (Map.Entry<TopicPartition,Long> entry : dataOffsets.entrySet()) {
            if (entry.getValue() == 0L)
                dataOffsets.remove(entry.getKey());
        }

        if (dataOffsets.isEmpty())
            throw new IllegalArgumentException("Data topic has no records: " + dataTopic);

        cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (Producer<byte[],OpMessage> producer = new KafkaProducer<>(cfg, new ByteArraySerializer(),
            new OpMessageSerializer<>(new ByteArraySerializer()))
        ) {
            for (Map.Entry<TopicPartition,Long> entry : dataOffsets.entrySet()) {
                producer.send(new ProducerRecord<>(opsTopic, entry.getKey().partition(), null,
                    new OpMessage(OP_FLUSH_NOTIFICATION, 0L, entry.getValue() - 1, 0, 0L)));
            }
            producer.flush();
        }

        return "Found data topic end offsets: " + dataOffsets;
    }
}
