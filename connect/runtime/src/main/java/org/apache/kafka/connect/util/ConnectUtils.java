/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.util;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.concurrent.ExecutionException;

public final class ConnectUtils {
    private static final Logger log = LoggerFactory.getLogger(ConnectUtils.class);

    private static final int currentEncodeVersion = 1;
    private static final int shiftDistance = 3;
    private static final Gson gson = new Gson();

    public static Long checkAndConvertTimestamp(Long timestamp) {
        if (timestamp == null || timestamp >= 0)
            return timestamp;
        else if (timestamp == RecordBatch.NO_TIMESTAMP)
            return null;
        else
            throw new InvalidRecordException(String.format("Invalid record timestamp %d", timestamp));
    }

    public static String lookupKafkaClusterId(WorkerConfig config) {
        log.info("Creating Kafka admin client");
        try (Admin adminClient = Admin.create(config.originals())) {
            return lookupKafkaClusterId(adminClient);
        }
    }

    static String lookupKafkaClusterId(Admin adminClient) {
        log.debug("Looking up Kafka cluster ID");
        try {
            KafkaFuture<String> clusterIdFuture = adminClient.describeCluster().clusterId();
            if (clusterIdFuture == null) {
                log.info("Kafka cluster version is too old to return cluster ID");
                return null;
            }
            log.debug("Fetching Kafka cluster ID");
            String kafkaClusterId = clusterIdFuture.get();
            log.info("Kafka cluster ID: {}", kafkaClusterId);
            return kafkaClusterId;
        } catch (InterruptedException e) {
            throw new ConnectException("Unexpectedly interrupted when looking up Kafka cluster info", e);
        } catch (ExecutionException e) {
            throw new ConnectException("Failed to connect to and describe Kafka cluster. "
                    + "Check worker's broker connection and security properties.", e);
        }
    }

    /**
     * Encode byte array in order of
     * 1. base64 encode
     * 2. decrement by 3 on each byte
     * to mask sensitive data in Kafka Connect service. Then wrap the version and
     * the final byte array into class
     * TGConnectConfig and convert to JSON.
     */
    public static byte[] encode(byte[] data) {
        byte[] b64EncodedBytes = Base64.getEncoder().encode(data);
        byte[] buffer = new byte[b64EncodedBytes.length];
        for (int i = 0; i < b64EncodedBytes.length; i++) {
            buffer[i] = (byte) (b64EncodedBytes[i] - shiftDistance);
        }
        TGConnectConfig config = new TGConnectConfig(currentEncodeVersion, buffer);
        return gson.toJson(config).getBytes();
    }

    /**
     * If the data is an object of TGConnectConfig and version is
     * currentEncodeVersion, decode byte array in order of
     * 1. increment by 3 on each byte
     * 2. base64 decode
     * to recover the original content.
     *
     * Otherwise, if it's not an object of TGConnectConfig, return the original data
     * as is. If the version is not currentEncodeVersion, throw an error for now. In
     * future, more decode methods will be possibly supported as encoding algorithm
     * evolves.
     */
    public static byte[] decode(byte[] data) {
        TGConnectConfig config;
        try {
            config = gson.fromJson(new String(data), TGConnectConfig.class);
        } catch (JsonSyntaxException e) {
            StringBuilder message = new StringBuilder();
            message.append("[");
            for (int i = 0; i < data.length; i++) {
                message.append(data[i]);
                if (i < data.length - 1) {
                    message.append(" ");
                }
            }
            message.append("]");
            throw new RuntimeException(String.format("Unknown data to decode: %s", message.toString()));
        } catch (NullPointerException e) {
            throw new RuntimeException("Cannot decode null value");
        }

        switch (config.version) {
            case currentEncodeVersion:
                if (config.value == null) {
                    throw new RuntimeException(String.format("Value field is missing. Version: %d", config.version));
                }
                byte[] buffer = new byte[config.value.length];
                for (int i = 0; i < config.value.length; i++) {
                    buffer[i] = (byte) (config.value[i] + shiftDistance);
                }
                return Base64.getDecoder().decode(buffer);
            case 0:
                if (config.value == null) {
                    // not a valid TGConnectConfig, return the raw data as is because it's an old
                    // config value defined by `KafkaConfigBackingStore`
                    return data;
                } else {
                    // otherwise, it contains unknown version
                    throw new RuntimeException(String.format("Unknown version: %d", config.version));
                }
            default:
                throw new RuntimeException(String.format("Unknown version: %d", config.version));
        }
    }

    private static class TGConnectConfig {
        private int version;
        private byte[] value;

        TGConnectConfig(int version, byte[] value) {
            this.version = version;
            this.value = value;
        }
    }
}
