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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class ConnectUtilsTest {

    @Test
    public void testLookupKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1);

        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupNullKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

        assertNull(ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test(expected = ConnectException.class)
    public void testLookupKafkaClusterIdTimeout() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1);
        adminClient.timeoutNextRequest(1);

        ConnectUtils.lookupKafkaClusterId(adminClient);
    }

    @Test
    public void testEncodeAndDecode() {
        String[] testCases = new String[] {
                "",
                "hello world",
                "{\"password\":\"some-secret\"}"
        };
        String[] expectedEncodeResult = new String[] {
                "{\"version\":1,\"value\":[]}",
                "{\"version\":1,\"value\":[94,68,83,112,95,68,53,100,97,47,54,118,95,68,78,58]}",
                "{\"version\":1,\"value\":[98,118,71,116,86,85,75,119,97,47,54,118,87,64,70,51,70,107,75,115,95,84,82,113,96,47,83,103,96,106,83,45,70,107,45,58]}"
        };
        for (int i = 0; i < testCases.length; i++) {
            String testCase = testCases[i];
            byte[] input = testCase.getBytes();
            byte[] encodeResult = ConnectUtils.encode(input);
            assertEquals(expectedEncodeResult[i], new String(encodeResult));
            byte[] decodeResult = ConnectUtils.decode(encodeResult);
            assertArrayEquals(input, decodeResult);
        }
    }

    @Test
    public void testDecodeEdgeCases() {
        byte[] nonVersionBytes = "{\"foo\":\"bar\"}".getBytes();
        byte[] decodeResult1 = ConnectUtils.decode(nonVersionBytes);
        assertArrayEquals(nonVersionBytes, decodeResult1);

        byte[] oddVersionBytes1 = "{\"version\":123,\"value\":[94,68,83,112,95,68,53,100,97,47,54,118,95,68,78,58]}"
                .getBytes();
        assertThrows("Expect RuntimeException thrown.", RuntimeException.class,
                () -> ConnectUtils.decode(oddVersionBytes1));

        byte[] oddVersionBytes2 = "{\"version\":0,\"value\":[94,68,83,112,95,68,53,100,97,47,54,118,95,68,78,58]}"
                .getBytes();
        assertThrows("Expect RuntimeException thrown.", RuntimeException.class,
                () -> ConnectUtils.decode(oddVersionBytes2));

        byte[] nonNumVersionBytes = "{\"version\":\"hello\",\"value\":[94,68,83,112,95,68,53,100,97,47,54,118,95,68,78,58]}"
                .getBytes();
        assertThrows("Expect RuntimeException thrown.", RuntimeException.class,
                () -> ConnectUtils.decode(nonNumVersionBytes));

        // null input
        assertThrows("Expect RuntimeException thrown.", RuntimeException.class, () -> ConnectUtils.decode(null));

        // case that has correct version number but no value
        byte[] correctVersionBytes = "{\"version\":1,\"foo\":\"bar\"}".getBytes();
        assertThrows("Expect RuntimeException thrown.", RuntimeException.class,
                () -> ConnectUtils.decode(correctVersionBytes));
    }
}
