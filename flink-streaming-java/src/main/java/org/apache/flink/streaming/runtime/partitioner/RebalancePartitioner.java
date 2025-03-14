/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Partitioner that distributes the data equally by cycling through the output channels.
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 */
@Internal
public class RebalancePartitioner<T> extends StreamPartitioner<T> {
    private static final long serialVersionUID = 1L;

    private final boolean loadBasedChannelSelectorEnabled;

    private int nextChannelToSendTo;

    public RebalancePartitioner() {
        this(false);
    }

    public RebalancePartitioner(boolean loadBasedChannelSelectorEnabled) {
        this.loadBasedChannelSelectorEnabled = loadBasedChannelSelectorEnabled;
    }

    @Override
    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);

        nextChannelToSendTo = ThreadLocalRandom.current().nextInt(numberOfChannels);
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        return nextChannelToSendTo;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ROUND_ROBIN;
    }

    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "REBALANCE";
    }

    @Override
    public boolean isLoadBasedChannelSelectorEnabled() {
        return loadBasedChannelSelectorEnabled;
    }
}
