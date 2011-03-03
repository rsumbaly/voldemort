/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.routed.action;

import java.util.List;
import java.util.Map;

import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.GetAllKeysPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

import com.google.common.collect.Lists;

public class GetAllKeysConfigureNodes extends
        AbstractConfigureNodes<ByteArray, List<ByteArray>, GetAllKeysPipelineData> {

    private final int preferred;

    private final Zone clientZone;

    private final Map<ByteArray, byte[]> transforms;

    public GetAllKeysConfigureNodes(GetAllKeysPipelineData pipelineData,
                                    Event completeEvent,
                                    FailureDetector failureDetector,
                                    int preferred,
                                    int required,
                                    RoutingStrategy routingStrategy,
                                    Map<ByteArray, byte[]> transforms,
                                    Zone clientZone) {
        super(pipelineData, completeEvent, failureDetector, required, routingStrategy);
        this.preferred = preferred;
        this.transforms = transforms;
        this.clientZone = clientZone;
    }

    public void execute(Pipeline pipeline) {
        pipelineData.setNodes(Lists.newArrayList(routingStrategy.getNodes()));
        pipeline.addEvent(completeEvent);
    }

}
