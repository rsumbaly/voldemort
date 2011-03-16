package voldemort.store.routed;

import java.util.Set;

import voldemort.utils.ByteArray;

import com.google.common.collect.Sets;

public class GetAllKeysPipelineData extends BasicPipelineData<Set<ByteArray>> {

    private final Set<ByteArray> result = Sets.newHashSet();

    public Set<ByteArray> getResult() {
        return result;
    }

}
