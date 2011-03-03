package voldemort.store.memory;

import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class InMemoryStorageEngineSITest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[], byte[]> store;

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return store;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.store = new InMemoryStorageEngineSI("test", secIdxTestUtils.getSecIdxProcessor());
    }

    @Override
    protected boolean isSecondaryIndexEnabled() {
        return true;
    }

}
