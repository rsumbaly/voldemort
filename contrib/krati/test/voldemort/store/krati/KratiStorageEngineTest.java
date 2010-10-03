package voldemort.store.krati;

import java.io.File;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class KratiStorageEngineTest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[]> store = null;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        File storeDir = TestUtils.createTempDir();
        storeDir.mkdirs();
        storeDir.deleteOnExit();
        this.store = new KratiStorageEngine("storeName", 1000, 8, 8, 50, 2, storeDir);
    }

    @Override
    public StorageEngine<ByteArray, byte[]> getStorageEngine() {
        return this.store;
    }

    @Override
    public void tearDown() {
        store.truncate();
    }

}
