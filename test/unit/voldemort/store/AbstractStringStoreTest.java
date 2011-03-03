package voldemort.store;

import java.util.List;

import voldemort.TestUtils;

public abstract class AbstractStringStoreTest extends AbstractStoreTest<String, String, String> {

    @Override
    public List<String> getKeys(int numKeys) {
        return getStrings(numKeys, 10);
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 12);
    }

    @Override
    public String getKey(int size) {
        return TestUtils.randomLetters(size);
    }

    @Override
    public String getValue(int size) {
        return TestUtils.randomLetters(size);
    }

}
