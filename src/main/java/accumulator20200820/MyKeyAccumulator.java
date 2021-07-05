package accumulator20200820;

import org.apache.spark.util.AccumulatorV2;

public class MyKeyAccumulator extends AccumulatorV2<MyKey,MyKey> {
    private MyKey myKey = new MyKey(0,0);

    public MyKey getMyKey() {
        return myKey;
    }

    public void setMyKey(MyKey myKey) {
        this.myKey = myKey;
    }

    @Override
    public boolean isZero() {
        return myKey.getUserCount() == 0 && myKey.getTotalCount() ==0;
    }

    @Override
    public AccumulatorV2<MyKey, MyKey> copy() {
        MyKeyAccumulator myKeyAccumulator = new MyKeyAccumulator();
        myKeyAccumulator.myKey = this.myKey;
        return myKeyAccumulator;
    }

    @Override
    public void reset() {
        myKey = new MyKey(0,0);
    }

    @Override
    public void add(MyKey v) {
        myKey.setUserCount(myKey.getUserCount() + v.getUserCount());
        myKey.setTotalCount(myKey.getTotalCount() + v.getTotalCount());
    }

    @Override
    public void merge(AccumulatorV2<MyKey, MyKey> other) {
        MyKey value = other.value();
        myKey.setUserCount(myKey.getUserCount() + value.getUserCount());
        myKey.setTotalCount(myKey.getTotalCount() + value.getTotalCount());
    }

    @Override
    public MyKey value() {
        return myKey;
    }
}
