package accumulator;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;

public class UserAccumulator extends AccumulatorV2<String,UserEntry> {
    private UserEntry userEntry = new UserEntry(0,0);

    public UserEntry getUserEntry() {
        return userEntry;
    }

    public void setUserEntry(UserEntry userEntry) {
        this.userEntry = userEntry;
    }


    @Override
    public boolean isZero() {
        return userEntry.getUersCount() == 0 && userEntry.getTotalCount() == 0;
    }

    @Override
    public AccumulatorV2<String, UserEntry> copy() {
        UserAccumulator userAccumulator = new UserAccumulator();
        userAccumulator.userEntry = this.userEntry;
        return userAccumulator;
    }

    @Override
    public void reset() {
        userEntry = new UserEntry(0,0);
    }

    @Override
    public void add(String v) {
        String[] split = v.split(" ");
        String key = split[0];
        String value = split[1];
        List<String> list = new ArrayList<>();
        list.add(key);
        userEntry.setUersCount(list.size() + userEntry.getUersCount());
        userEntry.setTotalCount(userEntry.getTotalCount() + Integer.parseInt(value));

    }

    @Override
    public void merge(AccumulatorV2<String, UserEntry> other) {
        UserEntry value = other.value();
        userEntry.setUersCount(userEntry.getUersCount() + value.getUersCount());
        userEntry.setTotalCount(userEntry.getTotalCount() +value.getTotalCount());
    }

    @Override
    public UserEntry value() {
        return userEntry;
    }
}
