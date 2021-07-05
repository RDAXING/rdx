package accumulator;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class PersonAccumulator extends AccumulatorV2<String,PersonEntry> {

    private PersonEntry personEntry = new PersonEntry(null,0);

    public PersonEntry getPersonEntry() {
        return personEntry;
    }

    public void setPersonEntry(PersonEntry personEntry) {
        this.personEntry = personEntry;
    }


    @Override
    public boolean isZero() {
        return personEntry.getName() == null && personEntry.getTotalCount() == 0;
    }

    @Override
    public AccumulatorV2<String, PersonEntry> copy() {
        PersonAccumulator personAccumulator = new PersonAccumulator();
        personAccumulator.personEntry = this.personEntry;
        return personAccumulator;
    }

    @Override
    public void reset() {
       personEntry =  new PersonEntry(null,0);
    }

    @Override
    public void add(String v) {
        String[] val = v.split(" ");
        String value = val[0];
        List<String> list = new ArrayList<>();
        list.add(value);
        if(personEntry.getName() == null){
            personEntry.setName(list);
        }
        list.addAll(personEntry.getName());
        personEntry.setName(list);
        personEntry.setTotalCount(personEntry.getTotalCount() + Integer.parseInt(val[1]));
    }

    @Override
    public void merge(AccumulatorV2<String, PersonEntry> other) {

        List<String> list = new ArrayList<>();
        list.add("");
        PersonEntry value = other.value();
        if(value.getName() == null){
            value.setName(list);
        }
        if(personEntry.getName() == null){
            personEntry.setName(list);
        }
            value.getName().addAll(personEntry.getName());
        HashSet<String> set = new HashSet<>();
        set.addAll(value.getName());
        set.remove("");
        personEntry.setName(new ArrayList<>(set));
        list.clear();
        set.clear();
        personEntry.setTotalCount(personEntry.getTotalCount() + value.getTotalCount());
    }

    @Override
    public PersonEntry value() {

        return personEntry;
    }
}
