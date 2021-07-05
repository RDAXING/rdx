package accumulator20200820;

import org.apache.spark.sql.sources.In;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PersonAccumulator extends AccumulatorV2<String,Person> implements Serializable{
    private Person person  = new Person(null,0);

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    @Override
    public boolean isZero() {
        return person.getUsers()==null && person.getTotalCount() == 0;
    }

    @Override
    public AccumulatorV2<String, Person> copy() {
        PersonAccumulator personAccumulator = new PersonAccumulator();
        personAccumulator.person = this.person;
        return personAccumulator;
    }

    @Override
    public void reset() {
        person = new Person(null,0);
    }

    @Override
    public void add(String v) {
        String[] value = v.split(" ");
        String name = value[0];
        List<String> list = new ArrayList<>();
        list.add(name);
        if(person.getUsers() == null){
            person.setUsers(list);
        }
        list.addAll(person.getUsers());
        person.setUsers(list);
        person.setTotalCount(person.getTotalCount() + Integer.parseInt(value[1]));
    }

    @Override
    public void merge(AccumulatorV2<String, Person> other) {

        List<String> list = new ArrayList<>();
        Set<String> set = new HashSet<>();
        Person value = other.value();
        list.add("");
        if(this.person.getUsers() == null){
            person.setUsers(list);
        }
        if(value.getUsers() == null){
            value.setUsers(list);
        }
        set.addAll(person.getUsers());
        set.addAll(value.getUsers());
        set.remove("");
        list = new ArrayList<>(set);
        person.setUsers(list);
        person.setTotalCount(person.getTotalCount() + value.getTotalCount());
    }

    @Override
    public Person value() {
        return person;
    }
}
