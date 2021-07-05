package accumulator;

import java.io.Serializable;
import java.util.List;

public class PersonEntry implements Serializable {
    private List<String> name;
    private Integer totalCount;

    public List<String> getName() {
        return name;
    }

    public void setName(List<String> name) {
        this.name = name;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public PersonEntry(List<String> name, Integer totalCount) {
        this.name = name;
        this.totalCount = totalCount;
    }

    public PersonEntry() {
    }

    @Override
    public String toString() {
        return "PersonEntry{" +
                "name=" + name +
                ", totalCount=" + totalCount +
                '}';
    }
}
