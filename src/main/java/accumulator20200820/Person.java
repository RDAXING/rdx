package accumulator20200820;

import java.io.Serializable;
import java.util.List;

public class Person implements Serializable {
    private List<String> users;
    private Integer totalCount;

    public List<String> getUsers() {
        return users;
    }

    public void setUsers(List<String> users) {
        this.users = users;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Person(List<String> users, Integer totalCount) {
        this.users = users;
        this.totalCount = totalCount;
    }

    public Person() {
    }

    @Override
    public String toString() {
        return "users=" + users + ", totalCount=" + totalCount ;
    }
}
