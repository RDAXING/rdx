package accumulator;

import java.io.Serializable;

public class UserEntry implements Serializable {
    private Integer uersCount;
    private Integer totalCount;

    public Integer getTotalCount() {
        return totalCount;
    }

    public Integer getUersCount() {
        return uersCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public void setUersCount(Integer uersCount) {
        this.uersCount = uersCount;
    }

    public UserEntry(Integer uersCount, Integer totalCount) {
        this.uersCount = uersCount;
        this.totalCount = totalCount;
    }

    public UserEntry() {
    }

    @Override
    public String toString() {
        return "uersCount=" + uersCount + ", totalCount=" + totalCount;
    }
}
