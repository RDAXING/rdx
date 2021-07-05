package accumulator20200820;

import java.io.Serializable;

public class MyKey implements Serializable {
    private Integer userCount;
    private Integer totalCount;

    public Integer getTotalCount() {
        return totalCount;
    }

    public Integer getUserCount() {
        return userCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public void setUserCount(Integer userCount) {
        this.userCount = userCount;
    }

    public MyKey(Integer userCount, Integer totalCount) {
        this.userCount = userCount;
        this.totalCount = totalCount;
    }

    public MyKey() {
    }

    @Override
    public String toString() {
        return  "userCount=" + userCount +
                ", totalCount=" + totalCount;
    }
}
