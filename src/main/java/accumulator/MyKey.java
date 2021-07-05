package accumulator;

import java.io.Serializable;

public class MyKey implements Serializable {
    private Integer personNum;
    private Integer personAgeSum;

    public MyKey() {
    }

    public MyKey(Integer personNum, Integer personAgeSum) {
        this.personNum = personNum;
        this.personAgeSum = personAgeSum;
    }

    public Integer getPersonNum() {
        return personNum;
    }

    public void setPersonNum(Integer personNum) {
        this.personNum = personNum;
    }

    public Integer getPersonAgeSum() {
        return personAgeSum;
    }

    public void setPersonAgeSum(Integer personAgeSum) {
        this.personAgeSum = personAgeSum;
    }

    @Override
    public String toString() {
        return "MyKey{" +
                "personNum=" + personNum +
                ", personAgeSum=" + personAgeSum +
                '}';
    }
}
