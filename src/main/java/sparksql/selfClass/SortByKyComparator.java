package sparksql.selfClass;

import java.io.Serializable;
import java.util.Comparator;

public class SortByKyComparator  implements Serializable,Comparator<Integer> {


    @Override
    public int compare(Integer o1, Integer o2) {
        if (o1 - o2 > 0) {
            return -1;
        } else if (o1 - o2 < 0)
            return 1;
        else return 0;
    }
}
