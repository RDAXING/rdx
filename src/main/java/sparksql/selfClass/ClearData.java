package sparksql.selfClass;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Array;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ClearData implements PairFlatMapFunction<Iterator<String>, Integer, List<String>> {

    @Override
    public Iterator<Tuple2<Integer, List<String>>> call(Iterator<String> row) throws Exception {
        List<Tuple2<Integer, List<String>>> tuple2s = new ArrayList<>();
        while(row.hasNext()){
            String value = row.next();
            String[] split1 = value.split("\001");
            String key = split1[0];
            String val = split1[1];
            List<String> list = Arrays.asList(val.split(","));
            tuple2s.add(new Tuple2<>(Integer.valueOf(key),list));
        }
        return tuple2s.iterator();
    }
}
