import org.apache.spark.util.AccumulatorV2;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapAccumulator extends AccumulatorV2<String,Map<String,Integer>> {
    private Map<String,Integer> map = new HashMap<>();


    @Override
    public boolean isZero() {
        return map.size() == 0;
    }

    @Override
    public AccumulatorV2<String, Map<String,Integer>> copy() {
        MapAccumulator mapAccumulator = new MapAccumulator();
        synchronized (mapAccumulator) {
            mapAccumulator.map.putAll(this.map);
        }

        return mapAccumulator;
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(String v) {
        synchronized (map) {
            Integer value = map.get(v);
            if(map.containsKey(v)){
                map.put(v,value+1);
            }
            map.put(v,1);
        }


    }

    @Override
    public void merge(AccumulatorV2<String, Map<String,Integer>> other) {
        if(other instanceof  MapAccumulator){
            this.map.putAll(((MapAccumulator) other).map);
        }else{
            throw new UnsupportedOperationException("MapAccumulator merge failed");
        }
    }

    @Override
    public Map value() {
        Map<String,Integer> hashMap = new HashMap<>();
        hashMap.putAll(map);
        return hashMap;
    }

    @Test
    public void madeds(){
        Map<String, Integer> ma = new HashMap<>();
        List<String> list = Arrays.asList("a", "b", "c","a", "a", "c");

        for(String name : list){
            if(ma.containsKey(name)){
                ma.put(name,ma.get(name)+1);
                continue;
            }
            ma.put(name,1);
        }
        System.out.println(ma);
    }
}
