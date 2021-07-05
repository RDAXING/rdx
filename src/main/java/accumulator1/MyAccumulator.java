package accumulator1;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.util.AccumulatorV2;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MyAccumulator extends AccumulatorV2<String,String> implements  Serializable {
    private StringBuffer stringBuffer = new StringBuffer();
    @Override
    public boolean isZero() {
        return stringBuffer.length() ==0;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        MyAccumulator myAccumulator = new MyAccumulator();
        myAccumulator.stringBuffer .append(this.stringBuffer);

        return myAccumulator;
    }

    @Override
    public void reset() {
        stringBuffer.setLength(0); //= null;
    }

    @Override
    public void add(String v) {
        stringBuffer.append(v).append(",");
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
       stringBuffer.append(other.value());
    }

    @Override
    public String value() {
        String s = stringBuffer.toString();



        return s;
    }

    @Test
    public void tetssd(){
        String ss = "skdfjskdfjsdf,";
        ss = ss.substring(0,ss.length()-1);
        System.out.println(ss);
    }
}
