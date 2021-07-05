package accumulator20200820;

import org.apache.spark.util.AccumulatorV2;

public class AccumulatorString extends AccumulatorV2<String,String>{

    private StringBuffer sb = new StringBuffer();
    @Override
    public boolean isZero() {
        return sb.length() == 0;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        AccumulatorString as = new AccumulatorString();
        as.sb.append(this.sb);
        return as;
    }

    @Override
    public void reset() {
        sb.setLength(0);
    }

    @Override
    public void add(String v) {
        sb.append(v).append(",");
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        String value = other.value();
        sb.append(value);
    }

    @Override
    public String value() {
        return sb.toString();
    }
}
