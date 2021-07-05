import org.apache.spark.util.AccumulatorV2;

/**
 * spark 2.3
 * 自定义累计器需要继承AccumulatorV2，并且重写以下方法
 * 将符合条件的数据拼接在一起
 */
public class MyAccumulator extends AccumulatorV2<String,String> {

    private StringBuffer stringBuffer = new StringBuffer();
    /**
     * Returns if this accumulator is zero value or not.
     * 返回该累加器是否为零值。
     * @return
     */
    @Override
    public boolean isZero() {
        return stringBuffer.length() == 0;
    }

    /**
     * Creates a new copy of this accumulator.
     * @return
     */
    @Override
    public AccumulatorV2<String,String> copy() {
        MyAccumulator newMyAccu = new MyAccumulator();
        newMyAccu.stringBuffer.append(stringBuffer);
        return newMyAccu;
    }

    /**
     * Resets this accumulator, which is zero value.
     */
    @Override
    public void reset() {
        stringBuffer.setLength(0);
    }

    /**
     * Takes the inputs and accumulates.
     * @param input
     */
    @Override
    public void add(String input) {
        stringBuffer.append(input).append(",");
    }

    /**
     * Merges another same-type accumulator into this one and update its state, i.e.
     * @param other
     */
    @Override
    public void merge(AccumulatorV2 other) {
        stringBuffer.append(other.value());
    }

    /**
     * Defines the current value of this accumulator
     * @return
     */
    @Override
    public String value() {
        return stringBuffer.toString();
    }
}