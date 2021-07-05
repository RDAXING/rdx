package accumulator;

import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;

/**
 * 自定义累加器需要继承AccumulatorV2<IN,OUT>类
 * 并且要指定要累加的类型
 */
public class MyAccumulator extends AccumulatorV2<MyKey,MyKey> implements Serializable {

    /**
     * 该累加状态是在Driver端初始化
     * 并且值也是保存在Driver端
     */
    private MyKey info = new MyKey(0, 0);

    public MyKey getInfo() {
        return info;
    }

    public void setInfo(MyKey info) {
        this.info = info;
    }

    /**
     * 判断是否是初始化状态
     * 直接与原始状态的值比较
     * 该判断为自己定义的判断方式
     * @return
     */
    @Override
    public boolean isZero() {
        return info.getPersonAgeSum()==0 && info.getPersonNum()==0;
    }

    /**
     * 为每个分区创建一个新的累加器
     * @return
     */
    @Override
    public AccumulatorV2<MyKey, MyKey> copy() {
        MyAccumulator myAccumulator = new MyAccumulator();
        myAccumulator.info = this.info;
        return myAccumulator;
    }

    /**
     * 初始化不同的partition分区中的累加类型
     */
    @Override
    public void reset() {
        info = new MyKey(0, 0);
    }

    /**
     * 进行累加时以何种规则进行累加
     * @param v 每条新进来的记录
     */
    @Override
    public void add(MyKey v) {
        info.setPersonNum(info.getPersonNum() + v.getPersonNum());
        info.setPersonAgeSum(info.getPersonAgeSum() + v.getPersonAgeSum());
    }

    /**
     * 合并不同partition分区中accumulator中储存的状态值
     * @param other 每个分区中的累加器
     */
    @Override
    public void merge(AccumulatorV2<MyKey, MyKey> other) {
        MyKey value = other.value();
        info.setPersonNum(info.getPersonNum()+value.getPersonNum());
        info.setPersonAgeSum(info.getPersonAgeSum()+value.getPersonAgeSum());
    }

    /**
     * 最后返回的累加完成的状态值
     * @return
     */
    @Override
    public MyKey value() {
        return info;
    }
}
