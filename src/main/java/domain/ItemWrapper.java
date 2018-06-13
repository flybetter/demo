package domain;

import com.google.common.base.Objects;
import com.google.common.primitives.Doubles;

import java.io.Serializable;

/**
 * ItemWrapper class
 *
 * @author bingyu wu
 *         Date: 2018/6/12
 *         Time: 下午2:34
 */
public class ItemWrapper implements Comparable<ItemWrapper>, Serializable {

    private Item item;

    private double value;


    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public ItemWrapper(Item item, double value) {
        this.item = item;
        this.value = value;
    }

    @Override
    public int compareTo(ItemWrapper o) {
        return Doubles.compare(value, o.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ItemWrapper that = (ItemWrapper) o;
        return Objects.equal(item, that.item);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = item != null ? item.hashCode() : 0;
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ItemWrapper{" +
                "item=" + item +
                ", value=" + value +
                '}';
    }
}
