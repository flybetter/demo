package domain;

/**
 * Created by njuryan on 2015/12/8.
 * <p/>
 * 价格类型
 */
public enum PriceType {
    /**
     * 按元/平米计算
     */
    PricePerSQM,
    /**
     * 按万元/套计算
     */
    PricePerHouse,
    /**
     * 不能解析或未知
     */
    PriceUnknown
}
