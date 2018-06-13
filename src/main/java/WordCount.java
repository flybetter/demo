import com.google.common.collect.*;
import domain.Item;
import domain.ItemWrapper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * WordCount class
 *
 * @author bingyu wu
 *         Date: 2018/6/11
 *         Time: 上午10:53
 */

public class WordCount {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    private static String redis_host = "127.0.0.1";

    private static String redis_key = "recom_cid_orihid_to_hids";

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        SparkContext sparkContext = spark.sparkContext();

        JavaSparkContext sc = new JavaSparkContext(sparkContext);

        Dataset<Row> df_log = spark.read().json("hdfs://192.168.105.21:9000/database/source/accesslog/temp.txt");


        //TODO 新房的判断
        df_log = df_log.selectExpr("cast (userId as string) as USER_ID",
                "concat(cityId,'_',projectId) as ITEM_ID");

        //TODO 30
        df_log.coalesce(30);

        JavaPairRDD<String, String> pairRDD = df_log.javaRDD().mapToPair(new Functors.ToPairRDDFunctor());

        pairRDD = pairRDD.cache();

        BiMap<String, Integer> userIndexBiMap = HashBiMap.create();
        BiMap<String, Integer> itemIndexBiMap = HashBiMap.create();

        // 收集用户和房源ID
        List<String> userids = pairRDD.keys().distinct().collect();
        List<String> itemids = pairRDD.values().distinct().collect();

        // 生成USERID与行索引的双向映射
        int rowindex = 0;
        for (String id : userids) {
            userIndexBiMap.put(id, rowindex++);
        }

        // 生成ITEMID与列索引的双向映射
        int colindex = 0;
        for (String id : itemids) {
            itemIndexBiMap.put(id, colindex++);
        }

        // 广播所需变量
        Broadcast<BiMap<String, Integer>> userIndexMapBroadCast = sc.broadcast(userIndexBiMap);
        Broadcast<BiMap<String, Integer>> itemIndexMapBroadCast = sc.broadcast(itemIndexBiMap);
        JavaRDD<Tuple2<Integer, Integer>> uiRDD = pairRDD.map(
                new Functors.String2IndexFunctor(userIndexMapBroadCast, itemIndexMapBroadCast)
        );

        pairRDD.unpersist();

//        logger.info("*********************************" + uiRDD.count());


        Tuple3<BiMap<String, Integer>,
                BiMap<String, Integer>,
                JavaRDD<Tuple2<Integer, Integer>>> result = new Tuple3<>(userIndexBiMap, itemIndexBiMap, uiRDD);


        BiMap<String, Integer> userRSIDIndexBiMap = result._1();
        BiMap<String, Integer> itemRSIDIndexBiMap = result._2();

        uiRDD.cache();

        // 训练SVD相似性计算器
        SVDItemSimilarityComputer computer = new SVDItemSimilarityComputer();
        computer.train(uiRDD, userIndexBiMap.size(), itemIndexBiMap.size());

        //和item联系的
        //TODO 不做和数据库中的关联了
        // colindex, Item

        final BiMap<Integer, Item> indexItemBiMap = HashBiMap.create();
        for (String key : itemRSIDIndexBiMap.keySet()) {
            Item item = new Item(Integer.parseInt(key.split("_")[0]), Functors.getOriginHouseId(key.split("_")[1]));
            indexItemBiMap.put(itemRSIDIndexBiMap.get(key), item);
        }


        JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> ratingsGroupByItem = uiRDD
                .groupBy(new Functors.GroupByItem());

        JavaPairRDD<Integer, Integer> numVisiterPerItem = ratingsGroupByItem
                .mapToPair(new Functors.CountVisiter());

        JavaRDD<Tuple3<Integer, Integer, Integer>> ratingsWithNumVisiter = ratingsGroupByItem
                .join(numVisiterPerItem)
                .flatMap(new Functors.FlatMapUserItemCount());

        JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> ratingsWithNumVisiterKeyByUser = ratingsWithNumVisiter
                .keyBy(new Functors.KeyByUser());


        JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> itemPairsWithNumVisiter = ratingsWithNumVisiterKeyByUser
                .join(ratingsWithNumVisiterKeyByUser)
                .filter(new Functors.FilteDuplicatedPairs())
                .mapToPair(new Functors.MapToItemPairWithCount())
                .groupByKey()
                .mapToPair(new Functors.MapToItemPairWithCommonCount());

        Set<Integer> jaccardDomain = Sets.newHashSet();
        itemPairsWithNumVisiter = itemPairsWithNumVisiter.cache();
        for (Tuple2<Integer, Integer> t : itemPairsWithNumVisiter.keys().collect()) {
            jaccardDomain.add(t._1);
            jaccardDomain.add(t._2);
        }

        JavaRDD<MatrixEntry> jaccardSimilarityEntries = itemPairsWithNumVisiter
                .flatMap(new Functors.CalcJaccardSimilarity());


        logger.info("jaccardSimilary*********" + jaccardSimilarityEntries.count());

        // 若Item不在jaccard domain， 则没有其对应的MatrixEntry，
        // 导致下面生成IndexedRowMatrix时没有对应的行。
        // 在此，对不在jaccarddomain的中Item，添加MatrixEntry<i,0,0>
        // 以使，rowMatrix中产生对应的行
//        Set<Integer> notInDomain = Sets.newHashSet(indexItemBiMap.keySet());
//        notInDomain.removeAll(jaccardDomain);
//        List<MatrixEntry> additionalEntries = Lists.newArrayList();
//        for (Integer i : notInDomain) {
//            additionalEntries.add(new MatrixEntry(i, 0, 0));
//        }


//        jaccardSimilarityEntries = jaccardSimilarityEntries.union(sc.parallelize(additionalEntries));

        // 构造为相似性矩阵
        CoordinateMatrix similarityMatrix = new CoordinateMatrix(jaccardSimilarityEntries.rdd(),
                itemIndexBiMap.size(),
                itemIndexBiMap.size());

        IndexedRowMatrix rowMatrix = similarityMatrix.toIndexedRowMatrix();
        // 执行推荐

        //我已经得到一个矩阵
        int needRecommendNum = 10;
//        if (jobConf.getSourceType() == SourceType.WEB && jobConf.getRecommendType() != RecommendType.INCREMENTAL) {
//            needRecommendNum = jobConf.getIMRecommendNum();
//        }
//
//        JavaSparkContext sc = JavaSparkContext.fromSparkContext(ratings.context());

        // 返回PRJID->[(PRJ_ID,value)]形式的PairRDD

        JavaPairRDD<Integer, List<Tuple2<Integer, Double>>> recommends = rowMatrix
                .rows()
                .toJavaRDD()
                .mapToPair(new Functors.Recommender(
                        sc.broadcast(indexItemBiMap),
                        sc.broadcast(computer),
                        needRecommendNum
                ));


        logger.info("recommends:" + recommends.count());

        Map<Integer, List<Tuple2<Integer, Double>>> rawResult = recommends.collectAsMap();

        Map<Item, List<ItemWrapper>> recommend_result = Maps.newHashMap();
        for (Integer id : rawResult.keySet()) {
            Item i = indexItemBiMap.get(id);
            List<ItemWrapper> wps = Lists.transform(rawResult.get(id), new com.google.common.base.Function<Tuple2<Integer, Double>, ItemWrapper>() {
                @Nullable
                @Override
                public ItemWrapper apply(@Nullable Tuple2<Integer, Double> tuple) {
                    return new ItemWrapper(indexItemBiMap.get(tuple._1), tuple._2);
                }
            });
            recommend_result.put(i, Lists.newArrayList(wps));
        }

        logger.info("recommend_result:" + recommend_result.size());


        Jedis jedis = new Jedis(redis_host);

        Pipeline p = jedis.pipelined();

        for (Item item : recommend_result.keySet()) {
            ObjectMapper mapper = new ObjectMapper();
            logger.info("recom_cid_orihid_to_hids^" + item.getCityid() + "^" + item.getProjectId());
            try {
                p.set("recom_cid_orihid_to_hids^" + item.getCityid() + "^" + item.getProjectId(), mapper.writeValueAsString(recommend_result.get(item)));
            } catch (IOException e) {
                logger.error(e.toString());
            }
        }
        p.syncAndReturnAll();

    }


    static class Functors {


        private static String getOriginHouseId(final String houseId) {

            String lpadZeroHouseId = houseId.substring(7);
            return String.valueOf(Long.valueOf(lpadZeroHouseId));
        }

        private static class ToPairRDDFunctor implements PairFunction<Row, String, String> {

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String uid = row.getAs("USER_ID");
                String iid = row.getAs("ITEM_ID");
                return new Tuple2<>(uid, iid);
            }
        }

        private static class String2IndexFunctor implements Function<Tuple2<String, String>, Tuple2<Integer, Integer>> {
            private final Broadcast<BiMap<String, Integer>> userIndexMapBroadCast;
            private final Broadcast<BiMap<String, Integer>> itemIndexMapBroadCast;

            public String2IndexFunctor(Broadcast<BiMap<String, Integer>> rowIndexMapBroadCast, Broadcast<BiMap<String, Integer>> columnIndexMapBroadCast) {
                this.userIndexMapBroadCast = rowIndexMapBroadCast;
                this.itemIndexMapBroadCast = columnIndexMapBroadCast;
            }

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<String, String> tuple) throws Exception {
                return new Tuple2<>(
                        userIndexMapBroadCast.value().get(tuple._1),
                        itemIndexMapBroadCast.value().get(tuple._2)
                );
            }
        }


        public static class GroupByItem implements Function<Tuple2<Integer, Integer>, Integer> {

            @Override
            public Integer call(Tuple2<Integer, Integer> userItemPair) throws Exception {
                return userItemPair._2;
            }
        }

        public static class CountVisiter implements PairFunction<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>, Integer, Integer> {

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> tuple) throws Exception {
                return new Tuple2<>(tuple._1, Iterables.size(tuple._2));
            }
        }

        public static class FlatMapUserItemCount implements FlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Integer>>, Integer>>, Tuple3<Integer, Integer, Integer>> {

            @Override
            public Iterator<Tuple3<Integer, Integer, Integer>> call(final Tuple2<Integer, Tuple2<Iterable<Tuple2<Integer, Integer>>, Integer>> joinedTuple) throws Exception {
                return Iterators.transform(joinedTuple._2._1.iterator(), new com.google.common.base.Function<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Nullable
                    @Override
                    public Tuple3<Integer, Integer, Integer> apply(@Nullable Tuple2<Integer, Integer> userItemPair) {
                        return new Tuple3<>(userItemPair._1, userItemPair._2, joinedTuple._2._2);
                    }
                });
            }
        }

        public static class KeyByUser implements Function<Tuple3<Integer, Integer, Integer>, Integer> {

            @Override
            public Integer call(Tuple3<Integer, Integer, Integer> userItemNumTuple) throws Exception {
                return userItemNumTuple._1();
            }
        }

        public static class FilteDuplicatedPairs implements Function<Tuple2<Integer, Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>>, Boolean> {

            @Override
            public Boolean call(Tuple2<Integer, Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>> tuple) throws Exception {
                // 过滤重复的Item—Item pair。 ITEM_A的index 小于 ITEM_B的index
                return tuple._2._1._2() < tuple._2._2._2();
            }
        }

        public static class MapToItemPairWithCount implements PairFunction<Tuple2<Integer, Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

            @Override
            public Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> call(Tuple2<Integer, Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>> tuple) throws Exception {
                return new Tuple2<>(
                        new Tuple2<>(tuple._2._1._2(), tuple._2._2._2()),
                        new Tuple2<>(tuple._2._1._3(), tuple._2._2._3())
                );
            }
        }

        public static class MapToItemPairWithCommonCount implements PairFunction<Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Integer>>>, Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

            @Override
            public Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> call(Tuple2<Tuple2<Integer, Integer>, Iterable<Tuple2<Integer, Integer>>> tuple) throws Exception {
                return new Tuple2<>(
                        tuple._1,
                        new Tuple3<>(
                                Iterables.get(tuple._2, 0)._1,
                                Iterables.get(tuple._2, 0)._2,
                                Iterables.size(tuple._2)
                        )
                );
            }
        }

        public static class CalcJaccardSimilarity implements FlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>, MatrixEntry> {

            @Override
            public Iterator<MatrixEntry> call(Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>> tuple) throws Exception {
                int union = tuple._2._1() + tuple._2._2() - tuple._2._3();
                double similarity = tuple._2._3() * 1.0 / union;
                return Arrays.asList(
                        new MatrixEntry(tuple._1._1, tuple._1._2, similarity),
                        new MatrixEntry(tuple._1._2, tuple._1._1, similarity)
                ).iterator();
            }
        }

        public static class Recommender implements PairFunction<IndexedRow, Integer, List<Tuple2<Integer, Double>>> {

            private final BiMap<Integer, Item> vIdxItemMap;
            private final SVDItemSimilarityComputer vComputer;
            private final int recommendNum;
            private final BiMap<Item, Integer> vItemIdxMap;

            public Recommender(Broadcast<BiMap<Integer, Item>> indexItemBiMapBroadcast,
                               Broadcast<SVDItemSimilarityComputer> svdComputer,
                               int recommendNum) {
                this.vIdxItemMap = Maps.unmodifiableBiMap(indexItemBiMapBroadcast.getValue());
                this.vItemIdxMap = vIdxItemMap.inverse();
                this.vComputer = svdComputer.getValue();
                this.recommendNum = recommendNum;
            }

            @Override
            public Tuple2<Integer, List<Tuple2<Integer, Double>>> call(IndexedRow indexedRow) throws Exception {
                final int recommendFor = (int) indexedRow.index();

                if (!vIdxItemMap.containsKey(recommendFor)) {
                    return null;
                }

                final Item recommendForItem = vIdxItemMap.get(recommendFor);
                final Vector vector = indexedRow.vector();


                Set<Item> candidates = Sets.newHashSet();

                for (Item item : vIdxItemMap.values()) {

                    if (item.equals(recommendForItem)) {
                        continue;
                    }
                    //可以优化
                    if (item.getCityid() == recommendForItem.getCityid()) {
                        candidates.add(item);
                    }

                }


                Set<ItemWrapper> wrappers = Sets.newHashSet();
                for (Item item : candidates) {
                    int idx = vItemIdxMap.get(item);
                    double val = vector.apply(idx);
                    if (val != 0) {
                        val += 1; // Jaccard得到的推荐结果+1，排序时优先
                    } else {
                        val = vComputer.compute(vItemIdxMap.get(recommendForItem), vItemIdxMap.get(item));
                    }
                    wrappers.add(new ItemWrapper(item, val));
                }
                List<ItemWrapper> wps = Ordering.natural().greatestOf(wrappers, recommendNum);

                // 对于得到的推荐结果，将其评分换算为SVD的得分
                for (ItemWrapper w : wps) {
                    w.setValue(vComputer.compute(vItemIdxMap.get(recommendForItem), vItemIdxMap.get(w.getItem())));
                }

                List<Tuple2<Integer, Double>> recommendList = Lists.newArrayList(
                        Lists.transform(wps, new com.google.common.base.Function<ItemWrapper, Tuple2<Integer, Double>>() {
                            @Nullable
                            @Override
                            public Tuple2<Integer, Double> apply(@Nullable ItemWrapper itemWrapper) {
                                return new Tuple2<>(vItemIdxMap.get(itemWrapper.getItem()), itemWrapper.getValue());
                            }
                        })
                );

                return new Tuple2<>(recommendFor, recommendList);
            }
        }


        public static class IsNotNull implements Function<Tuple2<Integer, List<Tuple2<Integer, Double>>>, Boolean> {
            @Override
            public Boolean call(Tuple2<Integer, List<Tuple2<Integer, Double>>> v1) throws Exception {
                return v1 != null;
            }
        }

        public static class GetIndex implements Function<IndexedRow, Integer> {
            @Override
            public Integer call(IndexedRow v1) throws Exception {
                return (int) v1.index();
            }
        }


    }

}
