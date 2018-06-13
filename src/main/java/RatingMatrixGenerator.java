import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import scala.Tuple2;

/**
 * RatingMatrixGenerator class
 *
 * @author bingyu wu
 *         Date: 2018/6/12
 *         Time: 上午10:51
 */
public class RatingMatrixGenerator {
    public CoordinateMatrix generate(JavaRDD<Tuple2<Integer, Integer>> uiRDD, int nRows, int nCols) {

        JavaRDD<MatrixEntry> entries = uiRDD.map(
                new Functors.UIPairToMatrixEntity());

        return new CoordinateMatrix(entries.rdd(), nRows, nCols);
    }

    private static class Functors {
        private static class UIPairToMatrixEntity implements Function<Tuple2<Integer, Integer>, MatrixEntry> {

            @Override
            public MatrixEntry call(Tuple2<Integer, Integer> tuple) throws Exception {
                return new MatrixEntry(tuple._1, tuple._2, 1);
            }
        }
    }
}
