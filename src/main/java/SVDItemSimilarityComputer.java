import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import scala.Tuple2;

import java.io.Serializable;

/**
 * SVDItemSimilarityComputer class
 *
 * @author bingyu wu
 *         Date: 2018/6/12
 *         Time: 上午10:38
 */
public class SVDItemSimilarityComputer implements Serializable {
    /**
     * 房源特征矩阵，每一行为一个房源特征向量
     */
    private DenseMatrix itemMatrix;

    public void train(JavaRDD<Tuple2<Integer, Integer>> uiRDD, int nRows, int nCols) {

        CoordinateMatrix ratingMatrix = new RatingMatrixGenerator().generate(uiRDD, nRows, nCols);

        IndexedRowMatrix rowMatrix = ratingMatrix.toIndexedRowMatrix();
        rowMatrix = new IndexedRowMatrix(rowMatrix.rows().cache(), rowMatrix.numRows(), (int) rowMatrix.numCols());

        SingularValueDecomposition<IndexedRowMatrix, Matrix>
                svd = rowMatrix.computeSVD(2, false, 0);

        Matrix vprime = svd.V(); // V是 M * k的矩阵，M为房源个数，K为保留的奇异值个数

        itemMatrix = new DenseMatrix(vprime.numRows(), vprime.numCols(), vprime.toArray());
        //转置矩阵
        // 列向量归一化
        normr(itemMatrix);
    }

    public double compute(int idxA, int idxB) {
        double values[] = itemMatrix.values();
        int ncols = itemMatrix.numCols();
        // 计算cosine distance
        double sim = 0;
        //在v矩阵中的坐标
        for (int i = 0; i < ncols; ++i) {
            sim += (values[itemMatrix.index(idxA, i)]
                    * values[itemMatrix.index(idxB, i)]);
        }
        return sim;
    }

    /**
     * 将矩阵的每个行向量，使其模长为1。
     *
     * @param v
     */
    private void normr(Matrix v) {
        double[] values = v.toArray();
        int n = v.numRows();
        int m = v.numCols();

        double[] normr = new double[n];
        for (int i = 0; i < n; ++i) {
            double norm = 0;
            for (int j = 0; j < m; ++j) {
                double s = values[v.index(i, j)];
                norm += (s * s);
            }
            normr[i] = Math.sqrt(norm);
        }

        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < m; ++j) {
                values[v.index(i, j)] /= normr[i];
            }
        }
    }

}
