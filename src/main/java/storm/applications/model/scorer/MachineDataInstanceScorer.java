package storm.applications.model.scorer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import storm.applications.model.metadata.MachineMetadata;

/**
 * Calculate the data instance anomaly score based on its distance to the cluster center.
 * @author hongtai li
 *
 */
public class MachineDataInstanceScorer extends DataInstanceScorer<MachineMetadata> {
    @Override
    public List<ScorePackage> getScores(List<MachineMetadata> observationList) {
        List<ScorePackage> scorePackageList = new ArrayList<ScorePackage>();

        double[][] matrix = new double[observationList.size()][2];

        for (int i = 0; i < observationList.size(); ++i)	 {
            MachineMetadata metadata = observationList.get(i);
            // scorePackageList.add(new ScorePackage(metadata.getMachineIP(), 1.0, metadata));
            matrix[i][0] = metadata.getCpuIdleTime();
            matrix[i][1] = metadata.getFreeMemoryPercent();
        }

        double[] l2distances = calculateDistance(matrix);

        for (int i = 0; i < observationList.size(); ++i)	 {
            MachineMetadata metadata = observationList.get(i);
            scorePackageList.add(new ScorePackage(metadata.getMachineIP(), 1.0 + l2distances[i], metadata));
        }

        return scorePackageList;
    }

    public double[] calculateDistance(double[][] matrix) {
        double[] mins = new double[matrix[0].length];
        double[] maxs = new double[matrix[0].length];
        int colNumber = matrix[0].length;

        // find min and max for each column
        for (int col = 0; col < colNumber; ++col) {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (int row = 0; row < matrix.length; ++row) {
                if (matrix[row][col] < min) {
                    min = matrix[row][col];
                }
                if (matrix[row][col] > max) {
                    max = matrix[row][col];
                }
            }
            mins[col] = min;
            maxs[col] = max;
        }
        // cpu 
        mins[0] = 0.0;
        maxs[0] = 1.0;
        // memory
        mins[1] = 0.0;
        maxs[1] = 100.0;

        // min-max normalization by column
        double[] centers = new double[colNumber];
        Arrays.fill(centers, 0);
        for (int col = 0; col < colNumber; ++col) {
            if (mins[col] == 0 && maxs[col] == 0) {
                continue;
            }
            
            for (int row = 0; row < matrix.length; ++row) {
                matrix[row][col] = (matrix[row][col] - mins[col]) / (maxs[col] - mins[col]);
                centers[col] += matrix[row][col];
            }
            centers[col] /= matrix.length;
        }

        double[][] distances = new double[matrix.length][matrix[0].length];

        for (int row = 0; row < matrix.length; ++row) {
            for(int col = 0; col < matrix[row].length; ++col) {
                distances[row][col] = Math.abs(matrix[row][col] - centers[col]);
            }
        }

        double[] l2distances = new double[matrix.length];
        for (int row = 0; row < l2distances.length; ++row) {
            l2distances[row] = 0.0;
            for (int col = 0; col < distances[row].length; ++col) {
                l2distances[row] += Math.pow(distances[row][col], 2);
            }
            l2distances[row] = Math.sqrt(l2distances[row]);
        }

        return l2distances;
    }

    private void printMatrix(double[][] matrix) {
        for (int i = 0; i < matrix.length; ++i) {
            for (int j = 0; j < matrix[i].length; ++j) {
                System.out.print(matrix[i][j] + "\t");
            }
            System.out.println();
        }
    }
}
