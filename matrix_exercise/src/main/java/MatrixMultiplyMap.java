import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MatrixMultiplyMap extends Mapper<LongWritable, Text, Text, Text> {

    private int[][] submatrix_a;
    private int[][] submatrix_b;
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        int[][] submatrix = parseInputValue(value);
        String matrix = parseInputMatrix(value);
        if (matrix.equals("A")) {
            submatrix_a = submatrix;
        } else {
            submatrix_b = submatrix;
        }
        
        int[][] submatrix_c = multiply(submatrix_a, submatrix_b);
        for (int i = 0; i < submatrix_c.length; i++) {
            for (int j = 0; j < submatrix_c[0].length; j++) {
                context.write(new Text(i + "," + j), new Text(String.valueOf(submatrix_c[i][j])));
            }
        }
    }
    private String parseInputMatrix(Text value) {
        String[] parts = value.toString().split(",");
        return parts[0];
    }

    private int[][] parseInputValue(Text value) {
        String[] parts = value.toString().split(",");
        String matrix = parts[0];
        int rowStart = Integer.parseInt(parts[1]);
        int colStart = Integer.parseInt(parts[2]);
        int[][] submatrix = new int[10][10];
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                submatrix[i][j] = Integer.parseInt(parts[3 + i * 10 + j]);
            }
        }
        return submatrix;
    }

    private int[][] multiply(int[][] submatrix_a, int[][] submatrix_b) {
        int[][] submatrix_c = new int[10][10];
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                int sum = 0;
                for (int k = 0; k < 10; k++) {
                    sum += submatrix_a[i][k] * submatrix_b[k][j];
                }
                submatrix_c[i][j] = sum;
            }
        }
        return submatrix_c;
    }
}
