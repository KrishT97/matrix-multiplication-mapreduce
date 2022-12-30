import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MatrixMultiplyReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Sum up the values for each cell in the output matrix
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        // Emit the final value for the cell in the output matrix
        context.write(key, new Text(String.valueOf(sum)));
    }


}
