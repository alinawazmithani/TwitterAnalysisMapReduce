import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;
	int binstart = 0;
	int binend = 0;

        for (IntWritable value : values) {

		sum+=value.get();

        }
	    	result.set(sum);
		binstart = Integer.parseInt(key.toString())*5 + 1;
		binend = Integer.parseInt(key.toString())*5 + 5;
		key = new Text(Integer.toString(binstart) + "-" + Integer.toString(binend));
		context.write(key,result);
    }
}
