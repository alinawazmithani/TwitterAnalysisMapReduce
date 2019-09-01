import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tweets = value.toString().split(";");
	int cat = 0;
	if(tweets.length == 4 && tweets[2].length() <= 140)
	{
		for(int i=1; i<=140; i++)
		{  
			if(tweets[2].length() == i)
			{
				cat = (i-1) / 5;
				context.write(new Text(Integer.toString(cat)), one);
			}
		}
	}
    }
}
