import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tweets = value.toString().split(";");
	int hour = 0;
	try {
		if(tweets.length == 4)
		{
			Date date = new Date(Long.parseLong(tweets[0]));
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);  
			hour = cal.get(Calendar.HOUR_OF_DAY);
			for(int i=0; i<=23; i++)
			{  
				if(hour == i)
				{
					context.write(new Text(Integer.toString(hour)), one);				
				}
			}
		}
	}
	catch(NumberFormatException nfe) { 
  	  }
	}
}
