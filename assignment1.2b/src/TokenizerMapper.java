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
	int maxhour = 2;
	try {
		if(tweets.length == 4)
		{
			Date date = new Date(Long.parseLong(tweets[0]));
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);  
			hour = cal.get(Calendar.HOUR_OF_DAY);
			if(hour == maxhour)
			{
				String[] words = tweets[2].split(" ");
				for(String word : words) {
				if(word.startsWith("#")){
				context.write(new Text(word), one);
			}
		}
	}
	}
	}
	catch(NumberFormatException nfe) {	
	}
    }
}
