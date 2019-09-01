import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterReplJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Hashtable<String, String> athleteInfo;

	public IntWritable one = new IntWritable(1);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] tweets = value.toString().split(";");
		if(tweets.length == 4)
		{			
			String tweet = tweets[2].toLowerCase();
			for(String athleteNames : athleteInfo.keySet())
			{
				if(tweet.contains(athleteNames)){
				context.write(new Text(athleteNames), one); }
			}
		}
}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athleteInfo = new Hashtable<String, String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {

				String[] fields = line.split(",");
				// Fields are: 1:name 7:sport
				if (fields.length == 11)
					athleteInfo.put(fields[1].toLowerCase(), fields[7].toLowerCase());
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
