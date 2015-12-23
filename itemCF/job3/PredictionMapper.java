package job3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* receive (_, (MovieId, UserId, Rating))
 * emit (UserId, (MovieId,Rating) */

public class PredictionMapper extends Mapper<Object, Text, Text, Text> {
	private Text K2 = new Text();
	private Text V2 = new Text();
	private String testingDataFileName = "/Users/hahooy1/Desktop/CSE_427s_HomeWork/hw11/TestingRatings.txt";
	private Set<Integer> testingUserId = new HashSet<Integer>();


	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String valueAsString = value.toString().trim();
		String[] tokens = valueAsString.split(",");
		if (tokens.length != 3) {
			return;
		}

		String movieId = tokens[0];
		String userId = tokens[1];
		String rating = tokens[2];
		
		/* filter out users we don't test */
		if (testingUserId.contains(Integer.parseInt(userId))) {
			K2.set(userId);
			V2.set(movieId + "," + rating);
			context.write(K2, V2);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		File movieTitle = new File(testingDataFileName);
		BufferedReader br = new BufferedReader(new FileReader(movieTitle));
		String line = null;
		while ((line = br.readLine()) != null) {
			testingUserId.add(Integer.parseInt(line.split(",")[1]));
		}
		br.close();
	}
}