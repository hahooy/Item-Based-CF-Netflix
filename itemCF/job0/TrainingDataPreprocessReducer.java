package job0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* receive (MovieId, list of (UserId,Rating))
 * emit (UserId, MovieId,Rating;numRatings;sumRatings)
 * */

public class TrainingDataPreprocessReducer extends
		Reducer<Text, Text, Text, Text> {
	private Text K2 = new Text();
	private Text V2 = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int numRatings = 0;
		long sumRatings = 0;
		List<String> users = new ArrayList<String>();

		for (Text i : values) {
			/* tokens = [UserId, Rating] */
			String[] tokens = i.toString().split(",");
			numRatings++;
			sumRatings += (int) Double.parseDouble(tokens[1]);
			users.add(i.toString());
		}

		for (String i : users) {
			/* tokens = [UserId, Rating] */
			String[] tokens = i.split(",");
			K2.set(tokens[0]); // UserId
			V2.set(key.toString() + "," + tokens[1] + ";" + numRatings + ";"
					+ sumRatings); // MovieId,Rating;numRatings;sumRatings
			context.write(K2, V2);
		}
	}
}
