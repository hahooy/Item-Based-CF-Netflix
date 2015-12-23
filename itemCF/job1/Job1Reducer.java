package job1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Input (UserId,list of (MovieId,Rating;numRatings;sumRatings))
 * cross product the list with itself
 * emit (MivieId_1, MovieId_2, Rating_1;numRatings_1;sumRatings_1, Rating_2;numRatings_2;sumRatings_2)
 * the information about UserId is lost in this job because it is not needed in the next job
 */

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {
	private Text K2 = new Text();
	private Text V2 = new Text();

	static final Comparator<String> MOVIEID_ORDER = new Comparator<String>() {
		public int compare(String s1, String s2) {
			return Integer.parseInt(s1.split(",")[0])
					- Integer.parseInt(s2.split(",")[0]);
		}
	};

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> moviesList = new ArrayList<String>();

		for (Text value : values) {
			moviesList.add(value.toString().trim());
		}

		/* sorting is necessary here to avoid duplicate */
		Collections.sort(moviesList, MOVIEID_ORDER);

		/* cross product */
		for (int i = 0; i < moviesList.size(); i++) {
			for (int j = i + 1; j < moviesList.size(); j++) {
				/* movie1Tokens = [MovieId1, Ratings1] */
				String[] movie1Tokens = moviesList.get(i).split(",");
				String[] movie2Tokens = moviesList.get(j).split(",");
				K2.set(movie1Tokens[0] + "," + movie2Tokens[0]);
				V2.set(movie1Tokens[1] + "," + movie2Tokens[1]);
				context.write(K2, V2);
			}
		}
	}
}