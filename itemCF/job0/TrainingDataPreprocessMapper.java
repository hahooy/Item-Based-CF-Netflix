package job0;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* receive (_, (MovieId, UserId, Rating))
 * emit (MovieId, (UserId,Rating) */

public class TrainingDataPreprocessMapper extends Mapper<Object, Text, Text, Text> {
	private Text K2 = new Text();
	private Text V2 = new Text();

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
		K2.set(movieId);
		V2.set(userId + "," + rating);
		context.write(K2, V2);
	}

}