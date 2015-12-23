package job3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* receive (UserId, list of (MovieId,Rating))
 * emit (PredictingMovieID, PredictingUserID, actualRating, predictedRating)
 * */

public class PredictionReducer extends Reducer<Text, Text, Text, Text> {
	private Text K2 = new Text();
	private Text V2 = new Text();
	private String testingDataFileName = "/Users/hahooy1/Desktop/CSE_427s_HomeWork/hw11/TestingRatings.txt";
	private String simFileName = "/Users/hahooy1/Desktop/CSE_427s_HomeWork/hw11/similarities.txt";
	/* userId -> (movieId,rating) */
	private Map<Integer, List<Entry>> testingEntries = new HashMap<Integer, List<Entry>>();
	/*
	 * movieId1 -> movieId2 -> rating
	 */
	private Map<Integer, Map<Integer, Float>> movieSimilarities = new HashMap<Integer, Map<Integer, Float>>();

	class Entry {
		int movieId;
		float rating;

		public Entry(int movieId, float rating) {
			this.movieId = movieId;
			this.rating = rating;
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/* retrieve all ratings rated by the testing user */
		List<Entry> ratings = new ArrayList<Entry>();
		for (Text value : values) {
			String[] tokens = value.toString().split(",");
			ratings.add(new Entry(Integer.parseInt(tokens[0]), Float
					.parseFloat(tokens[1])));
		}
		/*
		 * for all ratings to be predicted, make prediction by calculating the
		 * weighted average of this user's ratings. We weight the user's rating
		 * for each of these items by the similarity between that and the target
		 * item.
		 */
		for (Entry e : testingEntries.get(Integer.parseInt(key.toString()))) {
			float p = weightedAvg(e.movieId, ratings);

			/*
			 * K2.set(String.valueOf(e.rating)); V2.set(String.valueOf(p));
			 */

			K2.set(e.movieId + "," + key.toString());
			V2.set(String.format("true: %.4f, predicted %.4f", e.rating, p));

			context.write(K2, V2);
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		initTestingData();
		initSimData();
	}

	class SimRatingPair {
		float sim;
		float rating;

		SimRatingPair(float sim, float rating) {
			this.sim = sim;
			this.rating = rating;
		}
	}

	static final Comparator<SimRatingPair> SIM_PAIR_ORDER = new Comparator<SimRatingPair>() {
		public int compare(SimRatingPair s1, SimRatingPair s2) {
			return Float.compare(s1.sim, s2.sim);
		}
	};

	/* predict the rating using weighted average */
	private float weightedAvg(int itemId, List<Entry> ratings) {
		float sum = 0, n = 0;
		int N = 30;
		PriorityQueue<SimRatingPair> queue = new PriorityQueue<SimRatingPair>(
				N, SIM_PAIR_ORDER);

		for (Entry e : ratings) {

			float sim;

			if (movieSimilarities.containsKey(Math.min(itemId, e.movieId))
					&& movieSimilarities.get(Math.min(itemId, e.movieId))
							.containsKey(Math.max(itemId, e.movieId))) {
				sim = movieSimilarities.get(Math.min(itemId, e.movieId)).get(
						Math.max(itemId, e.movieId));
			} else {
				sim = 0; // handle the case where item1 and item2 have nothing
							// related
			}

			if (queue.size() < N) {
				queue.add(new SimRatingPair(sim, e.rating));
			} else if (Float.compare(queue.peek().sim, sim) < 0) {
				queue.remove();
				queue.add(new SimRatingPair(sim, e.rating));
			}

		}

		for (SimRatingPair i : queue) {
			sum += i.sim * i.rating;
			n += i.sim;
		}

		return sum / n;
	}

	/*
	 * initialize the testing data. Load all data from TestingRatings.txt into
	 * testingEntries
	 */
	private void initTestingData() throws FileNotFoundException, IOException {
		/*
		 * movieId, userId, rating
		 */
		File testingDataFile = new File(testingDataFileName);
		BufferedReader br = new BufferedReader(new FileReader(testingDataFile));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(",");
			int userId = Integer.parseInt(tokens[1]), movieId = Integer
					.parseInt(tokens[0]);
			float rating = Float.parseFloat(tokens[2]);

			if (testingEntries.containsKey(userId)) {
				testingEntries.get(userId).add(new Entry(movieId, rating));
			} else {
				List<Entry> temp = new ArrayList<Entry>();
				temp.add(new Entry(movieId, rating));
				testingEntries.put(userId, temp);
			}
		}
		br.close();
	}

	/*
	 * Load data from similarities.txt into moviesSimilarities
	 */
	private void initSimData() throws FileNotFoundException, IOException {
		/*
		 * (item1,item2 \t rating)
		 */
		File simFile = new File(simFileName);
		try {
			BufferedReader br = new BufferedReader(new FileReader(simFile));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				int item1 = Integer.parseInt(tokens[0].split(",")[0]), item2 = Integer
						.parseInt(tokens[0].split(",")[1]);
				float sim = Float.parseFloat(tokens[1]);

				/*
				 * item1 is always smaller than item2 in the nested map !! i.e.
				 * the key of outer map must be smaller than the key of the
				 * inner map. To get ratings, use movieSimilarities.get(smaller
				 * item-id).get(larger item-id)
				 */
				if (item1 > item2) {
					int temp = item1;
					item1 = item2;
					item2 = temp;
				}

				if (movieSimilarities.containsKey(item1)) {
					movieSimilarities.get(item1).put(item2, sim);
				} else {
					Map<Integer, Float> temp = new HashMap<Integer, Float>();
					temp.put(item2, sim);
					movieSimilarities.put(item1, temp);
				}

			}
			br.close();
		} catch (Exception e) {
			System.out.println("Could not open file");
			return;
		}
	}
}
