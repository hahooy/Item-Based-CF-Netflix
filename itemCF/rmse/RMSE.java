package rmse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class RMSE {
	/* return [RMSE, MAE] */
	public static double[] getError(String filename) {
		int n = 0;
		double rmseSum = 0;
		double maeSum = 0;

		File input = new File(filename);
		try {
			BufferedReader br = new BufferedReader(new FileReader(input));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split("\t");
				double actual = Double.parseDouble(tokens[0]), predicted = Double
						.parseDouble(tokens[1]);
				rmseSum += Math.pow(predicted - actual, 2);
				maeSum += Math.abs(predicted - actual);
				n++;
			}
			br.close();
		} catch (Exception e) {
			System.out.println("Could not open file");
		}

		// double[] ret = [1.2, 2.1];
		double[] ret = { Math.sqrt(rmseSum / n), maeSum / n };
		return ret;
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("usage: RMSE filename");
			return;
		}
		String filename = args[0];
		double[] error = getError(filename);
		System.out.printf("RMSE: %.4f, MAE: %.4f", error[0], error[1]);
	}
}
