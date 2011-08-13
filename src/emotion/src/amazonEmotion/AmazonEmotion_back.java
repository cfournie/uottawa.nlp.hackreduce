package amazonEmotion;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * This MapReduce job will get the title and score of each article.
 *
 */
public class AmazonEmotion_back extends Configured implements Tool {
	
	public static String[] emotions = new String[]{"anger", "anticipation", "disgust", "fear", "joy", 
		"sadness", "surprise", "trust", "noEmotion", "positive", "negative", "noSentiment"};
	
	public static HashSet<String> angerWords;			// 0
	public static HashSet<String> anticipationWords;	// 1
	public static HashSet<String> disgustWords;			// 2
	public static HashSet<String> fearWords;			// 3
	public static HashSet<String> joyWords;				// 4
	public static HashSet<String> sadnessWords;			// 5
	public static HashSet<String> surpriseWords;		// 6
	public static HashSet<String> trustWords;			// 7
	public static HashSet<String> noEmotionWords;		// 8

	public static HashSet<String> positiveWords;		// 9
	public static HashSet<String> negativeWords;		// 10
	public static HashSet<String> noSentimentWords;		// 11
														// 12 is the total identifiable word count
	
	public static class DoubleArrayWritable extends ArrayWritable
	{
	    public DoubleArrayWritable() {
	        super(DoubleWritable.class);
	    }
	    public DoubleArrayWritable(DoubleWritable[] values) {
	        super(DoubleWritable.class, values);
	    }
	}

	public static class ReviewMapper extends AmazonMapper<DoubleWritable, DoubleArrayWritable> {
		
		// Our own made up key to send all counts to a single Reducer, so we can
		// aggregate a total value.
		//public static final Text TOTAL_COUNT = new Text("total");

		// Just to save on object instantiation
		//public static  DoubleWritable ONE_COUNT = new DoubleWritable(1);

		@Override
		protected void map(AmazonRecord record, Context context) throws IOException, InterruptedException {
			String review = record.getReview();
			String[] words = review.split("\\b");
			int[] counts = getEmoCounts(words);
			//String theScores = getScores(counts);
			DoubleWritable[] theScores = getScores(counts);

			DoubleArrayWritable as = new DoubleArrayWritable(theScores);
			
			context.write(new DoubleWritable(record.getScore()), as);
			
			
			//ONE_COUNT = new DoubleWritable(counts[0]);
			//context.getCounter(Count.SCORE).increment(1);
			//context.write(TOTAL_COUNT, ONE_COUNT);
			
		}

		/**
		 * Creates a String to return the results in
		 * 
		 * @param counts
		 * @return
		 */
		private String getScoresString(int[] counts) {
			if(counts[counts.length-1] == 0){
				counts[counts.length-1] = 1;
			}
			String result = ""+(double)counts[0]/(double)counts[counts.length-1];
			for(int i = 1; i < counts.length-1; i++){
				result += "\t" + (double)counts[i]/(double)counts[counts.length-1];
			}
			return result;
		}
		
		private DoubleWritable[] getScores(int[] counts) {
			DoubleWritable[] dwArray = new DoubleWritable[counts.length-1];
			if(counts[counts.length-1] == 0){
				counts[counts.length-1] = 1;
			}
			for(int i = 0; i < counts.length-1; i++){
				dwArray[i] = new DoubleWritable((double)counts[i]/(double)counts[counts.length-1]);
			}
			return dwArray;
		}

		/**
		 * Counts Emotional Words
		 * 
		 * @param words
		 * @return
		 */
		private int[] getEmoCounts(String[] words) {
			int[] counts = new int[13];
			int totalWords = 0;
			//int totalWords2 = 0;
			for(String word : words){
				boolean foundEmo = false;
				//boolean foundSenti = false;
				if(angerWords.contains(word)){
					counts[0]++;
					foundEmo = true;
				}
				if(anticipationWords.contains(word)){
					counts[1]++;
					foundEmo = true;
				}
				if(disgustWords.contains(word)){
					counts[2]++;
					foundEmo = true;
				}
				if(fearWords.contains(word)){
					counts[3]++;
					foundEmo = true;
				}
				if(joyWords.contains(word)){
					counts[4]++;
					foundEmo = true;
				}
				if(sadnessWords.contains(word)){
					counts[5]++;
					foundEmo = true;
				}
				if(surpriseWords.contains(word)){
					counts[6]++;
					foundEmo = true;
				}
				if(trustWords.contains(word)){
					counts[7]++;
					foundEmo = true;
				}
				if(noEmotionWords.contains(word)){
					counts[8]++;
					foundEmo = true;
				}
				if(positiveWords.contains(word)){
					counts[9]++;
				}
				if(negativeWords.contains(word)){
					counts[10]++;
				}
				if(noSentimentWords.contains(word)){
					counts[11]++;
				}
				if(foundEmo){
					totalWords++;
				}
			}
			counts[12] = totalWords;
			return counts;
		}

	}
	
	public static class AmazonReducer extends Reducer<DoubleWritable, DoubleArrayWritable, Text, Text> {

		@Override
		protected void reduce(DoubleWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
			//context.getCounter(Count.SCORE).increment(1);

			DecimalFormat myFormatter = new DecimalFormat("#.###");
			
			int count = 0;
			double[] array = new double[12];
			for (DoubleArrayWritable value : values) {
				Writable[] writeArray = value.get();
				
				count++;
				String lineToPrint = "";
				for(int i = 0; i < writeArray.length; i++){
					DoubleWritable d = (DoubleWritable) writeArray[i];
					array[i] += d.get();
					lineToPrint += myFormatter.format(d.get()) + "\t";
				}
				//context.write(new Text(""+key), new Text(lineToPrint));
			}
				

			String output = "";
			
			myFormatter = new DecimalFormat("#.###");
			
			for(int i = 0; i < array.length; i++){
				//System.out.println(array1[i] + " " + count1);
				array[i] = array[i]/(double)count;
				output += emotions[i] + ":" + myFormatter.format(array[i]) + "\t";
			}
			
			context.write(new Text(""+key), new Text(output));
			
			//highestScore += value.get();
			//context.write(key, new Text("score: "+highestScore));
			//context.write(SCORE);
		}

	}
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
        	System.err.println("Usage: programName <input> <output> <emotionsFile>");
        	System.exit(2);
        }
        
        loadEmotions(args[2]);
		
		int result = ToolRunner.run(new Configuration(), new AmazonEmotion_back(), args);
		System.exit(result);
	}


	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        /*if (args.length != 3) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output> <emotionsFile>");
        	System.exit(2);
        }
        
        loadEmotions(args[2]);*/

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(ReviewMapper.class);
		job.setReducerClass(AmazonReducer.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    AmazonMapper.configureJob(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static void loadEmotions(String directory) {
		angerWords = new HashSet<String>();
		loadWords(angerWords, directory + "/anger.txt");
		anticipationWords = new HashSet<String>();
		loadWords(anticipationWords, directory + "/anticipation.txt");
		disgustWords = new HashSet<String>();
		loadWords(disgustWords, directory + "/disgust.txt");
		fearWords = new HashSet<String>();
		loadWords(fearWords, directory + "/fear.txt");
		joyWords = new HashSet<String>();
		loadWords(joyWords, directory + "/joy.txt");
		sadnessWords = new HashSet<String>();
		loadWords(sadnessWords, directory + "/sadness.txt");
		surpriseWords = new HashSet<String>();
		loadWords(surpriseWords, directory + "/surprise.txt");
		trustWords = new HashSet<String>();
		loadWords(trustWords, directory + "/trust.txt");
		noEmotionWords = new HashSet<String>();
		loadWords(noEmotionWords, directory + "/noEmotion.txt");

		positiveWords = new HashSet<String>();
		loadWords(positiveWords, directory + "/positive.txt");
		negativeWords = new HashSet<String>();
		loadWords(negativeWords, directory + "/negative.txt");
		noSentimentWords = new HashSet<String>();
		loadWords(noSentimentWords, directory + "/noSentiment.txt");
	}

	private static void loadWords(HashSet<String> wordSet, String fname) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fname));
	         
			for ( ; ; ) {
				String line = br.readLine();
	
				if (line == null) {
					br.close();
					break;
				}
	
				else {
					wordSet.add(line);
				}
			}
	
		} catch (Exception e) {
	    	 e.printStackTrace();
		}
	}

}
