package amazonEmotion;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import amazonEmotion.AmazonEmotion.DoubleArrayWritable;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class ReviewMapper extends AmazonMapper<DoubleWritable, DoubleArrayWritable> {

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
	public static HashSet<String> noSentimentWords;		//11
	
	protected StanfordCoreNLP pipeline = null;
	
	public ReviewMapper()
	{
		super();
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		props.put("tokenizerOptions", "americanize=false");
		this.pipeline = new StanfordCoreNLP(props);
		loadEmotions(args[2]);
	}
	

	@Override
	protected void map(AmazonRecord record, Context context) throws IOException, InterruptedException {
		String review = record.getReview();
		
		// Tokenize, sentence split, POS tag and lemmatize
		LinkedList<String> words = new LinkedList<String>();
		Annotation document = new Annotation(review);
		this.pipeline.annotate(document);
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		// For each sentence
		for (CoreMap sentence : sentences) {
			// For each token
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				// Add only the lemma
				words.add(token.get(LemmaAnnotation.class));
			}
		}
		
		int[] counts = getEmoCounts(words);
		//String theScores = getScores(counts);
		DoubleWritable[] theScores = getScores(counts);

		DoubleArrayWritable as = new DoubleArrayWritable(theScores);
		
		context.write(new DoubleWritable(record.getScore()), as);
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
	private int[] getEmoCounts(List<String> words) {
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
	
	private  void loadEmotions(String directory) {
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
