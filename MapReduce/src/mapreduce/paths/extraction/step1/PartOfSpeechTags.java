package mapreduce.paths.extraction.step1;

import java.util.HashSet;
import java.util.Set;

public class PartOfSpeechTags {
	private static String[]  verbTags={
			"VB","VBD","VBG","VBN","VBP","VBZ"
	};
	private static String[]  nounTags={
			"NN","NNS","NNP","NNPS","PRP","PRP$"
	};
	private static String[]  adjectiveTags={
			"JJ","JJR","JJS"
	};
	private static String[]  adverbTags={
			"RB","RBR","RBS"
	};
	private static String[]  validTags={
			"VB","VBD","VBG","VBN","VBP","VBZ",
			"NN","NNS","NNP","NNPS",
			"JJ","JJR","JJS",
			"RB","RBR","RBS",
			"PRP","PRP$",
			"IN","TO",
	};
	public static Set<String> getAdverbTags(){
		Set<String> adverbTagsSet=new HashSet<String>();
		for(String word:adverbTags){
			adverbTagsSet.add(word);
		}
		return adverbTagsSet;
	}
	public static Set<String> getVerbTags(){
		Set<String> verbTagsSet=new HashSet<String>();
		for(String word:verbTags){
			verbTagsSet.add(word);
		}
		return verbTagsSet;
	}
	public static Set<String> getAdjectiveTags(){
		Set<String> adjectiveTagsSet=new HashSet<String>();
		for(String word:adjectiveTags){
			adjectiveTagsSet.add(word);
		}
		return adjectiveTagsSet;
	}
	public static Set<String> getNounTags(){
		Set<String> nounTagsSet=new HashSet<String>();
		for(String word:nounTags){
			nounTagsSet.add(word);
		}
		return nounTagsSet;
	}
	public static Set<String> getValidTags(){
		Set<String> validTagsSet=new HashSet<String>();
		for(String word:validTags){
			validTagsSet.add(word);
		}
		return validTagsSet;
	}
}
