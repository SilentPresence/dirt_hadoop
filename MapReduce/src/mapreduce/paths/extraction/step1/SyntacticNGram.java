package mapreduce.paths.extraction.step1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.stanford.nlp.process.Morphology;


public class SyntacticNGram {
	private final  Set<String> nounTagsSet=PartOfSpeechTags.getNounTags();
	private final  Set<String> verbTagsSet=PartOfSpeechTags.getVerbTags();
	private final  Set<String> adverbTagsSet=PartOfSpeechTags.getAdverbTags();
	private final  Set<String> adjectiveTagsSet=PartOfSpeechTags.getAdjectiveTags();
	private final  Set<String> validTagSet=PartOfSpeechTags.getValidTags();
	private final  Set<String> auxiliaryVerbSet=AuxiliaryVerbs.getAuxiliaryVerbs();
	private final  Set<String> stopWords=StopWords.getStopWords();

	private  String[] line=null;
	private  String[] tokens=null;
	private  String[][] syntacticNGramTokens=null;
	private  List<String[]> nounList=null;

	private final Morphology morph=new Morphology();

	private long occurences=0;

	public SyntacticNGram(){

	}

	public List<List<String[]>> processLine(String line) throws IOException{
		this.line=line.split("\t");
		ArrayList<List<String[]>> list = null;
		if(this.line.length>=4){
			tokens=this.line[1].split(" ");
			if(tokens.length>=3){
				parseTokens();
				if(syntacticNGramTokens!=null){
					if(isHeadVerb()){
						getNounList();
						//If the N-Gram contains less than 2 nouns, there is no point in building the paths
						if(nounList.size()>=2){
							list=getHalfRelationsList();
							occurences=Long.parseLong(this.line[2]);
						}
					}
				}
			}
		}
		return list;
	}
	//check that the first edge from the root to the next word is not shared
	public boolean hasSharedPath(List<String[]> first,List<String[]> second){
		return first.get(first.size()-2)==second.get(second.size()-2);
	}
	//Split the tokens into two-dimensional array for easier access
	public  void parseTokens(){
		syntacticNGramTokens=new String[tokens.length][];
		for(int i=0;i<tokens.length;i++){
			syntacticNGramTokens[i]=tokens[i].split("/");
			if(syntacticNGramTokens[i].length!=4){
				syntacticNGramTokens=null;
				break;
			}
			syntacticNGramTokens[i][0]=morph.lemma(syntacticNGramTokens[i][0],syntacticNGramTokens[i][1]);
		}
	}

	public long getOccurences(){
		return this.occurences;
	}
	public  void getNounList(){
		nounList=new ArrayList<String[]>();
		for(String[] token:syntacticNGramTokens){
			if(isNounTag(token[1])&&!isStopWord(token[0])){
				nounList.add(token);
			}
		}
	}
	public String forwardBuildPathToHead(List<String[]> path){
		StringBuilder builder=new StringBuilder();
		for(int i=0;i<path.size();i++){
			if(i==0){
				builder.append(" ");
			}
			else if(i==path.size()-1){
				builder.append(path.get(i)[0]);
			}
			else{
				builder.append(path.get(i)[0]);
				builder.append(" ");
			}
		}
		return builder.toString();
	}
	public String forwardBuildPathFromHead(List<String[]> path){
		StringBuilder builder=new StringBuilder();
		for(int i=path.size()-1;i>=0;i--){
			if(i==0){
				//builder.append("Y");

			}
			else if(i==path.size()-1){
				builder.append(" ");
			}
			else{
				builder.append(path.get(i)[0]);
				builder.append(" ");
			}
		}
		return builder.toString();
	}
	/*
	public String forwardBuildPathToHead(List<String[]> path){
		StringBuilder builder=new StringBuilder();
		for(int i=0;i<path.size();i++){
			if(i==0){
				builder.append("X ");
			}
			else if(i==path.size()-1){
				builder.append(path.get(i)[0]);
			}
			else{
				builder.append(path.get(i)[0]);
				builder.append(" ");
			}
		}
		return builder.toString();
	}
	public String forwardBuildPathFromHead(List<String[]> path){
		StringBuilder builder=new StringBuilder();
		for(int i=path.size()-1;i>=0;i--){
			if(i==0){
				builder.append("Y");

			}
			else if(i==path.size()-1){
				builder.append(" ");
			}
			else{
				builder.append(path.get(i)[0]);
				builder.append(" ");
			}
		}
		return builder.toString();
	}
	
	public String inverseBuildPathToHead(List<String[]> path){
		StringBuilder builder=new StringBuilder();
		for(int i=0;i<path.size();i++){
			if(i==0){
				builder.append("Y ");
			}
			else if(i==path.size()-1){
				builder.append(path.get(i)[0]);
			}
			else{
				builder.append(path.get(i)[0]);
				builder.append(" ");
			}
		}
		return builder.toString();
	}
	public String inverseBuildPathFromHead(List<String[]> path){
		StringBuilder builder=new StringBuilder();
		for(int i=path.size()-1;i>=0;i--){
			if(i==0){
				builder.append("X");

			}
			else if(i==path.size()-1){
				builder.append(" ");
			}
			else{
				builder.append(path.get(i)[0]);
				builder.append(" ");
			}
		}
		return builder.toString();
	}*/
	//Get all the relation path from a noun to the head, exclude non-content words
	public  ArrayList<List<String[]>> getHalfRelationsList(){
		ArrayList<List<String[]>> halfRelationsList=new ArrayList<List<String[]>>();
		List<String[]> tempList;
		String[] tempToken;
		String[] prevTempToken = null;
		int wordIndex=0;
		for(String[] word:nounList){
			tempList=new LinkedList<String[]>();
			tempToken=word;
			//While the token is not the head
			while(!tempToken[3].equals("0")){
				//Check if the token contains a content word
				if(isContentWord(tempToken[1])&&isValidWord(tempToken[0])){
					if(isVerbTag(tempToken[1])){
						if(!isAuxiliaryVerb(tempToken[0])){
							if(prevTempToken==null){
								tempList.add(tempToken);
							}
							else if(isContentWord(prevTempToken[1])){
								tempList.add(tempToken);
							}
						}
					}
					else{
						if(prevTempToken==null){
							tempList.add(tempToken);
						}
						else if(isContentWord(prevTempToken[1])){
							tempList.add(tempToken);
						}
					}
				}
				wordIndex=Integer.parseInt(tempToken[3]);
				prevTempToken=tempToken;
				tempToken=syntacticNGramTokens[wordIndex-1];
			}

			tempList.add(tempToken);
			//If the path only contains the head, exclude it
			if(tempList.size()>=2){
				halfRelationsList.add(tempList);
			}
		}
		return halfRelationsList;
	}
	public boolean isHeadVerb(){
		for(String[] token:syntacticNGramTokens){
			if(token[3].equals("0")){
				if(isVerbTag(token[1])&&isValidWord(token[0])){
					return true;
				}
			}
		}
		return false;
	}
	public boolean isAuxiliaryVerb(String word,String tag){
		if(isVerbTag(tag)&&auxiliaryVerbSet.contains(word)){
			return true;
		}
		else{
			return false;
		}
	}
	public boolean isStopWord(String word){
		return this.stopWords.contains(word.toLowerCase());
	}
	public boolean isValidWord(String word){
		if(word.matches("[\\s]+")){
			return false;
		}
		else if(word.matches("[\\W]+")){
			return false;
		}
		return word.matches("[a-zA-Z\\s'-]+");
	}
	public boolean isAuxiliaryVerb(String word){
		return auxiliaryVerbSet.contains(word);
	}
	public  boolean isAdverbTag(String tag){
		return adverbTagsSet.contains(tag);
	}
	public  boolean isAdjectiveTag(String tag){
		return adjectiveTagsSet.contains(tag);
	}
	public  boolean isVerbTag(String tag){
		return verbTagsSet.contains(tag);
	}
	public  boolean isNounTag(String tag){
		return nounTagsSet.contains(tag);
	}
	//Check if the dep-label indicates that the word is not a content word
	//The dep-labels are taken from appendix A from the readme
	public  boolean isContentWord(String tag){
		return validTagSet.contains(tag);
	}
}
