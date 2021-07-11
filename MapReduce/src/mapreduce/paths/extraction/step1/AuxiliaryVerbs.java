package mapreduce.paths.extraction.step1;

import java.util.HashSet;
import java.util.Set;

public class AuxiliaryVerbs {
	private static String[]  auxiliaryVerbs={
		"be","can","could","dare","do","have","may","might","must","need","ought","shall","should","will","would"
	};

	public static Set<String> getAuxiliaryVerbs(){
		Set<String> auxiliaryVerbsSet=new HashSet<String>();
		for(String word:auxiliaryVerbs){
			auxiliaryVerbsSet.add(word);
		}
		return auxiliaryVerbsSet;
	}
}
