package storm.cookbook.tfidf;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.washington.cs.knowitall.morpha.MorphaStemmer;
import storm.cookbook.snowball.ext.PorterStemmer;




public class TestStemmer {
	
	public static String stemTerm (String term) {
	    PorterStemmer stemmer = new PorterStemmer();
	    stemmer.setCurrent(term.toLowerCase());
	    stemmer.stem();
	    return stemmer.getCurrent();
	}
	
	public static String lemma(String term){
		return MorphaStemmer.stemToken(term);
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		File dir = new File(System.getProperty("user.home") + "/dictionaries");
		Directory directory;
		try {
			directory = FSDirectory.open(dir);
			SpellChecker spellchecker = new SpellChecker(directory);
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
			URL dictionaryFile = TestStemmer.class.getResource("/dictionaries/fulldictionary00.txt");
			spellchecker.indexDictionary(new PlainTextDictionary(new File(dictionaryFile.toURI())), config, true);
			String[] terms = new String[]{"climbed", "lazy", "learning", "misleading", "interestingly","passed", "sdfsdf", "1", "1.2"};
			for(int i = 0; i < terms.length; i++){
				System.out.println("Stem: " + stemTerm(terms[i]) + " Is Valid: " + spellchecker.exist(stemTerm(terms[i])));
				System.out.println("Lemma: " + lemma(terms[i]) + " Is Valid: " + spellchecker.exist(lemma(terms[i])));
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
