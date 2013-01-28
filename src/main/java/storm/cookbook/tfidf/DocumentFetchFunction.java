package storm.cookbook.tfidf;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.URLEntity;
import backtype.storm.tuple.Values;

public class DocumentFetchFunction extends BaseFunction {
	
	private static final long serialVersionUID = 1L;
	private List<String> mimeTypes;
	
	public DocumentFetchFunction(String[] supportedMimeTypes){
		mimeTypes = Arrays.asList(supportedMimeTypes);
	}
	
	private String getHash(String url){
		MessageDigest md;
		try {
			byte[] bytesOfMessage = url.getBytes("UTF-8");
			md = MessageDigest.getInstance("MD5");
			byte[] thedigest = md.digest(bytesOfMessage);
			return Arrays.toString(thedigest);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Integer.toString(url.hashCode());
		} 
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		URLEntity[] urls = (URLEntity[]) tuple.getValueByField(TfidfTopologyFields.TWEET_URLS);
		for(int i = 0; i < urls.length; i++){
			try{
				Parser parser = new AutoDetectParser();
	            Metadata metadata = new Metadata();
	            ParseContext parseContext = new ParseContext();
	            URL url = new URL(urls[i].getExpandedURL());
	            ContentHandler handler = new BodyContentHandler(10*1024*1024);
	            parser.parse((InputStream) url.getContent(), handler, metadata, parseContext);
	            String[] mimeDetails = metadata.get("Content-Type").split(";");
	            if((mimeDetails.length > 0) && (mimeTypes.contains(mimeDetails[0]))){
	            	collector.emit(new Values(handler.toString(),"[URL]" + urls[i].getDisplayURL()));
	            }
			} catch(Exception e){}
		}
		//The tweet text is always a document itself
		//collector.emit(new Values(tuple.getStringByField(TfidfTopologyFields.TWEET_TEXT),
		//		"[TWEET]" + tuple.getValueByField(TfidfTopologyFields.TWEET_ID)));
	}

}
