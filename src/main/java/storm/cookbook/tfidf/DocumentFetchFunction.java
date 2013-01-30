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

	public DocumentFetchFunction(String[] supportedMimeTypes) {
		mimeTypes = Arrays.asList(supportedMimeTypes);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String url = tuple.getStringByField("url");
		try {
			Parser parser = new AutoDetectParser();
			Metadata metadata = new Metadata();
			ParseContext parseContext = new ParseContext();
			URL urlObject = new URL(url);
			ContentHandler handler = new BodyContentHandler(10 * 1024 * 1024);
			parser.parse((InputStream) urlObject.getContent(), handler,
					metadata, parseContext);
			String[] mimeDetails = metadata.get("Content-Type").split(";");
			if ((mimeDetails.length > 0)
					&& (mimeTypes.contains(mimeDetails[0]))) {
				collector.emit(new Values(handler.toString(), url.trim(), "twitter"));
			}
		} catch (Exception e) {
		}

	}

}
