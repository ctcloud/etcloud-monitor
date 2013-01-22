package storm.cookbook.tfidf;

import java.io.IOException;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Expectation;
import org.jmock.internal.ExpectationBuilder;
import org.jmock.internal.ExpectationCollector;
import org.jmock.lib.legacy.ClassImposteriser;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class StormTestCase {
    
	protected Mockery context = new Mockery() {{
        setImposteriser(ClassImposteriser.INSTANCE);
    }}; 

    protected Tuple getTuple(){
        final Tuple tuple = context.mock(Tuple.class);
        return tuple;
    }


}
