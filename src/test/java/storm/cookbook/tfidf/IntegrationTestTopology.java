package storm.cookbook.tfidf;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.utils.Utils;

/**
 * The integration test basically injects on the input queue, and then
 * introduces a test bolt which simply persists the tuple into a JSON object
 * onto an output queue. Note that test is parameter driven, but the cluster is
 * only shutdown once all tests have run
 * */
public class IntegrationTestTopology {

	

}
