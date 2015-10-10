/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.transforms;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Transform;
import com.clearspring.analytics.util.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class ParseCSVTest {

  private static final Schema INPUT1 = Schema.recordOf("input1",
                                                       Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  
  private static final Schema OUTPUT1 = Schema.recordOf("output1",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.STRING)));

  private static final Schema OUTPUT2 = Schema.recordOf("output2",
                                                        Schema.Field.of("a", Schema.of(Schema.Type.LONG)),
                                                        Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                                                        Schema.Field.of("c", Schema.of(Schema.Type.INT)),
                                                        Schema.Field.of("d", Schema.of(Schema.Type.DOUBLE)),
                                                        Schema.Field.of("e", Schema.of(Schema.Type.BOOLEAN)));


  @Test
  public void testDefaultCSVParser() throws Exception {
    String s = OUTPUT1.toString();
    String commaDelimiter = ",";

    ParseCSV.Config config = new ParseCSV.Config("DEFAULT", "body", commaDelimiter, OUTPUT1.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new ParseCSV(config);
    transform.initialize(null);

    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    
    // Test missing field. 
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,4,").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("", emitter.getEmitted().get(0).get("e"));

    // Test adding quote to field value. 
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,'4',5").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("'4'", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));

    // Test adding spaces in a field and quoted field value. 
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2, 3 ,'4',5").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(" 3 ", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("'4'", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
    
    // Test Skipping empty lines.
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,4,5\n\n").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
    Assert.assertEquals(1, emitter.getEmitted().size());
    
    // Test multiple records
    emitter.clear();
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "1,2,3,4,5\n6,7,8,9,10").build(), emitter);
    Assert.assertEquals("1", emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("2", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals("3", emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals("4", emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals("5", emitter.getEmitted().get(0).get("e"));
    Assert.assertEquals("6", emitter.getEmitted().get(1).get("a"));
    Assert.assertEquals("7", emitter.getEmitted().get(1).get("b"));
    Assert.assertEquals("8", emitter.getEmitted().get(1).get("c"));
    Assert.assertEquals("9", emitter.getEmitted().get(1).get("d"));
    Assert.assertEquals("10", emitter.getEmitted().get(1).get("e"));
    
    // Test with records supporting different types. 
    emitter.clear();
    ParseCSV.Config config1 = new ParseCSV.Config("DEFAULT", "body", commaDelimiter, OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform1 = new ParseCSV(config1);
    transform1.initialize(null);
    
    transform1.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,3,4.32,true").build(), emitter);
    Assert.assertEquals(10L, emitter.getEmitted().get(0).get("a"));
    Assert.assertEquals("stringA", emitter.getEmitted().get(0).get("b"));
    Assert.assertEquals(3, emitter.getEmitted().get(0).get("c"));
    Assert.assertEquals(4.32, emitter.getEmitted().get(0).get("d"));
    Assert.assertEquals(true, emitter.getEmitted().get(0).get("e"));
  }
  
  @Test(expected=RuntimeException.class)
  public void testDoubleException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    String commaDelimiter = ",";

    ParseCSV.Config config = new ParseCSV.Config("DEFAULT", "body", commaDelimiter, OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new ParseCSV(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,3,,true").build(), emitter);
  }

  @Test(expected=RuntimeException.class)
  public void testIntException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    String commaDelimiter = ",";

    ParseCSV.Config config = new ParseCSV.Config("DEFAULT", "body", commaDelimiter, OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new ParseCSV(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", "10,stringA,,4.32,true").build(), emitter);
  }

  @Test(expected=RuntimeException.class)
  public void testLongException() throws Exception {
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    String commaDelimiter = ",";

    ParseCSV.Config config = new ParseCSV.Config("DEFAULT", "body", commaDelimiter, OUTPUT2.toString());
    Transform<StructuredRecord, StructuredRecord> transform = new ParseCSV(config);
    transform.initialize(null);
    transform.transform(StructuredRecord.builder(INPUT1)
                          .set("body", ",stringA,3,4.32,true").build(), emitter);
  }
//
//  @Test
//  public void testFoo() throws Exception {
//    List<String> words = Lists.newArrayList();
//    words.add("this");
//    words.add("the");
//    words.add("is");
//    words.add("a");
//    words.add("of");
//    CharArraySet stopWords = StopFilter.makeStopSet(words);
//
//    List<String> result = Lists.newArrayList();
//    Analyzer analyzer = new SimpleAnalyzer();
//    TokenStream stream = analyzer.tokenStream(null, "This is a great example of text analysis");
//    stream = new StandardFilter(stream);
//    stream = new StopFilter(stream, stopWords);
//    stream = new PorterStemFilter(stream);
//
//    stream.reset();
//    while(stream.incrementToken()) {
//      result.add(stream.getAttribute(CharTermAttribute.class).toString());
//    }
//    stream.end();
//    stream.close();
//    Assert.assertEquals(1, result.size());
//  }

}