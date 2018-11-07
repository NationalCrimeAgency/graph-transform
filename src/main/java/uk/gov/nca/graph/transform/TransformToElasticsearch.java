/*
National Crime Agency (c) Crown Copyright 2018

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uk.gov.nca.graph.transform;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.transform.rules.TransformRule;

/**
 * Transforms a graph into Elasticsearch, using all {@link TransformRule}s currently on the
 * classpath.
 */
public class TransformToElasticsearch {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformToElasticsearch.class);
  private static final int BULK_SIZE = 5000000;  //5MB Size

  private TransformToElasticsearch(){
    //Private constructor for utility class
  }

  /**
   * Process the sourceGraph using the transform rules on the classpath, and output the results
   * into Elasticsearch via the REST API.
   *
   * Alongside using the transform rules (whose indices will be prefixed with objIndexPrefix), the
   * raw content of each vertex will be added into indices prefixed with the rawIndexPrefix.
   *
   * This operation is performed multi-threaded where possible, with the number of threads set by
   * threadCount.
   */
  public static void transformGraph(Graph sourceGraph, RestClientBuilder targetClient, String rawIndexPrefix, String objIndexPrefix, int threadCount) {

    final String rawIndexPrefixNotNull = (rawIndexPrefix == null) ? "" : rawIndexPrefix;
    final String objIndexPrefixNotNull = (objIndexPrefix == null) ? "" : objIndexPrefix;

    RestHighLevelClient client = new RestHighLevelClient(targetClient);

    LOGGER.info("Checking connection to Elasticsearch");
    try {
      if (!client.ping(RequestOptions.DEFAULT)) {
        throw new IOException("Unable to ping server");
      }
    } catch (IOException ioe) {
      LOGGER.error("Unable to connect to Elasticsearch", ioe);
      return;
    }

    LOGGER.info("Transforming content from Graph to Elasticsearch (raw) using {} threads", threadCount);
    //Can't use sourceGraph.vertices() as it doesn't seem to work correctly across multiple threads (for TinkerGraph at least)
    Iterator<Vertex> iterVertices = sourceGraph.traversal().V().toList().iterator();

    Thread.UncaughtExceptionHandler h = (th, ex) -> LOGGER.error("Uncaught exception thrown by thread {}",
        th.getName(), ex);

    List<Thread> rawThreads = new ArrayList<>();
    for(int i = 0; i < threadCount; i++) {
      Thread t = new Thread(new RawTransformer(iterVertices, client, rawIndexPrefixNotNull));
      t.setUncaughtExceptionHandler(h);
      t.start();
      rawThreads.add(t);
    }

    while(rawThreads.stream().anyMatch(Thread::isAlive)){
      //Do nothing, wait for threads to finish
    }

    //TODO: Add a mapping

    //Loop through all the rules to produce processed objects
    LOGGER.info("Transforming content from Graph to Elasticsearch (processed), 1 thread per rule");

    ScanResult sr = new ClassGraph().enableClassInfo().scan();

    List<Class<TransformRule>> transformRulesClasses = sr.getClassesImplementing(TransformRule.class.getName())
        .loadClasses(TransformRule.class, true);

    List<Thread> ruleThreads = new ArrayList<>();


    for (Class<TransformRule> clazz : transformRulesClasses) {
      if (Modifier.isAbstract(clazz.getModifiers())) {
        continue;
      }

      LOGGER.info("Creating new thread for TransformRule {}", clazz.getName());

      TransformRule rule;
      try {
        rule = clazz.getConstructor().newInstance();
      } catch (Exception e) {
        LOGGER.error("Couldn't instantiate TransformRule {}", clazz.getName(), e);
        continue;
      }

      Thread t = new Thread(new RuleTransformer(rule, sourceGraph, client, objIndexPrefixNotNull));
      t.setUncaughtExceptionHandler(
          (th, ex) -> LOGGER.error("Uncaught exception thrown by thread {} ({})",
          th.getName(), rule.getClass().getSimpleName(), ex));
      ruleThreads.add(t);
      t.start();
    }

    while(ruleThreads.stream().anyMatch(Thread::isAlive)){
      //Do nothing, wait for threads to finish
    }

    try {
      client.close();
    } catch (IOException e) {
      //Do nothing, closing client anyway
    }
    LOGGER.info("Finished transforming to Elasticsearch");
  }

  private static synchronized void submitBulkRequest(RestHighLevelClient client, BulkRequest bulkRequest) {
    try {
      client.bulk(bulkRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      LOGGER.error("Unable to write vertices to Elasticsearch", e);
    }
  }

  private static class RawTransformer implements Runnable{

    private final Iterator<Vertex> vertexIterator;
    private final RestHighLevelClient restClient;
    private final String indexPrefix;

    public RawTransformer(Iterator<Vertex> vertexIterator, RestHighLevelClient restClient, String indexPrefix){
      this.vertexIterator = vertexIterator;
      this.restClient = restClient;
      this.indexPrefix = indexPrefix;
    }

    @Override
    public void run() {
      BulkRequest br = new BulkRequest();

      long count = 0;

      while(true) {
        Vertex v;

        if(!vertexIterator.hasNext())
          break;

        try {
          v = vertexIterator.next();
        } catch (NoSuchElementException e) {
          //No more vertices to process
          break;
        }

        if(!includeVertex(v))
          continue;

        //Transform from vertex to document
        Map<String, Object> doc = new HashMap<>();
        doc.put("originalId", v.id());

        v.properties().forEachRemaining(p -> doc.put(p.key(), p.value()));

        //Add vertex to bulk request
        String index = indexPrefix + v.label();
        br.add(new IndexRequest(index.toLowerCase(), "raw_" + v.label()).source(doc));
        count++;

        if (br.estimatedSizeInBytes() >= BULK_SIZE) {  //5MB Size
          submitBulkRequest(restClient, br);
          br = new BulkRequest();

          LOGGER.info("{} has ingested {} raw vertices", Thread.currentThread().getName(), count);
        }

      }

      if(br.numberOfActions() > 0)
        submitBulkRequest(restClient, br);

      LOGGER.info("{} has finished ingesting {} raw vertices", Thread.currentThread().getName(), count);
    }

    private static boolean includeVertex(Vertex v) {
      //Only keep vertices with properties
      return v.properties().hasNext();
    }
  }

  private static class RuleTransformer implements Runnable{

    private final TransformRule rule;
    private final Graph graph;
    private final RestHighLevelClient restClient;
    private final String indexPrefix;

    public RuleTransformer(TransformRule rule, Graph graph, RestHighLevelClient restClient, String indexPrefix){
      this.rule = rule;
      this.graph = graph;
      this.restClient = restClient;
      this.indexPrefix = indexPrefix;
    }

    @Override
    public void run() {
      BulkRequest br = new BulkRequest();

      long count = 0;

      for (Map<String, Object> obj : rule.transform(graph)) {   //TODO: Multi-thread this too?
        String index = indexPrefix + rule.getIndex();
        br.add(new IndexRequest(index.toLowerCase(), rule.getType()).source(obj));
        count++;

        if (br.estimatedSizeInBytes() >= BULK_SIZE) {
          submitBulkRequest(restClient, br);
          br = new BulkRequest();

          LOGGER.info("{} has ingested {} objects produced by rule {}", Thread.currentThread().getName(), count, rule.getClass().getSimpleName());
        }
      }

      if(br.numberOfActions() > 0)
        submitBulkRequest(restClient, br);

      LOGGER.info("{} has finished ingesting {} objects produced by rule {}", Thread.currentThread().getName(), count, rule.getClass().getSimpleName());
    }
  }
}
