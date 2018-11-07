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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.utils.GraphUtils;

/**
 * Transform a source graph into another Tinkerpop graph. This is different to copying, as copying
 * may require nodes in the source graph and existing nodes in the target graph to have unique IDs;
 * whereas transforming creates new IDs (optionally you can add the original ID as a property).
 */
public class TransformToGremlin {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformToGremlin.class);

  private TransformToGremlin(){
    //Private constructor for utility class
  }

  /**
   * Transform a source graph into a target graph, without preserving the original ID
   */
  public static void transformGraph(Graph sourceGraph, Graph targetGraph) {
    transformGraph(sourceGraph, targetGraph, false);
  }

  /**
   * Transform a source graph into a target graph
   */
  public static void transformGraph(Graph sourceGraph, Graph targetGraph, boolean preserveOriginalId) {
    Map<Object, Object> ids = new HashMap<>();

    LOGGER.info("Transforming vertices from Graph to Graph");
    long vertexCount = 0L;

    Iterator<Vertex> iterVertices = sourceGraph.vertices();
    while(iterVertices.hasNext()){
      Vertex v = iterVertices.next();

      if(includeVertex(v)) {
        vertexCount++;

        List<Object> vertexArguments = new ArrayList<>();
        vertexArguments.add(T.label);
        vertexArguments.add(v.label());

        if (preserveOriginalId) {
          vertexArguments.add("originalId");
          vertexArguments.add(v.id());
        }

        v.properties().forEachRemaining(vp -> {
          if (vp.isPresent() && vp.value() != null) {
            vertexArguments.add(vp.key());
            vertexArguments.add(vp.value());
          }
        });

        Vertex newV = targetGraph.addVertex(vertexArguments.toArray(new Object[vertexArguments.size()]));
        ids.put(v.id(), newV.id());
      }

      if (vertexCount % 10000 == 0)
        LOGGER.info("{} vertices processed", vertexCount);
    }

    LOGGER.info("Finished processing {} vertices", vertexCount);

    LOGGER.info("Transforming edges from Graph to Graph");
    long edgeCount = 0L;

    Iterator<Edge> iterEdges = sourceGraph.edges();
    while(iterEdges.hasNext()){
      Edge e = iterEdges.next();
      edgeCount++;

      if(includeEdge(e)) {
        Object outId = ids.get(e.outVertex().id());
        Object inId = ids.get(e.inVertex().id());

        if (outId == null || inId == null) {
          LOGGER.warn("Couldn't find ID in map");
          return;
        }

        Iterator<Vertex> src = targetGraph.vertices(outId);
        Iterator<Vertex> tgt = targetGraph.vertices(inId);

        if (src.hasNext() && tgt.hasNext()) {
          List<Object> edgeArguments = new ArrayList<>();

          e.properties().forEachRemaining(ep -> {
            if (ep.isPresent() && ep.value() != null) {
              edgeArguments.add(ep.key());
              edgeArguments.add(ep.value());
            }
          });

          src.next().addEdge(e.label(), tgt.next(),
              edgeArguments.toArray(new Object[edgeArguments.size()]));
        }
      }

      if (edgeCount % 10000 == 0)
        LOGGER.info("{} edges processed", edgeCount);
    }

    LOGGER.info("Finished processing {} edges", edgeCount);

    LOGGER.info("Committing graph");
    GraphUtils.commitGraph(targetGraph);
  }

  private static boolean includeVertex(Vertex v) {
    return v != null;
  }
  private static boolean includeEdge(Edge e) {
    return e != null;
  }
}
