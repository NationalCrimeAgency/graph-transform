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

package uk.gov.nca.graph.transform.rules;

import java.util.Collection;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Interface describing how to transform a graph into flat objects for
 * storing in mediums such as Elasticsearch
 */
public interface TransformRule {

  /**
   * Returns a collection of Map objects representing the content in the graph
   * to be processed by this rule.
   */
  Collection<Map<String, Object>> transform(Graph graph);

  /**
   * The name of the index into which this rule should output results
   */
  String getIndex();

  /**
   * The document type to associate with the outputs of this rule
   */
  String getType();
}
