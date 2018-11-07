# Graph Transform

Tools to transform TinkerPop graphs into other mediums, namely another TinkerPop graph
or into Elasticsearch. Transformations into Elasticsearch are governed by classes
implementing the TransformRule interface, which programmatically describes how to convert
motifs within the graph into a flat structure suitable for storing in Elasticsearch. These
rules are not provided with this project.

Two command line tools, `TransformGraphToElasticsearch` and `TransformGraphToGremlin`
are also provided, and can be used to invoke transformations from the command line rather
than in code. To see the available options for each tool, call the tool directly without
any options.

E.g. `java -cp transform-1.1-shaded.jar uk.gov.nca.graph.transform.cli.TransformGraphToGremlin`