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

package uk.gov.nca.graph.transform.cli;

import static uk.gov.nca.graph.utils.cli.CommandLineUtils.createRequiredOption;
import static uk.gov.nca.graph.utils.cli.CommandLineUtils.parseCommandLine;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.transform.TransformToGremlin;
import uk.gov.nca.graph.utils.GraphUtils;

public class TransformGraphToGremlin {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformGraphToGremlin.class);

    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(createRequiredOption("i", "inputgraph", true, "Configuration file to connect to source Gremlin graph"));
        options.addOption(createRequiredOption("o", "outputgraph", true, "Configuration file to connect to target Gremlin graph"));
        options.addOption("p", "preserveid", false, "Preserve the original ID (as a new property)");

        CommandLine cmd = parseCommandLine(args, options, TransformGraphToGremlin.class, "Transform a graph from a Gremlin graph into a different Gremlin graph, filtering data as required");
        if(cmd == null)
            return;

        LOGGER.info("Connecting to source Gremlin graph");
        Graph sourceGraph = GraphFactory.open(cmd.getOptionValue('i'));

        LOGGER.info("Connecting to target Gremlin graph");
        Graph targetGraph = GraphFactory.open(cmd.getOptionValue('o'));

        try {
            TransformToGremlin.transformGraph(sourceGraph, targetGraph, cmd.hasOption('p'));
        }catch (Exception e){
            LOGGER.error("Error thrown whilst transforming graph", e);
        }

        LOGGER.info("Closing connection to target Gremlin graph");
        GraphUtils.closeGraph(targetGraph);

        LOGGER.info("Closing connection to source Gremlin graph");
        GraphUtils.closeGraph(sourceGraph);
    }
}
