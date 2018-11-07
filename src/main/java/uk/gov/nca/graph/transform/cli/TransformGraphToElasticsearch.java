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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.transform.TransformToElasticsearch;
import uk.gov.nca.graph.utils.GraphUtils;

public class TransformGraphToElasticsearch {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformGraphToElasticsearch.class);

    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(createRequiredOption("g", "graph", true, "Configuration file to connect to Gremlin graph"));

        options.addOption(new Option("r", "rawIndex", true, "Prefix to use for Elasticsearch indices when inserting raw data"));
        options.addOption(new Option("o", "objectIndex", true, "Prefix to use for Elasticsearch indices when inserting processed data"));
        options.addOption(new Option("s", "scheme", true, "Elasticsearch scheme"));
        options.addOption(new Option("h", "host", true, "Elasticsearch host"));
        options.addOption(new Option("p", "port", true, "Elasticsearch port"));
        options.addOption(new Option("c", "cluster", true, "Elasticsearch cluster"));
        options.addOption(new Option("u", "username", true, "Elasticsearch username"));
        options.addOption(new Option("w", "password", true, "Elasticsearch password"));
        options.addOption(new Option("j", "threads", true, "Thread count for ingesting raw data"));

        CommandLine cmd = parseCommandLine(args, options, TransformGraphToElasticsearch.class, "Transform a graph from a Gremlin graph into an Elasticsearch index, filtering data as required");
        if(cmd == null)
            return;

        Graph graph = null;
        try{
            LOGGER.info("Connecting to Gremlin graph");
            graph = GraphFactory.open(cmd.getOptionValue('g'));

            LOGGER.info("Connecting to Elasticsearch");
            String host = cmd.getOptionValue('h', "localhost");

            int port;
            try {
                port = Integer.parseInt(cmd.getOptionValue('p', "9200"));
            } catch (NumberFormatException nfe) {
                LOGGER.error("Unable to parse port, default will be used");
                port = 9200;
            }

            String scheme = cmd.getOptionValue('s', "https");

            Settings.Builder settings = Settings.builder();
            settings.put("cluster.name", cmd.getOptionValue('c', "elasticsearch"));

            String username = cmd.getOptionValue('u');
            String password = cmd.getOptionValue('w');

            RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));
            if (username != null && !username.isEmpty() && password != null && !password
                .isEmpty()) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

                builder = builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider)
                );
            }

            int threads;
            try {
                threads = Integer.parseInt(cmd.getOptionValue('j', "4"));
            } catch (NumberFormatException nfe) {
                LOGGER.error("Unable to parse threads, default will be used");
                threads = 4;
            }

            TransformToElasticsearch
                .transformGraph(graph, builder, cmd.getOptionValue('r'), cmd.getOptionValue('o'),
                    threads);
        }finally {
            if(graph != null) {
                LOGGER.info("Closing connection to Gremlin graph");
                GraphUtils.closeGraph(graph);
            }
        }

    }
}
