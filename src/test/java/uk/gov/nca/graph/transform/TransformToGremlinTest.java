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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.nca.graph.utils.ElementUtils.getProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

public class TransformToGremlinTest {

    @Test
    public void test() throws Exception{
        Graph source = TinkerGraph.open();
        Vertex vP1 = source.addVertex(T.label, "Person", "name", "James", "email", "james@example.com");
        Vertex vP2 = source.addVertex(T.label, "Person", "name", "Simon", "email", "simon@example.com", "sameAs", "http://www.example.com/simon");
        Vertex vP3 = source.addVertex(T.label, "Person", "name", "Jim", "email", "james@example.com");

        Vertex vI1 = source.addVertex(T.label, "IPAddress", "identifier", "127.0.0.1");
        Vertex vI2 = source.addVertex(T.label, "IPAddress", "identifier", "127.0.0.2");
        Vertex vI3 = source.addVertex(T.label, "IPAddress", "identifier", "127.0.0.1");

        vP1.addEdge("uses", vI1);
        vP2.addEdge("uses", vI2);
        vP3.addEdge("uses", vI3);

        Graph target = TinkerGraph.open();
        TransformToGremlin.transformGraph(source, target, true);

        //People
        List<Vertex> peopleVertices = new ArrayList<>();
        target.traversal().V().has(T.label, "Person").fill(peopleVertices);
        assertEquals(3, peopleVertices.size());

        Map<Object,Vertex> people = new HashMap<>();
        peopleVertices.forEach(v -> people.put(getProperty(v,"name"), v));

        Vertex p1 = people.get("James");
        assertEquals("james@example.com", getProperty(p1,"email"));

        Vertex p2 = people.get("Simon");
        assertEquals("simon@example.com", getProperty(p2,"email"));
        assertEquals("http://www.example.com/simon", getProperty(p2,"sameAs"));

        Vertex p3 = people.get("Jim");
        assertEquals("james@example.com", getProperty(p3,"email"));

        //IPAddresses
        List<Vertex> ipVertices = new ArrayList<>();
        target.traversal().V().has(T.label, "IPAddress").fill(ipVertices);
        assertEquals(3, ipVertices.size());

        List<Object> identifiers = new ArrayList<>();
        ipVertices.forEach(v -> identifiers.add(getProperty(v, "identifier")));

        assertEquals(3, identifiers.size());
        assertTrue(identifiers.remove("127.0.0.1"));
        assertTrue(identifiers.remove("127.0.0.1"));
        assertTrue(identifiers.remove("127.0.0.2"));

        //Edges
        List<Edge> edges = new ArrayList<>();
        target.traversal().E().has(T.label, "uses").fill(edges);

        assertEquals(3, edges.size());

        List<String> edgeDescs = new ArrayList<>();
        edges.forEach(e -> edgeDescs.add(getProperty(e.outVertex(),"name") +"--"+e.label()+"->"+getProperty(e.inVertex(), "identifier")));

        assertTrue(edgeDescs.contains("James--uses->127.0.0.1"));
        assertTrue(edgeDescs.contains("Simon--uses->127.0.0.2"));
        assertTrue(edgeDescs.contains("Jim--uses->127.0.0.1"));

        source.close();
        target.close();
    }
}
