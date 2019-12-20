package com.github.dag.core.graph;

import com.github.dag.core.ConcExecute;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GraphTest {

    private ExecutorService pool = Executors.newFixedThreadPool(10);

    /***
     *      e(4)
     *    /   \
     *   b(1)  d(3)
     *   \     \
     *    \     c(2)
     *     \   /
     *       a(0)
     *
     *
     *      e(3)
     *    /   \
     *   b(1)  d(2)
     *   \     \
     *    \     c(2)
     *     \   /
     *       a(0)
     */
    @Test
    public void testChain1() {
        Graph<String> graph = Graph.builder()
                .addNodes(new String[]{"a", "b", "c", "d", "e"})
                .addEdge("a", "b")
                .addEdge("a", "c")
                .addEdge("c", "d")
                .addEdge("b", "e")
                .addEdge("d", "e")
                .build();
        System.out.println(graph);
        Graph<List<String>> chain = graph.chaining();
        System.out.println(chain);
        Assert.assertEquals("Graph(nodes=[[a], [b], [c, d], [e]], inDegree=[0, 1, 1, 2], adjacencyList=[[1, 2], [3], [3], []], outDegree=[2, 1, 1, 0], reverseAdjacencyList=[[], [0], [0], [1, 2]], zeroDegreeIdx=[0])", chain.toString());
        testExecute(chain);
    }


    /***
     *       d(3)
     *       |
     *       c(2)
     *       |
     *       b(1)
     *       |
     *       a(0)
     *
     *
     *
     *       d(0)
     *       |
     *       c(0)
     *       |
     *       b(0)
     *       |
     *       a(0)
     */
    @Test
    public void testChain2() {
        Graph<String> graph = Graph.builder()
                .addNodes(new String[]{"a", "b", "c", "d"})
                .addEdge("a", "b")
                .addEdge("b", "c")
                .addEdge("c", "d")
                .build();
        System.out.println(graph);
        Graph<List<String>> chain = graph.chaining();
        System.out.println(chain);
        Assert.assertEquals("Graph(nodes=[[a, b, c, d]], inDegree=[0], adjacencyList=[[]], outDegree=[0], reverseAdjacencyList=[[]], zeroDegreeIdx=[0])", chain.toString());
        testExecute(chain);
    }


    /***
     *       d(3)
     *       |
     *       c(2)
     *       |
     *       b(1)  f(5)
     *       |     |
     *       a(0)  e(4)
     *
     *
     *
     *       d(0)
     *       |
     *       c(0)
     *       |
     *       b(0)   f(1)
     *       |      |
     *       a(0)   e(1)
     */
    @Test
    public void testChain3() {
        Graph<String> graph = Graph.builder()
                .addNodes(new String[]{"a", "b", "c", "d", "e", "f"})
                .addEdge("a", "b")
                .addEdge("b", "c")
                .addEdge("c", "d")
                .addEdge("e", "f")
                .build();
        System.out.println(graph);
        Graph<List<String>> chain = graph.chaining();
        System.out.println(chain);
        Assert.assertEquals("Graph(nodes=[[a, b, c, d], [e, f]], inDegree=[0, 0], adjacencyList=[[], []], outDegree=[0, 0], reverseAdjacencyList=[[], []], zeroDegreeIdx=[0, 1])", chain.toString());
        testExecute(chain);
    }


    /***
     *
     *
     *
     *              e(4)
     *             /   \
     *           d(3)   h(7)
     *          /        \
     *         c(2)       g(6)
     *        /   \        \
     *       a(0)  b(1)     f(5)
     *
     *
     *
     *
     *              e(4)
     *             /   \
     *           d(3)   h(2)
     *          /        \
     *         c(3)       g(2)
     *        /   \        \
     *       a(0)  b(1)     f(2)
     *
     */
    @Test
    public void testChain4() {
        Graph<String> graph = Graph.builder()
                .addNodes(new String[]{"a", "b", "c", "d", "e", "f", "g", "h"})
                .addEdge("a", "c")
                .addEdge("b", "c")
                .addEdge("c", "d")
                .addEdge("d", "e")
                .addEdge("f", "g")
                .addEdge("g", "h")
                .addEdge("h", "e")
                .build();
        System.out.println(graph);
        Graph<List<String>> chain = graph.chaining();
        System.out.println(chain);
        Assert.assertEquals("Graph(nodes=[[a], [b], [f, g, h], [c, d], [e]], inDegree=[0, 0, 0, 2, 2], adjacencyList=[[3], [3], [4], [4], []], outDegree=[1, 1, 1, 1, 0], reverseAdjacencyList=[[], [], [], [0, 1], [2, 3]], zeroDegreeIdx=[0, 1, 2])", chain.toString());
        testExecute(chain);
    }


    /***
     *
     *
     *         f(6)
     *          |
     *         e(5)
     *          |
     *         d(4)
     *        /   \
     *       b(2)  c(3)
     *        \   /
     *         a(1)
     *         |
     *         z(0)
     *
     *
     *
     *         f(3)
     *          |
     *         e(3)
     *          |
     *         d(3)
     *        /   \
     *       b(1)  c(2)
     *        \   /
     *         a(0)
     *         |
     *         z(0)
     *
     */
    @Test
    public void testChain5() {
        Graph<String> graph = Graph.builder()
                .addNodes(new String[]{"z", "a", "b", "c", "d", "e", "f"})
                .addEdge("z", "a")
                .addEdge("a", "b")
                .addEdge("a", "c")
                .addEdge("b", "d")
                .addEdge("c", "d")
                .addEdge("d", "e")
                .addEdge("e", "f")
                .build();
        System.out.println(graph);
        Graph<List<String>> chain = graph.chaining();
        System.out.println(chain);
        Assert.assertEquals("Graph(nodes=[[z, a], [b], [c], [d, e, f]], inDegree=[0, 1, 1, 2], adjacencyList=[[1, 2], [3], [3], []], outDegree=[2, 1, 1, 0], reverseAdjacencyList=[[], [0], [0], [1, 2]], zeroDegreeIdx=[0])", chain.toString());
        testExecute(chain);
    }

    private void testExecute(Graph<List<String>> chain) {
        new ConcExecute<>(chain, pool).executeSync(new ConcExecute.Handler<List<String>>() {
            @Override
            public void handle(List<String> node, long submitTime) {
                System.out.println(node + " Done");
            }

            @Override
            public void cleanup(List<String> node) {
                System.out.println(node + " Cleanup");
            }
        });
    }

}
