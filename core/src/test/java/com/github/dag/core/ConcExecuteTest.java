package com.github.dag.core;

import com.github.dag.core.graph.Graph;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangyafeng on 2019/6/7.
 */
public class ConcExecuteTest {

    class Node {
        String name;
        int age;

        public Node(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    @Test
    public void test() {
        Node[] nodes = new Node[]{
                new Node("hyf", 18),
                new Node("wm", 19),
                new Node("bb1", 20),
                new Node("bb2", 21),
        };
        Graph<Node> graph = Graph.builder()
                .addNodes(nodes)
                .addEdge(nodes[0], nodes[1])
                .addEdge(nodes[1], nodes[2])
                .addEdge(nodes[2], nodes[3])
                .build();

        Assert.assertEquals(graph.isDAG(), true);

        System.out.println(graph.toString());
        ExecutorService executorService = getExecutorService(10);
        new ConcExecute<>(graph, executorService).executeSync((node, submitTime) -> {
            System.out.println(node.name + " : " + node.age);
        });
    }

    @Test
    public void test1() {
        Node[] nodes = new Node[]{
                new Node("test0", 0),
                new Node("test1", 1),
                new Node("test2", 2),
                new Node("test3", 3),
                new Node("test4", 4),
                new Node("test5", 5),
                new Node("test6", 6),
                new Node("test7", 7),
        };
        Graph<Node> graph = Graph.builder()
                .addNodes(nodes)
                .addEdge(nodes[0], nodes[1])
                .addEdge(nodes[0], nodes[2])
                .addEdge(nodes[0], nodes[3])
                .addEdge(nodes[2], nodes[4])
                .addEdge(nodes[3], nodes[4])
                .addEdge(nodes[0], nodes[6])
                .addEdge(nodes[0], nodes[7])
                .build();

        Assert.assertEquals(graph.isDAG(), true);

        System.out.println(graph.toString());
        ExecutorService executorService = getExecutorService(2);
        new ConcExecute<>(graph, executorService).executeSync((node, submitTime) -> {
            System.out.println(node.name + " : " + node.age);
        });
    }

    @Test
    public void test2() {
        Node[] nodes = new Node[]{
                new Node("test0", 0),
                new Node("test1", 1),
                new Node("test2", 2),
                new Node("test3", 3),
                new Node("test4", 4),
                new Node("test5", 5),
                new Node("test6", 6),
                new Node("test7", 7),
        };
        Graph<Node> graph = Graph.builder()
                .addNodes(nodes)
                .addEdge(nodes[0], nodes[1])
                .addEdge(nodes[1], nodes[2])
                .addEdge(nodes[0], nodes[3])
                .addEdge(nodes[2], nodes[4])
                .addEdge(nodes[3], nodes[4])
                .addEdge(nodes[0], nodes[6])
                .addEdge(nodes[2], nodes[0])
                .build();

        Assert.assertEquals(graph.isDAG(), false);
    }

    @Test
    public void test3() {
        Node[] nodes = new Node[]{
                new Node("test0", 0),
                new Node("test1", 1),
                new Node("test2", 2),
                new Node("test3", 3),
                new Node("test4", 4),
                new Node("test5", 5),
                new Node("test6", 6),
                new Node("test7", 7),
        };
        Graph<List<Node>> graph = Graph.builder()
                .addNodes(nodes)
                .addEdge(nodes[0], nodes[1])
                .addEdge(nodes[1], nodes[2])
                .addEdge(nodes[2], nodes[3])
                .addEdge(nodes[2], nodes[4])
                .addEdge(nodes[3], nodes[6])
                .addEdge(nodes[4], nodes[7])
                .build().chaining();

        System.out.println(graph);

        ExecutorService executorService = getExecutorService(2);
        new ConcExecute<>(graph, executorService).executeSync((ns, submitTime) -> {
            for (Node node : ns) {
                System.out.println(node.name + " : " + node.age);
            }
        });
    }

    @Test
    public void test4() {
        Node[] nodes = new Node[]{
                new Node("test0", 0),
                new Node("test1", 1),
                new Node("test2", 2),
                new Node("test3", 3),
                new Node("test4", 4),
        };
        Graph<List<Node>> graph = Graph.builder()
                .addNodes(nodes)
                .addEdge(nodes[0], nodes[1])
                .addEdge(nodes[1], nodes[2])
                .addEdge(nodes[2], nodes[3])
                .addEdge(nodes[3], nodes[4])
                .build().chaining();

        ExecutorService executorService = getExecutorService(2);
        new ConcExecute<>(graph, executorService).executeSync((ns, submitTime) -> {
            for (Node node : ns) {
                System.out.println(node.name + " : " + node.age);
            }
        });
    }

    private ExecutorService getExecutorService(int i) {
        ExecutorService executorService = Executors.newFixedThreadPool(i);
        // 初始化线程
        for (int j = 0; j < i; j++) {
            executorService.submit(() -> {
                // no op
            });
        }
        return executorService;
    }


}
