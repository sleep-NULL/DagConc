package com.github.dag.core;

import com.github.dag.core.graph.Graph;
import org.junit.Test;

import java.util.ArrayList;
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
    }

    @Test
    public void test() {
        Graph<Node> graph = new Graph<>();
        Node[] nodes = new Node[]{
                new Node("hyf", 18),
                new Node("wm", 19),
                new Node("bb1", 20),
                new Node("bb2", 21),
        };
        graph.setNodes(nodes);
        graph.setInDegree(new int[]{0, 1, 1, 1});
        graph.setAdjacencyList(new List[]{
                new ArrayList() {{
                    add(1);
                }},
                new ArrayList() {{
                    add(2);
                }},
                new ArrayList() {{
                    add(3);
                }},
                null
        });
        graph.setZeroDegreeIdx(new ArrayList<Integer>() {{
            add(0);
        }});
        System.out.println(graph.toString());
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        new ConcExecute<>(graph, executorService).executeSync((node, submitTime) -> {
            System.out.println(node.name);
        });
    }


}
