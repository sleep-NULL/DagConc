# DagConc
dag task concurrent execute tool

DagConc inspired by graph topological sort, use in degree table and adjacency table to control dag task concurrent execute, inspired by Spark stage segmentation and Flink operator chaining to reduce thread context switch
                                                                 
# Example
```java
Node[] nodes = new Node[]{
        new Node("hyf", 18),
        new Node("wm", 19),
        new Node("bb1", 20),
        new Node("bb2", 21),
};

Graph<List<Node>> graph = Graph.builder()
        .addNodes(nodes)
        .addEdge(nodes[0], nodes[1])
        .addEdge(nodes[1], nodes[2])
        .addEdge(nodes[1], nodes[3])
        .build().chaining();

ExecutorService executorService = Executors.newFixedThreadPool(i);
new ConcExecute<>(graph, executorService).executeSync((ns, submitTime) -> {
    for (Node node : ns) {
        System.out.println(node.name + " : " + node.age);
    }
});
```