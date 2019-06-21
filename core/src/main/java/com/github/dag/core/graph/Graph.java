package com.github.dag.core.graph;

import lombok.Data;

import java.util.*;

/**
 * Created by huangyafeng on 2019/6/6.
 */
@Data
public class Graph<T> {

    /**
     * 节点
     */
    private T[] nodes;

    /**
     * 节点入度表
     */
    private int[] inDegree;

    /**
     * 邻接表
     */
    private List<Integer>[] adjacencyList;

    /**
     * 入度为 0 的节点下标
     */
    private List<Integer> zeroDegreeIdx;

    private Graph(T[] nodes, int[] inDegree, List<Integer>[] adjacencyList, List<Integer> zeroDegreeIdx) {
        this.nodes = nodes;
        this.inDegree = inDegree;
        this.adjacencyList = adjacencyList;
        this.zeroDegreeIdx = zeroDegreeIdx;
    }

    public T getNodeByIdx(int idx) {
        return nodes[idx];
    }

    public List<Integer> getAdjacencyListByIdx(int idx) {
        return adjacencyList[idx];
    }

    /**
     * 生成 Graph 拷贝, 只有入度表为深拷贝
     *
     * @return
     */
    public Graph<T> copy() {
        Graph copy = new Graph(this.nodes, inDegreeCopy(), this.adjacencyList, this.zeroDegreeIdx);
        return copy;
    }

    /**
     * 基于拓扑排序校验是否为 DAG
     *
     * @return
     */
    public boolean isDAG() {
        int[] inDegreeCopy = inDegreeCopy();
        // deep copy
        Deque<Integer> queue = new LinkedList<>(zeroDegreeIdx);
        while (!queue.isEmpty()) {
            Integer e = queue.poll();
            List<Integer> adjacency = adjacencyList[e];
            if (adjacency != null) {
                for (Integer adj : adjacency) {
                    inDegreeCopy[adj]--;
                    if (inDegreeCopy[adj] == 0) {
                        queue.push(adj);
                    }
                }
            }
        }
        for (int i = 0; i < inDegreeCopy.length; i++) {
            if (inDegreeCopy[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 节点 chaining,便于后续执行,减少上下文切换开销
     *
     * @return
     */
    public Graph<List<T>> chaining() {
        // TODO
        return null;
    }

    /**
     * 拷贝入度表
     *
     * @return
     */
    private int[] inDegreeCopy() {
        int[] inDegreeCopy = new int[this.inDegree.length];
        System.arraycopy(this.inDegree, 0, inDegreeCopy, 0, this.inDegree.length);
        return inDegreeCopy;
    }

    public static Builder builder() {
        return new Builder<>();
    }

    /**
     * 非线程安全的 Graph 构造器
     *
     * @param <T>
     */
    public static class Builder<T> {

        private int idx = 0;

        private Map<T, Integer> nodeMap = new HashMap<>();

        private List<Edge<T>> edges = new ArrayList<>();

        /**
         * 添加一个节点
         *
         * @param node
         * @return
         */
        public Builder<T> addNode(T node) {
            nodeMap.put(node, idx++);
            return this;
        }

        /**
         * 添加一批节点
         *
         * @param nodes
         * @return
         */
        public Builder<T> addNodes(T... nodes) {
            for (T node : nodes) {
                nodeMap.put(node, idx++);
            }
            return this;
        }

        /**
         * 添加一个边
         *
         * @param from
         * @param to
         * @return
         */
        public Builder<T> addEdge(T from, T to) {
            edges.add(Edge.<T>builder()
                    .from(from)
                    .to(to)
                    .build());
            return this;
        }

        public Graph<T> build() {
            T[] nodes = (T[]) new Object[idx];
            nodeMap.forEach((node, index) -> nodes[index] = node);
            int[] inDegree = new int[idx];
            List<Integer>[] adjacencyList = new List[idx];
            for (Edge<T> edge : edges) {
                Integer fromIdx = nodeMap.get(edge.getFrom());
                Integer toIdx = nodeMap.get(edge.getTo());
                assert fromIdx != null;
                assert toIdx != null;
                // 入度表加 1
                inDegree[toIdx]++;
                // 邻接表添加后继节点索引
                List<Integer> adj = adjacencyList[fromIdx];
                if (adj == null) {
                    adj = new ArrayList<>();
                    adjacencyList[fromIdx] = adj;
                }
                adj.add(toIdx);
            }

            // 入度为 0 的节点
            List<Integer> zeroDegreeIdx = new ArrayList<>();
            for (int i = 0; i < inDegree.length; i++) {
                if (inDegree[i] == 0) {
                    zeroDegreeIdx.add(i);
                }
            }
            return new Graph<>(nodes, inDegree, adjacencyList, zeroDegreeIdx);
        }
    }

}
