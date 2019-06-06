package com.github.dag.core.graph;

import lombok.Data;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

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
        Graph copy = new Graph();
        copy.nodes = this.nodes;
        copy.inDegree = inDegreeCopy();
        copy.zeroDegreeIdx = this.zeroDegreeIdx;
        copy.adjacencyList = this.adjacencyList;
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

}
