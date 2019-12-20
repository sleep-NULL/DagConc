package com.github.dag.core;

import com.github.dag.core.graph.Graph;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Created by huangyafeng on 2019/6/6.
 */
@Slf4j
public class ConcExecute<T> {

    private CountDownLatch latch;

    private ExecutorService executeBackend;

    private Graph<T> graph;

    private AtomicIntegerArray inDegree;

    private AtomicIntegerArray outDegree;

    private volatile Thread currentThread;

    private volatile Exception firstException;

    private AtomicBoolean running = new AtomicBoolean(true);

    public ConcExecute(Graph<T> graph, ExecutorService executeBackend) {
        this.graph = graph;
        this.inDegree = new AtomicIntegerArray(graph.getInDegree());
        this.outDegree = new AtomicIntegerArray(graph.getOutDegree());
        this.latch = new CountDownLatch(this.graph.getNodes().length);
        this.executeBackend = executeBackend;
    }

    /**
     * 同步执行
     *
     * @param handler
     * @return
     */
    public Optional<Exception> executeSync(Handler<T> handler) {
        executeAsync(handler);
        return await();
    }

    /**
     * 异步执行
     *
     * @param handler
     */
    public void executeAsync(Handler<T> handler) {
        this.currentThread = Thread.currentThread();
        for (Integer idx : graph.getZeroDegreeIdx()) {
            iter(handler, graph.getNodeByIdx(idx), idx);
        }
    }

    /**
     * 异步执行调用 await 等待直接结束
     *
     * @return
     */
    public Optional<Exception> await() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            // ignore
        }
        return Optional.ofNullable(firstException);
    }

    /**
     * 递归执行
     *
     * @param handler
     * @param node
     * @param idx
     */
    private void iter(Handler<T> handler, T node, int idx) {
        long submitTime = System.currentTimeMillis();
        executeBackend.execute(() -> {
            try {
                if (running.get()) {
                    handler.handle(node, submitTime);
                    Optional.ofNullable(graph.getReverseAdjacencyListByIdx(idx)).ifPresent(
                            list -> {
                                for (Integer reverseAdjIdx : list) {
                                    // 对反向邻接节点出度减 1, 若出度减到 0 则执行清理工作
                                    if (outDegree.decrementAndGet(reverseAdjIdx) == 0) {
                                        T reverseAdjNode = graph.getNodeByIdx(reverseAdjIdx);
                                        handler.cleanup(reverseAdjNode);
                                    }
                                }
                            }
                    );
                    latch.countDown();
                    Optional.ofNullable(graph.getAdjacencyListByIdx(idx)).ifPresent(
                            list -> {
                                for (Integer adjIdx : list) {
                                    // 对邻接节点的入度减 1, 若入度减到 0 则执行邻接节点任务
                                    if (inDegree.decrementAndGet(adjIdx) == 0) {
                                        T adjNode = graph.getNodeByIdx(adjIdx);
                                        iter(handler, adjNode, adjIdx);
                                    }
                                }
                            }
                    );
                }
            } catch (Exception e) {
                // 确保只 interrupt 一次主线程
                if (running.compareAndSet(true, false)) {
                    firstException = e;
                    currentThread.interrupt();
                }
                log.error("ConcExecute execute node {} failed.", idx, e);
            }
        });
    }

    /**
     * 通用的基于节点的处理方法,待实现
     *
     * @param <T>
     */
    public interface Handler<T> {
        void handle(T node, long submitTime);

        /**
         * 节点下游节点全部执行完毕,可以考虑进行清理工作,默认为空实现
         */
        default void cleanup(T node) {
        }

    }
}
