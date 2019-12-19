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

    private volatile Thread currentThread;

    private volatile Exception firstException;

    private AtomicBoolean running = new AtomicBoolean(true);

    public ConcExecute(Graph<T> graph, ExecutorService executeBackend) {
        this.graph = graph;
        this.inDegree = new AtomicIntegerArray(graph.getInDegree());
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
                handler.handle(node, submitTime);
                latch.countDown();
            } catch (Exception e) {
                // 确保只 interrupt 一次主线程
                if (running.compareAndSet(true, false)) {
                    firstException = e;
                    currentThread.interrupt();
                }
                log.error("ConcExecute execute node {} failed.", idx, e);
                // 直接抛出异常,无需执行后续节点
                throw e;
            }
            Optional.ofNullable(graph.getAdjacencyListByIdx(idx)).ifPresent(
                    list -> {
                        for (Integer adjIdx : list) {
                            T adjNode = graph.getNodeByIdx(adjIdx);
                            // 对邻接节点的入度减 1, 若入度减到 0 则执行邻接节点任务
                            if (inDegree.decrementAndGet(adjIdx) == 0) {
                                iter(handler, adjNode, adjIdx);
                            }
                        }
                    }
            );
        });
    }

    /**
     * 通用的基于节点的处理方法,待实现
     *
     * @param <T>
     */
    public interface Handler<T> {
        void handle(T node, long submitTime);
    }
}
