package com.github.dag.core.graph;

import lombok.Builder;
import lombok.Data;

/**
 * Created by huangyafeng on 2019/6/7.
 */
@Data
@Builder
public class Edge<T> {
    private T from;
    private T to;
}
