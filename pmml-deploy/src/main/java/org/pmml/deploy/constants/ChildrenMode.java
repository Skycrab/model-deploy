package org.pmml.deploy.constants;

/**
 * Created by yihaibo on 19/3/5.
 */

/**
 * 子节点运行模式
 */
public enum ChildrenMode {
    /**
     * 顺序处理
     */
    PIPELINE,
    /**
     * 合并所有特征
     */
    UNION,

    ;
}
