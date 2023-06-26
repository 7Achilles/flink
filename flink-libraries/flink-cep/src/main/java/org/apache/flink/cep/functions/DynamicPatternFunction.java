package org.apache.flink.cep.functions;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.cep.nfa.Callback;
import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;

/**
 * @author 7Achilles
 * @version V1.17.0
 * @description
 * @date 2023/06/14 14:06
 **/
public interface DynamicPatternFunction<T> extends Function, Serializable {

    /**
     *
     * @return Pattern<T, T>
     *
     * @author 7Achilles
     * @description TODO
     * @date 2023-06-25 17:10
     */

    Pattern<T, T> getPattern();

    /**
     * @return boolean
     *
     * @author 7Achilles
     * @description TODO
     * @date 2023-06-25 17:10
     */
    boolean isChanged();

    /**
     * @param callback:
     *
     * @author 7Achilles
     * @description TODO
     * @date 2023-06-25 17:11
     */
    void changed(Callback callback);

    /**
     * @param id:
     *
     * @author 7Achilles
     * @description TODO
     * @date 2023-06-25 17:11
     */
    void setId(String id);

    /**
     * @param check:
     *
     * @author 7Achilles
     * @description TODO
     * @date 2023-06-25 17:12
     */
    void setCheck(boolean check);

}
