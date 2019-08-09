package org.micro.tcc.common.serializer;

/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
public interface ObjectSerializer<T> {

    /**
     * Serialize the given object to binary data.
     *
     * @param t object to serialize
     *    the equivalent binary data
     */
    byte[] serialize(T t);

    /**
     * Deserialize an object from the given binary data.
     *
     * @param bytes object binary representation
     *    the equivalent object instance
     */
    T deserialize(byte[] bytes);


    T clone(T object);
}
