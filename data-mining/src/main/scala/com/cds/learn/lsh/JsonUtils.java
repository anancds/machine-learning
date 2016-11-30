/**
 * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
 * All rights reserved.
 * <http://www.uniview.com/>
 * -----------------------------------------------------------
 * Product      :BigData
 * Module Name  :
 * Project Name :salut-parent
 * Package Name :com.uniview.salut.common.util
 * Date Created :2015/12/21
 * Creator      :c02132
 * Description  :
 * -----------------------------------------------------------
 * Modification History
 * Date        Name          Description
 * ------------------------------------------------------------
 * 2015/12/21      c02132         BigData project,new code file.
 * ------------------------------------------------------------
 */
package com.cds.learn.lsh;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Jackson wrap
 */
@SuppressWarnings("unused") public class JsonUtils {

    private static Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    private ObjectMapper mapper;

    private ObjectNode node;

    private JsonNode jsonNode;

    private ArrayNode arrayNode;

    public JsonUtils() {
        this(null);
    }

    public JsonUtils(Include include) {
        mapper = new ObjectMapper();
        node = mapper.createObjectNode();
        // 设置输出时包含属性的风格
        if (include != null) {
            mapper.setSerializationInclusion(include);
        }
        // 设置输入时忽略在JSON字符串中存在但Java对象实际没有的属性
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static JsonUtils nonEmptyMapper() {
        return new JsonUtils(Include.NON_EMPTY);
    }

    public static JsonUtils nonDefaultMapper() {
        return new JsonUtils(Include.NON_DEFAULT);
    }

    /**
     * Object可以是POJO，也可以是Collection或数组。
     * 如果对象为Null, 返回"null".
     * 如果集合为空集合, 返回"[]".
     */
    public String toJson(Object object) {

        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            logger.warn("write to json string error:" + object, e);
            return null;
        }
    }

    public String toJson() {
        try {
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            logger.warn("write to json string error:" + node, e);
            return null;
        }
    }

    //str是json字符串
    public JsonNode parseJson(String str) throws IOException {
        jsonNode = mapper.readTree(str);
        return jsonNode;
    }

    //key是json文件中的key。待调试，如果是数组怎么办？
    public String getString(String key) {
        return jsonNode.get(key).asText();
    }

    /**
     * 具体用法见测试用例putTest
     */
    public ObjectNode put(String key, Object value) {
        node.putPOJO(key, value);
        return node;
    }

    /**
     * 反序列化POJO或简单Collection如List<String>.
     * <p>
     * 如果JSON字符串为Null或"null"字符串, 返回Null.
     * 如果JSON字符串为"[]", 返回空集合.
     * <p>
     * 如需反序列化复杂Collection如List<MyBean>, 请使用fromJson(String, JavaType)
     *
     * @see #fromJson(String, JavaType)
     */
    public <T> T fromJson(String jsonString, Class<T> clazz) {
        if (!StringUtils.hasLength(jsonString)) {
            return null;
        }

        try {
            return mapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            logger.warn("parse json string error:" + jsonString, e);
            return null;
        }
    }

    /**
     * 反序列化复杂Collection如List<Bean>, 先使用createCollectionType()或contructMapType()构造类型, 然后调用本函数.
     */
    @SuppressWarnings("unchecked") public <T> T fromJson(String jsonString, JavaType javaType) {
        if (!StringUtils.hasLength(jsonString)) {
            return null;
        }

        try {
            return (T) mapper.readValue(jsonString, javaType);
        } catch (IOException e) {
            logger.warn("parse json string error:" + jsonString, e);
            return null;
        }
    }

    /**
     * 构造Collection类型.
     */
    public JavaType contructCollectionType(Class<? extends Collection> collectionClass,
        Class<?> elementClass) {
        return mapper.getTypeFactory().constructCollectionType(collectionClass, elementClass);
    }

    /**
     * 构造Map类型.
     */
    public JavaType contructMapType(Class<? extends Map> mapClass, Class<?> keyClass,
        Class<?> valueClass) {
        return mapper.getTypeFactory().constructMapType(mapClass, keyClass, valueClass);
    }

    /**
     * 当JSON里只含有Bean的部分屬性時，更新一個已存在Bean，只覆盖该部分的屬性.
     */
    public void update(String jsonString, Object object) {
        try {
            mapper.readerForUpdating(object).readValue(jsonString);
        } catch (IOException e) {
            logger.warn("update json string:" + jsonString + " to object:" + object + " error.", e);
        }
    }

    /**
     * 輸出JSONP格式數據.
     */
    public String toJsonP(String functionName, Object object) {
        return toJson(new JSONPObject(functionName, object));
    }

    /**
     * 設定是否使用Enum的toString函數來讀寫Enum,
     * 為False時時使用Enum的name()函數來讀寫Enum, 默認為False.
     * 注意本函數一定要在Mapper創建後, 所有的讀寫動作之前調用.
     */
    public void enableEnumUseToString() {
        mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
        mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
    }

    /**
     * 支持使用Jaxb的Annotation，使得POJO上的annotation不用与Jackson耦合。
     * 默认会先查找jaxb的annotation，如果找不到再找jackson的。
     */
    public void enableJaxbAnnotation() {
        JaxbAnnotationModule module = new JaxbAnnotationModule();
        mapper.registerModule(module);
    }

    /**
     * 取出Mapper做进一步的设置或使用其他序列化API.
     */
    public ObjectMapper getMapper() {
        return mapper;
    }

    public ObjectNode getNode() {
        return node;
    }
}
