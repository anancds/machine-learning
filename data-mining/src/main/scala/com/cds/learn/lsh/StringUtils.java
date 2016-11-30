/**
 * Copyright (c) 2015, zhejiang Unview Technologies Co., Ltd.
 * All rights reserved.
 * <http://www.uniview.com/>
 * -----------------------------------------------------------
 * Product      :BigData
 * Module Name  :
 * Project Name :BigdataModules
 * Package Name :com.uniview.salut.common.utils
 * Date Created :2015/11/2
 * Creator      :c02132
 * Description  :
 * -----------------------------------------------------------
 * Modification History
 * Date        Name          Description
 * ------------------------------------------------------------
 * 2015/11/2      c02132         BigData project,new code file.
 * ------------------------------------------------------------
 */
package com.cds.learn.lsh;

import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * String类库
 */
public class StringUtils {


    /**
     * 对给定的字符序列判空
     * StringUtils.hasLength(null) = false
     * StringUtils.hasLength("") = false
     * StringUtils.hasLength(" ") = true
     * @param str
     * @return
     */
    public static boolean hasLength(CharSequence str) {
        return (null != str && str.length() > 0);
    }

    public static boolean hasLength(String str) {
        return hasLength((CharSequence) str);
    }

    public static boolean hasText(CharSequence str) {
        if (!hasLength(str)) {
            return false;
        }
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasText(String str) {
        return hasText((CharSequence) str);
    }

    public static final String EMPTY = "";
    public static final String SEPARATOR_MULTI = ";";
    public static final String SEPARATOR_SINGLE = "#";
    public static final String SQL_REPLACE = "_";

    private static final String BOOLEAN_TRUE_STRING = "true";
    private static final String BOOLEAN_FALSE_STRING = "false";
    private static final String BOOLEAN_TRUE_NUMBER = "1";
    private static final String BOOLEAN_FALSE_NUMBER = "0";

    /**
     * 在str右边加入足够多的addStr字符串
     *
     * @param str
     * @param addStr
     * @param length
     * @return
     */
    public static String addStringRight(String str, String addStr, int length) {
        StringBuilder builder = new StringBuilder(str);
        while (builder.length() < length) {
            builder.append(addStr);
        }
        return builder.toString();
    }

    /**
     * 在字符串str右边补齐0直到长度等于length
     *
     * @param str
     * @param length
     * @return
     */
    public static String addZeroRight(String str, int length) {
        return addStringRight(str, "0", length);
    }

    /**
     * 计算字符串str中字符sub的个数
     *
     * @param str
     * @param sub
     * @return
     */
    public static int charCount(String str, char sub) {
        int charCount = 0;
        int fromIndex = 0;

        while ((fromIndex = str.indexOf(sub, fromIndex)) != -1) {
            fromIndex++;
            charCount++;
        }
        return charCount;
    }

    /**
     * 计算字符串str右边出现多少次sub
     *
     * @param str
     * @param sub
     * @return
     */
    public static int charCountRight(String str, String sub) {
        if (str == null) {
            return 0;
        }

        int charCount = 0;
        String subStr = str;
        int currentLength = subStr.length() - sub.length();
        while (currentLength >= 0 && subStr.substring(currentLength).equals(sub)) {
            charCount++;
            subStr = subStr.substring(0, currentLength);
            currentLength = subStr.length() - sub.length();
        }
        return charCount;
    }


    /**
     * 在字符串str左边补齐0直到长度等于length
     *
     * @param str
     * @param len
     * @return
     */
    public static String enoughZero(String str, int len) {
        while (str.length() < len) {
            str = "0" + str;
        }
        return str;
    }



    /**
     * 格式化浮点型数字成字符串, 保留两位小数位.
     *
     * @param number
     *            浮点数字
     * @return 格式化之后的字符串
     */
    public static String formatDecimal(double number) {
        NumberFormat format = NumberFormat.getInstance();

        format.setMaximumIntegerDigits(Integer.MAX_VALUE);
        format.setMaximumFractionDigits(2);
        format.setMinimumFractionDigits(2);

        return format.format(number);
    }

    /**
     * 格式化浮点类型数据.
     *
     * @param number
     *            浮点数据
     * @param minFractionDigits
     *            最小保留小数位
     * @param maxFractionDigits
     *            最大保留小数位
     * @return 将浮点数据格式化后的字符串
     */
    public static String formatDecimal(double number, int minFractionDigits, int maxFractionDigits) {
        NumberFormat format = NumberFormat.getInstance();
        format.setMinimumFractionDigits(minFractionDigits);
        format.setMaximumFractionDigits(minFractionDigits);

        return format.format(number);
    }

    /**
     * 取得字符串的真实长度，一个汉字长度为两个字节。
     *
     * @param str
     *            字符串
     * @return 字符串的字节数
     */
    public static int getRealLength(String str) {
        if (str == null) {
            return 0;
        }

        char separator = 256;
        int realLength = 0;

        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) >= separator) {
                realLength += 2;
            }
            else {
                realLength++;
            }
        }
        return realLength;
    }

    /**
     * HTML 文本过滤，如果 value 为 <code>null</code> 或为空串，则返回 "&amp;nbsp;"。
     *
     * <p>
     * 转换的字符串关系如下：
     *
     * <ul>
     * <li>&amp; --> &amp;amp;</li>
     * <li>&lt; --> &amp;lt;</li>
     * <li>&gt; --> &amp;gt;</li>
     * <li>&quot; --> &amp;quot;</li>
     * <li>\n --> &lt;br/&gt;</li>
     * <li>\t --> &amp;nbsp;&amp;nbsp;&amp;nbsp;&amp;nbsp;</li>
     * <li>空格 --> &amp;nbsp;</li>
     * </ul>
     *
     * <strong>此方法适用于在 HTML 页面上的非文本框元素（div、span、table 等）中显示文本时调用。</strong>
     *
     * @param value
     *            要过滤的文本
     * @return 过滤后的 HTML 文本
     */
    public static String htmlFilter(String value) {
        if (value == null || value.length() == 0) {
            return "&nbsp;";
        }

        return value.replaceAll("&", "&amp;").replaceAll("\t", "    ").replaceAll(" ", "&nbsp;")
                .replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("\"", "&quot;").replaceAll("\n", "<br/>");
    }

    /**
     * HTML 文本过滤，如果 value 为 <code>null</code> 或为空串，则返回空串。
     *
     * <p>
     * 转换的字符串关系如下：
     *
     * <ul>
     * <li>&amp; --> &amp;amp;</li>
     * <li>&lt; --> &amp;lt;</li>
     * <li>&gt; --> &amp;gt;</li>
     * <li>&quot; --> &amp;quot;</li>
     * <li>\n --> &lt;br/&gt;</li>
     * </ul>
     *
     * <strong>此方法适用于在 HTML 页面上的文本框（text、textarea）中显示文本时调用。</strong>
     *
     * @param value
     *            要过滤的文本
     * @return 过滤后的 HTML 文本
     */
    public static String htmlFilterToEmpty(String value) {
        if (value == null || value.length() == 0) {
            return "";
        }

        return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
                .replaceAll("\"", "&quot;");
    }



    /**
     * 清除字符串左侧的空格
     *
     * @param str
     * @return
     */
    public static String ltrim(String str) {
        return ltrim(str, " ");
    }

    /**
     * 清除字符串左侧的指定字符串
     *
     * @param str
     * @param remove
     * @return
     */
    public static String ltrim(String str, String remove) {
        if (str == null || str.length() == 0 || remove == null || remove.length() == 0) {
            return str;
        }

        while (str.startsWith(remove)) {
            str = str.substring(remove.length());
        }
        return str;
    }

    /**
     * 清除字符串右侧的空格
     *
     * @param str
     * @return
     */
    public static String rtrim(String str) {
        return rtrim(str, " ");
    }

    /**
     * 清除字符串右侧的指定字符串
     *
     * @param str
     * @param remove
     * @return
     */
    public static String rtrim(String str, String remove) {
        if (str == null || str.length() == 0 || remove == null || remove.length() == 0) {
            return str;
        }

        while (str.endsWith(remove) && (str.length() - remove.length()) >= 0) {
            str = str.substring(0, str.length() - remove.length());
        }
        return str;
    }

    /**
     * 把字符串按照规则分割，比如str为“id=123&name=test”，rule为“id=#&name=#”，分隔后为["123", "test"];
     *
     * @param str
     * @param rule
     * @return
     */
    public static String[] split(String str, String rule) {
        if (rule.indexOf(SEPARATOR_SINGLE) == -1 || rule.indexOf(SEPARATOR_SINGLE + SEPARATOR_SINGLE) != -1) {
            throw new IllegalArgumentException("Could not parse rule");
        }

        String[] rules = rule.split(SEPARATOR_SINGLE);
        // System.out.println(rules.length);

        if (str == null || str.length() < rules[0].length()) {
            return new String[0];
        }

        boolean endsWithSeparator = rule.endsWith(SEPARATOR_SINGLE);

        String[] strs = new String[endsWithSeparator ? rules.length : rules.length - 1];
        if (rules[0].length() > 0 && !str.startsWith(rules[0])) {
            return new String[0];
        }

        int startIndex = 0;
        int endIndex = 0;
        for (int i = 0; i < strs.length; i++) {
            startIndex = str.indexOf(rules[i], endIndex) + rules[i].length();
            if (i + 1 == strs.length && endsWithSeparator) {
                endIndex = str.length();
            }
            else {
                endIndex = str.indexOf(rules[i + 1], startIndex);
            }

            // System.out.println(startIndex + "," + endIndex);

            if (startIndex == -1 || endIndex == -1) {
                return new String[0];
            }
            strs[i] = str.substring(startIndex, endIndex);
        }

        return strs;
    }

    /**
     * 把字符串按照指定的字符集进行编码
     *
     * @param str
     * @param charSetName
     * @return
     */
    public static String toCharSet(String str, String charSetName) {
        try {
            return new String(str.getBytes(), charSetName);
        }
        catch (UnsupportedEncodingException e) {
            return str;
        }
    }

    /**
     * 把一个字节数组转换为16进制表达的字符串
     *
     * @param bytes
     * @return
     */
    public static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            hexString.append(enoughZero(Integer.toHexString(bytes[i] & 0xff), 2));
        }
        return hexString.toString();
    }

    /**
     * 把16进制表达的字符串转换为整数
     *
     * @param hexString
     * @return
     */
    public static int hexString2Int(String hexString) {
        return Integer.valueOf(hexString, 16).intValue();
    }

    /**
     * 清除字符串两边的空格，null不处理
     *
     * @param str
     * @return
     */
    public static String trim(String str) {
        return str == null ? str : str.trim();
    }

    /**
     * <p>
     * Removes control characters (char &lt;= 32) from both ends of this String returning an empty String ("") if the
     * String is empty ("") after the trim or if it is <code>null</code>.
     * </p>
     *
     * @param str
     *            the String to be trimmed, may be null
     * @return the trimmed String, or an empty String if <code>null</code> input
     */
    public static String trimToEmpty(String str) {
        return (str == null ? "" : str.trim());
    }

    /**
     * 清除字符串中的回车和换行符
     *
     * @param str
     * @return
     */
    public static String ignoreEnter(String str) {
        if (str == null || str.length() == 0) {
            return null;
        }

        return str.replaceAll("\r|\n", "");
    }


    /**
     * 获得成对出现的第一个关键字对应的关键字的位置。
     *
     * @param str
     * @param keyword
     *            关键字，例如：select
     * @param oppositeKeyword
     *            对应的关键字，例如：from
     * @return 第一个关键字对应的关键字的位置
     */
//    public static int getFirstPairIndex(String str, String keyword, String oppositeKeyword) {
//        ArrayList<PairKeyword> keywordArray = new ArrayList<PairKeyword>();
//        int index = -1;
//        while ((index = str.indexOf(keyword, index)) != -1) {
//            keywordArray.add(new PairKeyword(keyword, index));
//            index += keyword.length();
//        }
//
//        index = -1;
//        while ((index = str.indexOf(oppositeKeyword, index)) != -1) {
//            keywordArray.add(new PairKeyword(oppositeKeyword, index));
//            index += oppositeKeyword.length();
//        }
//
//        if (keywordArray.size() < 2) {
//            return -1;
//        }
//
//        Collections.sort(keywordArray, new PairKeywordComparator());
//
//        PairKeyword firstKeyword = keywordArray.get(0);
//        if (!firstKeyword.getName().equals(keyword)) {
//            return -1;
//        }
//
//        while (keywordArray.size() > 2) {
//            boolean hasOpposite = false;
//            for (int i = 2; i < keywordArray.size(); i++) {
//                PairKeyword keyword0 = keywordArray.get(i - 1);
//                PairKeyword keyword1 = keywordArray.get(i);
//                if (keyword0.getName().equals(keyword) && keyword1.getName().equals(oppositeKeyword)) {
//                    keywordArray.remove(i);
//                    keywordArray.remove(i - 1);
//                    hasOpposite = true;
//                    break;
//                }
//            }
//            if (!hasOpposite) {
//                return -1;
//            }
//        }
//
//        if (keywordArray.size() != 2) {
//            return -1;
//        }
//
//        PairKeyword lastKeyword = keywordArray.get(1);
//        if (!lastKeyword.getName().equals(oppositeKeyword)) {
//            return -1;
//        }
//
//        return lastKeyword.getIndex();
//    }

    /**
     * 根据分割符对字符串进行分割，每个分割后的字符串将被 <tt>trim</tt> 后放到列表中。
     *
     * @param str
     *            将要被分割的字符串
     * @param delimiter
     *            分隔符
     * @return 分割后的结果列表
     */
    public static final List<String> split(String str, char delimiter) {
        // return no groups if we have an empty string
        if (str == null || "".equals(str)) {
            return Collections.emptyList();
        }

        ArrayList<String> parts = new ArrayList<String>();
        int currentIndex;
        int previousIndex = 0;

        while ((currentIndex = str.indexOf(delimiter, previousIndex)) > 0) {
            String part = str.substring(previousIndex, currentIndex).trim();
            parts.add(part);
            previousIndex = currentIndex + 1;
        }

        parts.add(str.substring(previousIndex, str.length()).trim());

        return parts;
    }

    /**
     * 判断 value 的值是否表示条件为真。例子：
     *
     *
     * @param value
     *            字符串
     * @return 如果 value 等于 “1” 或者 “true”（大小写无关） 返回 <code>true</code>，否则返回 <code>false</code>。
     */
    public static boolean isValueTrue(String value) {
        return BOOLEAN_TRUE_NUMBER.equals(value) || BOOLEAN_TRUE_STRING.equalsIgnoreCase(value);
    }

    /**
     * 判断 value 的值是否表示条件为假。例子：
     *
     * @param value
     *            字符串
     * @return 如果 value 等于 “0” 或者 “false”（大小写无关） 返回 <code>true</code>，否则返回 <code>false</code>。
     */
    public static boolean isValueFalse(String value) {
        return BOOLEAN_FALSE_NUMBER.equals(value) || BOOLEAN_FALSE_STRING.equalsIgnoreCase(value);
    }

    /**
     * 判断两个字符串是否equals
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean equals(String str1, String str2) {
        if (str1 == null || str2 == null) {
            return false;
        }
        return str1.equals(str2);
    }
}
