package com.shz.hadoop.hdfs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.Random;

public class GenerateFile {
    public static void main(String[] args) throws Exception {
        /**
         * 生成文本文件，每行文本内容格式为：{i} Hello {language}，其中i为循环变量值，language的值从数组中随机获取一个
         */
        FileOutputStream fos = new FileOutputStream("test.txt");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        String[] languages = new String[]{"c#", "java", "python", "go", "c", "c++"};
        Random random = new Random();
        for (int i = 1; i <= 100000; i++) {
            String str = i + " Hello " + languages[random.nextInt(languages.length)] + System.lineSeparator();
            bos.write(str.getBytes());
        }
        bos.flush();
    }
}

