package org.learn;

import java.util.HashMap;

public class Playground {
    public static void main(String[] args) {
        HashMap<String, Integer> wordCount = new HashMap<>();
        wordCount.put("file-1", 10);
        wordCount.put("file-2", 20);

        System.out.println(wordCount);

        wordCount.put("file-2", 25);
        System.out.println(wordCount);
    }
}
