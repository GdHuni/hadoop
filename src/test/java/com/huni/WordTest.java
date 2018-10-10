package com.huni;

import org.junit.Test;

public class WordTest {
    @Test
    public void wordtest(){
        String a = "b";
        char[] chars = a.toCharArray();
        for (char c :chars){

            byte b = (byte)c;
            System.out.println(b);
        }
    }
}
