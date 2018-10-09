package com.huni.serialize;

import java.io.Serializable;

public class Dog implements Serializable {
    private static final long serialVersionUID = -9196170612788211068L;
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
