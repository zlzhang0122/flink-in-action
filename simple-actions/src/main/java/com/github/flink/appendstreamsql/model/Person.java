package com.github.flink.appendstreamsql.model;

/**
 * @Author: zlzhang0122
 * @Date: 2019/11/5 7:11 PM
 */
public class Person {
    public Person() {

    }

    public Person(String name, String gender) {
        this.name = name;
        this.gender = gender;
    }

    private String name;

    private String gender;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
}
