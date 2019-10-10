package com.tosin.lombok;

public class TestPersion {
    public static void main(String[] args) {
        Person person = new Person();
        person.setName("name");
        person.setAge(20);
        person.setAddress("address");
        person = new Person("name",20,"address");
    }
}
