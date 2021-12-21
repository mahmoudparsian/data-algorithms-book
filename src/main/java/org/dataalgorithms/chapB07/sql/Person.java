package org.dataalgorithms.chapB07.sql;

import java.io.Serializable;

/**
 * Basic example of using SQL in Spark
 *
 * @author Mahmoud Parsian
 *
 */
public class Person implements Serializable {

    private String name;
    private int age;
    private String country;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public static Person buildPerson(String record) {
        // record has the following format:
        // <name><,><age><,><country>
        String[] tokens = record.split(",");
        // tokens[0] = name
        // tokens[1] = age
        // tokens[2] = country
        Person person = new Person();
        person.setName(tokens[0].trim());
        person.setAge(Integer.parseInt(tokens[1].trim()));
        person.setCountry(tokens[2].trim());
        System.out.println(person.toString());
        return person;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Person[name=");
        builder.append(name);
        builder.append(", age=");
        builder.append(age);
        builder.append(", country=");
        builder.append(country);
        builder.append("]");
        return builder.toString();
    }
}
