package com.guilin.elasticsearch.demo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by guilin1 on 16/3/15.
 * 课程
 */
public class Course {

    private String name;//课程名称

    private double score;//分数

    public Course() {
    }

    public Course(String name, double score) {
        this.name = name;
        this.score = score;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", this.getName());
        map.put("score", this.getScore());
        return map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
