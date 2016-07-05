package com.guilin.elasticsearch.demo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.DateTime;

import java.util.*;

/**
 * Created by guilin1 on 16/3/15.
 */
public class Student {

    private String stuNo;//学号

    private String name;//名字

    private int height;//身高

    private double weight;//体重

    private Date intendedTime;//入学时间

    private long graduationTime;//毕业时间

    private boolean isMale = true;//是否是男性

    private List<Course> scores;//分数

    private List<String> friends;

    private List<Double> scores2;

    private long d_long;

    private String d_date;

    private String d_datetime;

    public Student() {
    }

    public Student(String stuNo, String name, int height, double weight, Date intendedTime, long graduationTime, boolean isMale, List<Course> scores) {
        this.stuNo = stuNo;
        this.name = name;
        this.height = height;
        this.weight = weight;
        this.intendedTime = intendedTime;
        this.graduationTime = graduationTime;
        this.isMale = isMale;
        this.scores = scores;
    }

    public Student(String stuNo, String name, int height, double weight, Date intendedTime, long graduationTime, boolean isMale, List<Course> scores, List<String> friends) {
        this.stuNo = stuNo;
        this.name = name;
        this.height = height;
        this.weight = weight;
        this.intendedTime = intendedTime;
        this.graduationTime = graduationTime;
        this.isMale = isMale;
        this.scores = scores;
        this.friends = friends;
    }

    public Student(String stuNo, String name, int height, double weight, Date intendedTime, long graduationTime, boolean isMale, List<Course> scores, List<String> friends, List<Double> scores2, long d_long, String d_date, String d_datetime) {
        this.stuNo = stuNo;
        this.name = name;
        this.height = height;
        this.weight = weight;
        this.intendedTime = intendedTime;
        this.graduationTime = graduationTime;
        this.isMale = isMale;
        this.scores = scores;
        this.friends = friends;
        this.scores2 = scores2;
        this.d_long = d_long;
        this.d_date = d_date;
        this.d_datetime = d_datetime;
    }

    public Map<String,Object> toMap(){
        Map<String,Object> map = new HashMap<>();
        map.put("stuNo",this.getStuNo());
        map.put("name",this.getName());
        map.put("height",this.getHeight());
        map.put("weight",this.getWeight());
        map.put("intendedTime",this.getIntendedTime().toString());
        map.put("graduationTime",this.getGraduationTime());
        map.put("isMale",this.isMale());
        map.put("d_long",this.getD_long());
        map.put("d_date", this.getD_date());
        map.put("d_datetime",this.getD_datetime());
        if(CollectionUtils.isNotEmpty(this.getScores())){
            List<Map<String,Object>> scoreList = new ArrayList<>();
            List<Double> scoreList2 = new ArrayList<>();
            for(Course course:scores){
                scoreList.add(course.toMap());
                scoreList2.add(course.getScore());
            }
            map.put("scores",scoreList);
            map.put("scores2",scoreList2);
        }
        if(CollectionUtils.isNotEmpty(this.getFriends())){
            map.put("friends", this.getFriends());
        }
        
//        for(int i=0; i<10000; i++){
//            map.put("field"+i, i);
//        }
        return map;
    }

    public long getD_long() {
        return d_long;
    }

    public void setD_long(long d_long) {
        this.d_long = d_long;
    }

    public String getD_date() {
        return d_date;
    }

    public void setD_date(String d_date) {
        this.d_date = d_date;
    }

    public String getD_datetime() {
        return d_datetime;
    }

    public void setD_datetime(String d_datetime) {
        this.d_datetime = d_datetime;
    }

    public List<Double> getScores2() {
        return scores2;
    }

    public void setScores2(List<Double> scores2) {
        this.scores2 = scores2;
    }

    public List<String> getFriends() {
        return friends;
    }

    public void setFriends(List<String> friends) {
        this.friends = friends;
    }

    public String getStuNo() {
        return stuNo;
    }

    public void setStuNo(String stuNo) {
        this.stuNo = stuNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Date getIntendedTime() {
        return intendedTime;
    }

    public void setIntendedTime(Date intendedTime) {
        this.intendedTime = intendedTime;
    }

    public long getGraduationTime() {
        return graduationTime;
    }

    public void setGraduationTime(long graduationTime) {
        this.graduationTime = graduationTime;
    }

    public boolean isMale() {
        return isMale;
    }

    public void setIsMale(boolean isMale) {
        this.isMale = isMale;
    }

    public List<Course> getScores() {
        return scores;
    }

    public void setScores(List<Course> scores) {
        this.scores = scores;
    }
}
