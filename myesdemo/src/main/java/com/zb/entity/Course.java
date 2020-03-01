package com.zb.entity;
//添加1处
public class Course {

    //2处
    private String name;
    private String studymodel;
    private String description;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStudymodel() {
        return studymodel;
    }

    public void setStudymodel(String studymodel) {
        this.studymodel = studymodel;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "Course{" +
                "name='" + name + '\'' +
                ", studymodel='" + studymodel + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
