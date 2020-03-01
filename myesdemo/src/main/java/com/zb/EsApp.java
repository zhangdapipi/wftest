package com.zb;

import com.zb.entity.Course;
import com.zb.service.CourseService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@SpringBootApplication
public class EsApp {
    //已完成
    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext run = SpringApplication.run(EsApp.class, args);
        CourseService bean = run.getBean(CourseService.class);
        List<Course> curseByPage = bean.findCurseByPage(1, 2, "spring", null, null, null);
        for (Course course : curseByPage) {
            System.out.println(course);
        }
    }

}
