package com.hadoop;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jizhe.pan on 2019-03-22
 */
public class Client {

    private static Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws IOException, YarnException {
        // 创建并初始化yarnClient对象
        YarnClient yarnClient = YarnClient.createYarnClient();
//        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        // 获取创建应用成功与否的相应信息
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // 应用的入口
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        // 应用的id
        ApplicationId appId = appContext.getApplicationId();
//        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
//        appContext.setApplicationName(appName);

        // 设置ApplicationMaster的本地资源
        Map<String, LocalResource> localResources = new HashMap<>();
        LOGGER.info("Copy App Master jar from local filesystem and add to local environment");


    }

}
