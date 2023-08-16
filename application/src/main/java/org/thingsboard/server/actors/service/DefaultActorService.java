/**
 * Copyright © 2016-2023 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.actors.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.actors.DefaultTbActorSystem;
import org.thingsboard.server.actors.TbActorRef;
import org.thingsboard.server.actors.TbActorSystem;
import org.thingsboard.server.actors.TbActorSystemSettings;
import org.thingsboard.server.actors.app.AppActor;
import org.thingsboard.server.actors.app.AppInitMsg;
import org.thingsboard.server.actors.stats.StatsActor;
import org.thingsboard.server.common.msg.queue.PartitionChangeMsg;
import org.thingsboard.server.queue.discovery.TbApplicationEventListener;
import org.thingsboard.server.queue.discovery.event.PartitionChangeEvent;
import org.thingsboard.server.queue.util.AfterStartUp;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class DefaultActorService extends TbApplicationEventListener<PartitionChangeEvent> implements ActorService {

    public static final String APP_DISPATCHER_NAME = "app-dispatcher";
    public static final String TENANT_DISPATCHER_NAME = "tenant-dispatcher";
    public static final String DEVICE_DISPATCHER_NAME = "device-dispatcher";
    public static final String RULE_DISPATCHER_NAME = "rule-dispatcher";

    @Autowired
    private ActorSystemContext actorContext;

    private TbActorSystem system;

    private TbActorRef appActor;

    @Value("${actors.system.throughput:5}")
    private int actorThroughput;

    @Value("${actors.system.max_actor_init_attempts:10}")
    private int maxActorInitAttempts;

    @Value("${actors.system.scheduler_pool_size:1}")
    private int schedulerPoolSize;

    @Value("${actors.system.app_dispatcher_pool_size:1}")
    private int appDispatcherSize;

    @Value("${actors.system.tenant_dispatcher_pool_size:2}")
    private int tenantDispatcherSize;

    @Value("${actors.system.device_dispatcher_pool_size:4}")
    private int deviceDispatcherSize;

    @Value("${actors.system.rule_dispatcher_pool_size:4}")
    private int ruleDispatcherSize;

    /**第一阶段
     * 项目启动加载所有的actor
     */
    @PostConstruct
    public void initActorSystem() {
        log.info("Initializing actor system.");
        log.info("开始初始化actor系统相关的");
        actorContext.setActorService(this);
        //actory配置类，设置吞吐量、池子大小及最大数量
        TbActorSystemSettings settings = new TbActorSystemSettings(actorThroughput, schedulerPoolSize, maxActorInitAttempts);
        system = new DefaultTbActorSystem(settings);

        //创建不同类型的dispatcher,并使用线程池用于后续异步处理消息
        system.createDispatcher(APP_DISPATCHER_NAME, initDispatcherExecutor(APP_DISPATCHER_NAME, appDispatcherSize));
        system.createDispatcher(TENANT_DISPATCHER_NAME, initDispatcherExecutor(TENANT_DISPATCHER_NAME, tenantDispatcherSize));
        system.createDispatcher(DEVICE_DISPATCHER_NAME, initDispatcherExecutor(DEVICE_DISPATCHER_NAME, deviceDispatcherSize));
        system.createDispatcher(RULE_DISPATCHER_NAME, initDispatcherExecutor(RULE_DISPATCHER_NAME, ruleDispatcherSize));

        //将actors设置到上下文对象中
        actorContext.setActorSystem(system);

        //创建整个Actor模型的根
        appActor = system.createRootActor(APP_DISPATCHER_NAME, new AppActor.ActorCreator(actorContext));
        actorContext.setAppActor(appActor);
        //创建状态Actor，也是一个根，用于统计状态
        TbActorRef statsActor = system.createRootActor(TENANT_DISPATCHER_NAME, new StatsActor.ActorCreator(actorContext, "StatsActor"));
        actorContext.setStatsActor(statsActor);
        //打印日志输出actor系统初始化缓存
        log.info("Actor system initialized.");
        //启动成功之后执行第二阶段onApplicationEvent()
        log.info("actor系统初始化第一阶段完成。即将执行第二阶段");

    }

    private ExecutorService initDispatcherExecutor(String dispatcherName, int poolSize) {
        if (poolSize == 0) {
            int cores = Runtime.getRuntime().availableProcessors();
            poolSize = Math.max(1, cores / 2);
        }
        if (poolSize == 1) {
            return Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName(dispatcherName));
        } else {
            return ThingsBoardExecutors.newWorkStealingPool(poolSize, dispatcherName);
        }
    }


    /**
     *第二阶段
     * @param applicationReadyEvent
     */
    @AfterStartUp(order = AfterStartUp.ACTOR_SYSTEM)
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Received application ready event. Sending application init message to actor system");
        log.info("actor系统执行第二阶段=>给顶层AppActor邮箱发送消息AppInitMsg");
        //第二阶段：给顶层AppActor邮箱发送消息AppInitMsg
        appActor.tellWithHighPriority(new AppInitMsg());
    }

    @Override
    protected void onTbApplicationEvent(PartitionChangeEvent event) {
        log.info("Received partition change event.");
        this.appActor.tellWithHighPriority(new PartitionChangeMsg(event.getQueueKey().getType(), event.getPartitions()));
    }

    @PreDestroy
    public void stopActorSystem() {
        if (system != null) {
            log.info("Stopping actor system.");
            system.stop();
            log.info("Actor system stopped.");
        }
    }

}
