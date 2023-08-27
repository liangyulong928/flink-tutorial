### docker搭建Flink集群

```shell
docker network create flink-network
docker run --rm --name=jobmanager --network flink-network --publish 8081:8081 --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" b394e70 jobmanager
docker run --rm --name=taskmanager --network flink-network --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" b394e70 taskmanager
```

### 部署模式

| 部署模式   | 模式                                                         | 适合项目                         |
| ---------- | ------------------------------------------------------------ | -------------------------------- |
| 会话模式   | 启动一个集群，保持一个会话<br />客户端提交作业<br />所有提交的作业会竞争集群中的资源 | 单个规模小、执行时间短的大量作业 |
| 单作业模式 | 客户端每提交作业启动一个集群<br />作业完成后，集群关闭，资源释放<br />借助管理资源框架启动集群 | 单作业生产环境稳定               |
| 应用模式   | 每一个应用启动一个JobManager<br />无需客户端，之行结束JM关闭 | 通过客户端提交作业会大量占用带宽 |

### Yarn运行模式

部署过程：客户端把Flink应用提交给Yarn的ResourceManager，RM向Yarn的NodeManager申请容器。

​									=> 容器上部署JobManager和TaskManager的实例

​									=> Flink根据JobManager上作业所需要Slot数量动态分配TaskManager

### Flink运行时架构

##### JobManager：作业管理器

是Flink集群中任务管理和调度的核心，是控制应用执行的主进程。每个应用被唯一的JobManager所控制执行。

组件：

- JobMaster ： 负责处理单独的作业，JobMaster与Job一一对应，多个Job可以同时运行在一个Flink集群中。
  - JobMaster会将JobGraph转换成物理层面的数据流图，分析所有可以并发执行的任务，向资源管理器发出请求，申请执行任务必要的资源。得到足够资源后，将执行图分发运行他们的TaskManager。
- ResourceManager ： 负责资源的分配和管理。Flink集群中只有一个。
  - 每一个任务都需要分配到一个slot上执行。
  - slot是Flink集群中资源调配单元，包含了机器用来执行计算的一组CPU和内存资源。
- Dispatcher：分发器。提供一个REST接口，用来提交应用，对每一个提交的作业启动一个新的JobMaster组件。

##### TaskManager：任务管理器

TaskManager包含了一定数量的Slots，Slot是资源调度的最小单位，Slot数量限制了TaskManager能够并行处理的任务数量。

- 启动之后，TaskManager向RM注册Slots，收到资源管理器指令后，TaskManager将一个或多个Slots提供给JobMaster使用。

# Flink核心概念

### 并行度

当处理数据量很大时，可以把一个算子操作复制多份导多个节点，数据来了后就可以到其中任意一个执行。（一个算子就被拆分成了多个并行子任务，再分发到不同节点，实现并行运算）。

#### 并行度的设置

单个算子并行度设置

```java
stream.map(word->Tuple2.of(word,1L)).setParallelism(2);
```

对执行环境设置并行度（全局设定）

```java
env.setParallelism(2);
```

[^keyBy]: 不是算子，无法对keyBy设置并行度

#### 提交应用时设置

```shell
flink run -p 2 -c ac.sict.reid.leo.WordCount.SocketWordCount ./flink-tutorial-1.0-SNAPSHOT.jar
```

#### 配置文件中设置

```yaml
parallelism.default:2						//所在文件：flink-conf.yaml
```

无配置文件情况下默认并行度是当前设备CPU核心个数

### 算子链

##### 算子间的数据传输

- 一对一：不同算子之间不需要重新分区，也不需要调整数据顺序
- 重分区：类似于Spark中的shuffle，根据数据传输策略，发送到不同下游目标任务。

##### 合并算子链

并行度相同的一对一算子操作可以直接链接在一起形成一个大的任务。

### 任务槽（Slots）

Flink中每一个TaskManager都是一个JVM进程，它可以启动多个独立的线程执行多个子任务。任务槽是为了控制并发量，在TaskManager上每个任务运行所占用资源的划分。

##### 任务槽数量设置

```yaml
taskmanager.numberOfTaskSlots:8							//所在文件:flink-conf.yaml
																						//默认是1个
```

##### 任务对任务槽的共享

只要属于同一作业，对不同任务节点（算子）的并行子任务，就可以放到同一个slot上执行。同一算子的并行子任务必须放在不同slot上。

##### 任务槽和并行度的关系

任务槽是静态的概念，程序的并行度与任务槽有关。整个流处理程序的并行度是所有算子并行度最大的那个，这代表了应用程序需要的slot数量。