# Flink作业提交流程

### Standalone会话模式

脚本启动执行  =>  

​         				(客户端)  => 解析参数 => 生成逻辑流图 => 生成JobGraph => 封装提交参数

​										=> 提交任务至JobManager

​					（JobManager）=> 启动并提交应用 => 形成执行图ExecutionGraph 

​											=> 请求Slots => TaskManager 提供 Slots

​											=> JobMaster 分发任务 => TaskManager生成物理流图（确定并发任务） 

##### 逻辑流图

​	通过DataStream API编写的最初DAG图，表示程序拓扑结构，在客户端完成。

##### 作业图

​	需要提交给JobManager的是数据结构，确定作业中所有任务划分。

##### 执行图

​	作业图的并行化版本，调度层最核心的数据结构，按照并行子任务进行拆分。

##### 物理图

​	在执行图基础上，进一步确定数据存放位置和收发具体方式。

### Yarn应用模式作业提交流程

​	在Standalone模式基础上，在请求Slots之后，需要向Yarn的ResourceManager申请资源，在NodeManager节点上的TaskManager接收到申请后，向JobMaster注册Slots

