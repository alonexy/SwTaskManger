## SwTaskManger

使用symfony/console + Swoole Process Pool 一个简单的多进程任务管理



```
php bin/Console.php list
```

### 多消费者模式如何保证消息顺序执行
- http://www.alonexy.com/AMQP/message_list.html

### FORRX DEMO
```
定时插入 控制速度 启动服务
redis >> LPUSH  LZ_OHLC_JOB_LIST  XAUUSD
```