https://blog.csdn.net/kailuan2zhong/article/details/89857121
https://blog.csdn.net/echineselearning/category_8601782.html

TCC框架感知到@compensable 注解后具体是做了哪些事？

1、创建Transaction 对象，并保存至表中，保存成功后将Transaction注册到队列中

2、将调用业务方法，即TCC中的try业务

3、若业务失败则更新Transaction状态为CANCELLING,且调用TCC中的cancel方法回滚业务（支持失败重试）

4、若业务成功则更新Transaction状态为confirming,且调用TCC中的confirm方法确认业务（支持失败重试）

5、调用结束将当前Transaction从队列中删除