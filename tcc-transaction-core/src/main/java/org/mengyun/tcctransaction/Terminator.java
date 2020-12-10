package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.StringUtils;

import java.lang.reflect.Method;

/**
 * Created by changmingxie on 10/30/15.
 */
public final class Terminator {

    public Terminator() {

    }

    public static Object invoke(TransactionContext transactionContext, InvocationContext invocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {


        if (StringUtils.isNotEmpty(invocationContext.getMethodName())) {

            try {

                // 获得 参与者对象
                Object target = FactoryBuilder.factoryOf(invocationContext.getTargetClass()).getInstance();
                // 获得 方法
                Method method = null;

                method = target.getClass().getMethod(invocationContext.getMethodName(), invocationContext.getParameterTypes());
                // 设置 事务上下文 到方法参数
                FactoryBuilder.factoryOf(transactionContextEditorClass).getInstance().set(transactionContext, target, method, invocationContext.getArgs());
                // 执行方法
                return method.invoke(target, invocationContext.getArgs());

            } catch (Exception e) {
                throw new SystemException(e);
            }
        }
        return null;
    }
}
