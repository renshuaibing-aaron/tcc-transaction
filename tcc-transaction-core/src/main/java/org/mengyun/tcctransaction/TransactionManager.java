package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

/**
 * Created by changmingxie on 10/26/15.
 */
public class TransactionManager {

    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());

    private TransactionRepository transactionRepository;
    /**
     * 当前线程事务队列
     */
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    private ExecutorService executorService;

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TransactionManager() {


    }

    public Transaction begin(Object uniqueIdentify) {
        System.out.println("----发起根事务----");
        // 创建 根事务
        Transaction transaction = new Transaction(uniqueIdentify,TransactionType.ROOT);
        // 存储 事务
        transactionRepository.create(transaction);
        // 注册 事务
        registerTransaction(transaction);
        return transaction;
    }

    public Transaction begin() {
        Transaction transaction = new Transaction(TransactionType.ROOT);
        transactionRepository.create(transaction);
        registerTransaction(transaction);
        return transaction;
    }
    /**
     * 传播发起分支事务
     *
     * @param transactionContext 事务上下文
     * @return 分支事务
     */
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
        System.out.println("-----传播发起分支事务-----");
        // 创建 分支事务
        Transaction transaction = new Transaction(transactionContext);
        // 存储 事务
        transactionRepository.create(transaction);
        // 注册 事务
        registerTransaction(transaction);
        return transaction;
    }
    /**
     * 传播获取分支事务
     *
     * @param transactionContext 事务上下文
     * @return 分支事务
     * @throws NoExistedTransactionException 当事务不存在时
     */
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }
    /**
     * todo  其实这些个地方都可以看到
     * TCC中的本质是使用切面 利用切面获取到tcc中的try方法 commit 和cancle方法
     *  在提交出现异常的情况下进行 利用反射调用方法 进行事务的提交和回滚
     * 提交事务
     */
    public void commit(boolean asyncCommit) {
        // 获取 事务
        final Transaction transaction = getCurrentTransaction();
        // 设置 事务状态 为 CONFIRMING
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        // 更新 事务
        transactionRepository.update(transaction);

        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();

                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction);
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("compensable transaction async submit confirm failed, recovery job will try to confirm later.", commitException);
                throw new ConfirmingException(commitException);
            }
        } else {
            commitTransaction(transaction);
        }
    }


    public void rollback(boolean asyncRollback) {

        final Transaction transaction = getCurrentTransaction();
        //update 表中transaction的状态为CANCELLING
        transaction.changeStatus(TransactionStatus.CANCELLING);

        transactionRepository.update(transaction);

        if (asyncRollback) {

            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                logger.warn("compensable transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancellingException(rollbackException);
            }
        } else {

            //注意看这个地方是怎么回滚的
            //2.32中rollback 通过反射去调用TCC中的cancel方法回滚业务事务（1、ConfigurableCoordinatorAspect AOP切面中会分析如何拿到TCC中cancel方法名以及目标类的。2、看到这里有人会问update transaction的为CANCELLING和调TCC中的cancel方法不在同一个事务里，
            // transaction数据更新成功了，TCC cancel失败了怎么办呢？？其实这个问题也将在下一个TCC补偿里讲解
            rollbackTransaction(transaction);
        }
    }


    private void commitTransaction(Transaction transaction) {
        try {
            // 提交 事务
            transaction.commit();
            // 删除 事务
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {
            logger.warn("compensable transaction confirm failed, recovery job will try to confirm later.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    private void rollbackTransaction(Transaction transaction) {
        try {
            transaction.rollback();
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {
            logger.warn("compensable transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }

    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek();
        }
        return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }


    /**
     * 注册事务到当前线程事务队列
     *
     * @param transaction 事务
     */
    private void registerTransaction(Transaction transaction) {
        System.out.println("==========将Transaction对象注册到队列中============");

        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }

        CURRENT.get().push(transaction);
    }

    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
                if (CURRENT.get().size() == 0) {
                    CURRENT.remove();
                }
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }

    public void enlistParticipant(Participant participant) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.enlistParticipant(participant);
        transactionRepository.update(transaction);
    }
}
