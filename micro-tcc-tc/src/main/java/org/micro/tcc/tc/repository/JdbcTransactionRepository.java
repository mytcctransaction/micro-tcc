package org.micro.tcc.tc.repository;



import org.apache.commons.lang3.StringUtils;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.core.TransactionRepository;
import org.micro.tcc.common.core.TransactionGid;
import org.micro.tcc.common.exception.TransactionIOStreamException;
import org.micro.tcc.common.serializer.KryoPoolSerializer;
import org.micro.tcc.common.serializer.ObjectSerializer;

import javax.sql.DataSource;
import javax.transaction.xa.Xid;
import java.sql.*;
import java.util.Date;
import java.util.List;

/**
 *@author jeff.liu
 *@desc   描述
 *@date 2019/7/31
 */
public class JdbcTransactionRepository implements TransactionRepository {

    private String domain;

    private String tbSuffix;

    private DataSource dataSource;

    private ObjectSerializer serializer = new KryoPoolSerializer();

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTbSuffix() {
        return tbSuffix;
    }

    public void setTbSuffix(String tbSuffix) {
        this.tbSuffix = tbSuffix;
    }

    public void setSerializer(ObjectSerializer serializer) {
        this.serializer = serializer;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    protected int doCreate(Transaction transaction) {

       return 1;
    }

    protected int doUpdate(Transaction transaction) {
       return 1;
    }

    protected int doDelete(Transaction transaction) {
        return 1;
    }

    protected Transaction doFindOne(Xid xid) {


        return null;
    }


    protected List<Transaction> doFindAllUnmodifiedSince(java.util.Date date) {
        return null;

    }

    protected List<Transaction> doFind(List<Xid> xids) {
        return null;
    }

    protected void constructTransactions(ResultSet resultSet, List<Transaction> transactions) throws SQLException {
        while (resultSet.next()) {
            byte[] transactionBytes = resultSet.getBytes(3);
            Transaction transaction = (Transaction) serializer.deserialize(transactionBytes);
            transaction.changeStatus(TransactionStatus.valueOf(resultSet.getInt(4)));
            transaction.setLastUpdateTime(resultSet.getDate(7));
            transaction.setVersion(resultSet.getLong(9));
            transaction.resetRetriedCount(resultSet.getInt(8));
            transactions.add(transaction);
        }
    }


    protected Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new TransactionIOStreamException(e);
        }
    }

    protected void releaseConnection(Connection con) {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            throw new TransactionIOStreamException(e);
        }
    }

    private void closeStatement(Statement stmt) {
        try {
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
            }
        } catch (Exception ex) {
            throw new TransactionIOStreamException(ex);
        }
    }

    private String getTableName() {
        return StringUtils.isNotEmpty(tbSuffix) ? "TCC_TRANSACTION" + tbSuffix : "TCC_TRANSACTION";
    }

    @Override
    public int create(Transaction transaction) {
        return 0;
    }

    @Override
    public int update(Transaction transaction) {
        return 0;
    }

    @Override
    public int delete(Transaction transaction) {
        return 0;
    }

    @Override
    public Transaction findByGroupId(TransactionGid xid) {
        return null;
    }

    @Override
    public Transaction findByGroupId(String gid) {
        return null;
    }

    @Override
    public List<Transaction> findAll(Date date) {
        return null;
    }
}
