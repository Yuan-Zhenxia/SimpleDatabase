package simpleDatabase.tx;

import simpleDatabase.basic.Permissions;

import java.security.acl.Permission;

/**
 *
 * finished on 17 Aug. 2021
 *
 * 记录这个事务需要什么等级的权限
 */
public class LockState {

    private TransactionId tid;

    private Permissions permissions;

    public LockState(TransactionId tid, Permissions permissions) {
        this.tid = tid;
        this.permissions = permissions;
    }

    public TransactionId getTid() {return tid;}

    public Permissions getPermissions() {return permissions;};

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LockState lockState = (LockState) o;

        return tid.equals(lockState.tid) && permissions.equals(lockState.permissions);
    }

    @Override
    public int hashCode() {
        int result = tid.hashCode();
        result = 31 * result + permissions.hashCode();
        return result;
    }
}
