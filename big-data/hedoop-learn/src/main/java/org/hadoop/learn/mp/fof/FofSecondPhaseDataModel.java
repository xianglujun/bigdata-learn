package org.hadoop.learn.mp.fof;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class FofSecondPhaseDataModel implements WritableComparable<FofSecondPhaseDataModel> {

    private String userId;
    private String friendId;
    private Integer cnt;

    @Override
    public int compareTo(FofSecondPhaseDataModel o) {
        return o.getCnt().compareTo(this.cnt);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.userId);
        out.writeUTF(this.friendId);
        out.writeInt(this.cnt);
    }

    @Override
    public int hashCode() {
        return this.userId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof FofSecondPhaseDataModel)) {
            return false;
        }

        FofSecondPhaseDataModel other = (FofSecondPhaseDataModel) obj;
        return this.userId.equals(other.getUserId())
                && this.friendId.equals(other.getFriendId())
                && this.cnt.equals(other.getCnt());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.friendId = in.readUTF();
        this.cnt = in.readInt();
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FofSecondPhaseDataModel.class);
        }

        @Override
        public int compare(Object a, Object b) {
            FofSecondPhaseDataModel w1 = (FofSecondPhaseDataModel) a;
            FofSecondPhaseDataModel w2 = (FofSecondPhaseDataModel) b;
            return w1.getUserId().compareTo(w2.getUserId());
        }

        static {
            WritableComparator.define(FofSecondPhaseDataModel.class, new Comparator());
        }
    }
}
