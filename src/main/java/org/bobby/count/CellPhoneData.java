package org.bobby.count;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CellPhoneData implements Writable {
    private long total;
    private long up;
    private long down;

    public CellPhoneData() {
    }

    public CellPhoneData setUp(long up) {
        this.up = up;
        return this;
    }

    public CellPhoneData setDown(long down) {
        this.down = down;
        return this;
    }

    public void build() {
        this.total = up + down;
    }

    public long getTotal() {
        return total;
    }

    public long getUp() {
        return up;
    }

    public long getDown() {
        return down;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(total);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.total = dataInput.readLong();
    }

    @Override
    public String toString() {
        return up + " " + down + " " + total;
    }
}
