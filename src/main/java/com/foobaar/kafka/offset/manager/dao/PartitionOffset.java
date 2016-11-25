package com.foobaar.kafka.offset.manager.dao;

public class PartitionOffset {
  private int partition;
  private long offset;

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }
}
