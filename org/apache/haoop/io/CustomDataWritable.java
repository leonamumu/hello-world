package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomDataWritable implements Writable {
	
	private int upPackNum;
	private int downPackNum;
	private int upPayLoad;
	private int downPayLoad;
	private int countNum;
	

	public int getCountNum() {
		return countNum;
	}

	public void setCountNum(int countNum) {
		this.countNum = countNum;
	}

	public int getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(int upPackNum) {
		this.upPackNum = upPackNum;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(int downPackNum) {
		this.downPackNum = downPackNum;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(int upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(int downPayLoad) {
		this.downPayLoad = downPayLoad;
	}
	
	/*
	 * 序列化对象的字段到 out
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.upPackNum);
		out.writeInt(this.downPackNum);
		out.writeInt(this.upPayLoad);
		out.writeInt(this.downPayLoad);
		out.writeInt(this.countNum);
	}
	
	/*
	 * 从in反序列化对象的字段
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.setUpPackNum(in.readInt());
		this.setDownPackNum(in.readInt());
		this.setUpPayLoad(in.readInt());
		this.setDownPayLoad(in.readInt());
		this.setCountNum(in.readInt());
	}
	
	//最终输出CustomDataWritable类型对象的格式。
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad + "\t" + countNum;
	}
	
	//判断两个CustomDataWritable类型的对象是否相同
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		boolean flag = true;
		if (!(obj instanceof CustomDataWritable))
		{
			return false;
		}
		else
		{
			CustomDataWritable other = (CustomDataWritable) obj;
			if (other.upPackNum != this.upPackNum) flag = false;
			if (other.downPackNum != this.downPackNum) flag = false;
			if (other.upPayLoad != this.upPayLoad) flag = false;
			if (other.downPayLoad != this.downPayLoad) flag = false;
			if (other.countNum != this.countNum) flag = false;
		}
		
		return flag;
	}
	
	


}
