package com.axon.icloud.DistributeCache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class weixinBean implements Writable {
    private int tag;
    private int fre;
    private String date;
    private String tel;
    private String biz;
    public weixinBean(){}
	
	public void write(DataOutput out) throws IOException {
		     out.writeInt(tag);
		     out.writeInt(fre);
		     out.writeUTF(date);
		     out.writeUTF(tel);
		     out.writeUTF(biz);
	}

	public void readFields(DataInput in) throws IOException {
		this.tag = in.readInt();
		this.fre = in.readInt();
		this.date = in.readUTF();
		this.tel = in.readUTF();
		this.biz = in.readUTF();
	}

	public String getBiz() {
		return biz;
	}

	public void setBiz(String biz) {
		this.biz = biz;
	}

	public String getTel() {
		return tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public int getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = tag;
	}

	public int getFre() {
		return fre;
	}

	public void setFre(int fre) {
		this.fre = fre;
	}

	@Override
	public String toString() {
		return "weixinBean [tag=" + tag + ", fre=" + fre + ", date=" + date
				+ ", tel=" + tel + ", biz=" + biz + "]";
	}

}
