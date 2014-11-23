package com.neu.cs6240;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Post implements the Writable
 */


public class Comment implements Writable {
	// Comment Id
	Text id;
	// PostId 
	Text postId;
	// The Score of the comment
	Text score;
	// Creation Date for this comment
	Text creationDate;
	// User id who owns this Comment
	Text userId;
	// Text in the comment
	Text text;


	/**
	 * constructor
	 */
	public Comment(Document doc) {
		NodeList nList = doc.getElementsByTagName("row");
		Element eElement = (Element) nList.item(0);
		this.id = new Text(eElement.getAttribute("Id"));
		this.postId = new Text(eElement.getAttribute("PostId"));
		this.creationDate = new Text(eElement.getAttribute("CreationDate"));
		this.score = new Text(eElement.getAttribute("Score"));
		this.userId = new Text(eElement.getAttribute("UserId"));
		this.text = new Text(sanitize(eElement.getAttribute("Text")));
	}
	
	/**
	 * @return the id
	 */
	public Text getId() {
		return id;
	}

	/**
	 * overrider the write method to support write operation
	 */
	public void write(DataOutput out) throws IOException {
		this.id.write(out);
		this.postId.write(out);
		this.creationDate.write(out);
		this.score.write(out);
		this.userId.write(out);
		this.text.write(out);
	}

	/**
	 * overrider readFiled method to support reading fields
	 */
	public void readFields(DataInput in) throws IOException {
		this.id.readFields(in);
		this.postId.readFields(in);
		this.creationDate.readFields(in);
		this.score.readFields(in);
		this.userId.readFields(in);
		this.text.readFields(in);
	}
	
	private String sanitize(String data){
		if(data == null || data.isEmpty()){
			return data;
		}
		return data.replaceAll("(\\r|\\n|\\r\\n|,|\"|')+", "");
	}
	
	public String toCsv(){
		StringBuilder output = new StringBuilder();
		
		output.append(this.id.toString()).append(",");
		output.append(this.postId.toString()).append(",");
		output.append(this.creationDate.toString()).append(",");
		output.append(this.score.toString()).append(",");
		output.append(this.userId.toString()).append(",");		
		output.append(this.text.toString());
		
		return output.toString();
	}
}