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
public class Post implements Writable {
	// Post Id
	Text id;
	// PostTypeId; Question = 1, Answer = 2
	Text postTypeId;
	// Ans Id that has been Accepted as correct answer
	Text acceptedAnswerId;
	// Creation Date for this post whether its a question / answer
	Text creationDate;
	// User id who owns this Post
	Text ownerUserId;
	// Name of the user who last edited this post
	Text lastEditorDisplayName;
	// Date on which user who last edited this post
	Text lastEditDate;
	// last activity date on this post
	Text lastActivityDate;
	// All hash tags associated with this Post
	Text tags;
	// Number of answers given to this post if question
	Text answerCount;
	// CommentCount
	Text commentCount;
	// FavoriteCount
	Text favoriteCount;
	// CommunityOwnedDate
	Text communityOwnedDate;
	// title of this post is question
	Text title;
	// Score of this post
	Text score;
	// count as views of this posts
	Text viewCount;
	// Content of this post, sanitized
	Text body;

	/**
	 * constructor
	 */
	public Post(Document doc) {
		NodeList nList = doc.getElementsByTagName("row");
		Element eElement = (Element) nList.item(0);
		this.id = new Text(eElement.getAttribute("Id"));
		this.postTypeId = new Text(eElement.getAttribute("PostTypeId"));
		this.acceptedAnswerId = new Text(eElement.getAttribute("AcceptedAnswerId"));
		this.creationDate = new Text(eElement.getAttribute("CreationDate"));
		this.score = new Text(eElement.getAttribute("Score"));
		this.viewCount = new Text(eElement.getAttribute("ViewCount"));
		this.ownerUserId = new Text(eElement.getAttribute("OwnerUserId"));
		this.lastEditorDisplayName = new Text(eElement.getAttribute("LastEditorDisplayName"));
		this.lastEditDate = new Text(eElement.getAttribute("LastEditDate"));
		this.lastActivityDate = new Text(eElement.getAttribute("LastActivityDate"));
		this.tags = new Text(eElement.getAttribute("Tags"));
		this.answerCount = new Text(eElement.getAttribute("AnswerCount"));
		this.commentCount = new Text(eElement.getAttribute("CommentCount"));
		this.favoriteCount = new Text(eElement.getAttribute("FavoriteCount"));
		this.communityOwnedDate = new Text(eElement.getAttribute("CommunityOwnedDate"));
		this.title = new Text(sanitize(eElement.getAttribute("Title")));
		this.body = new Text(sanitize(eElement.getAttribute("Body")));
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
		this.postTypeId.write(out);
		this.acceptedAnswerId.write(out);
		this.creationDate.write(out);
		this.score.write(out);
		this.viewCount.write(out);
		this.ownerUserId.write(out);
		this.lastEditorDisplayName.write(out);
		this.lastEditDate.write(out);
		this.lastActivityDate.write(out);
		this.tags.write(out);
		this.answerCount.write(out);
		this.commentCount.write(out);
		this.favoriteCount.write(out);
		this.communityOwnedDate.write(out);
		this.title.write(out);
		this.body.write(out);
	}

	/**
	 * overrider readFiled method to support reading fields
	 */
	public void readFields(DataInput in) throws IOException {
		this.id.readFields(in);
		this.postTypeId.readFields(in);
		this.acceptedAnswerId.readFields(in);
		this.creationDate.readFields(in);
		this.score.readFields(in);
		this.viewCount.readFields(in);
		this.ownerUserId.readFields(in);
		this.lastEditorDisplayName.readFields(in);
		this.lastEditDate.readFields(in);
		this.lastActivityDate.readFields(in);
		this.tags.readFields(in);
		this.answerCount.readFields(in);
		this.commentCount.readFields(in);
		this.favoriteCount.readFields(in);
		this.communityOwnedDate.readFields(in);
		this.title.readFields(in);
		this.body.readFields(in);
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
		output.append(this.postTypeId.toString()).append(",");
		output.append(this.acceptedAnswerId.toString()).append(",");
		output.append(this.creationDate.toString()).append(",");
		output.append(this.score.toString()).append(",");
		output.append(this.viewCount.toString()).append(",");
		output.append(this.ownerUserId.toString()).append(",");		
		output.append(this.lastEditorDisplayName.toString()).append(",");
		output.append(this.lastEditDate.toString()).append(",");
		output.append(this.lastActivityDate.toString()).append(",");
		output.append(this.tags.toString()).append(",");
		output.append(this.answerCount.toString()).append(",");
		output.append(this.commentCount.toString()).append(",");
		output.append(this.favoriteCount.toString()).append(",");		
		output.append(this.communityOwnedDate.toString()).append(",");
		output.append(this.title.toString()).append(",");
		output.append(this.body.toString());
		
		return output.toString();
	}
}
