package com.btoddb.blog;

import me.prettyprint.hom.annotations.Column;
import me.prettyprint.hom.annotations.Id;

import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.UUID;

/**
 *
 *
 */
@Entity
@Table(name = "comments")
public class Comment {
    public static final String COL_POST_ID = "post_id";
    public static final String COL_CREATE_TS = "create_ts";
    public static final String COL_TEXT = "text";

    @Id
    private UUID id;

    @Column(name = "user_email")
    private String userEmail;

    @Column(name = "user_name")
    private String userDisplayName;

    @Column(name= COL_POST_ID)
    private UUID postId;

    @Column(name=COL_CREATE_TS)
    private long createTimestamp;

    @Column(name=COL_TEXT)
    private String text;


    private Long votes;

    public Comment() {
    }

    public Comment(UUID id, String userEmail, String userDisplayName, UUID postId, long createTimestamp, String text) {
        this.id = id;
        this.userEmail = userEmail;
        this.userDisplayName = userDisplayName;
        this.postId = postId;
        this.createTimestamp = createTimestamp;
        this.text = text;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getPostId() {
        return postId;
    }

    public void setPostId(UUID postId) {
        this.postId = postId;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public Long getVotes() {
        return votes;
    }

    public void setVotes(Long votes) {
        this.votes = votes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Comment comment = (Comment) o;

        if (id != null ? !id.equals(comment.id) : comment.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "id=" + id +
                ", userEmail='" + userEmail + '\'' +
                ", userName='" + userDisplayName + '\'' +
                ", postId=" + postId +
                ", createTimestamp=" + createTimestamp +
                ", votes='" + votes + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}
