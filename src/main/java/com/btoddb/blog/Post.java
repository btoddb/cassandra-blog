package com.btoddb.blog;

import me.prettyprint.hom.annotations.Column;
import me.prettyprint.hom.annotations.Id;
import me.prettyprint.hom.converters.JodaTimeHectorConverter;
import org.joda.time.DateTime;

import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.UUID;

/**
 *
 *
 */
@Entity
@Table(name="posts")
public class Post {
    public static final String POST_COL_USER_EMAIL = "user_email";
    public static final String POST_COL_USER_NAME = "user_display_name";
    public static final String POST_COL_TITLE = "title";
    public static final String POST_COL_CREATE_TS = "create_ts";
    public static final String POST_COL_TEXT = "text";

    @Id
    private UUID id;

    @Column(name=POST_COL_USER_EMAIL)
    private String userEmail;

    @Column(name=POST_COL_USER_NAME)
    private String userDisplayName;

    @Column(name=POST_COL_TITLE)
    private String title;

    @Column(name=POST_COL_CREATE_TS, converter = JodaTimeHectorConverter.class)
    private DateTime createTimestamp;

    @Column(name=POST_COL_TEXT)
    private String text;

    private Long votes;

    public Post() {
    }

    public Post(UUID id, String userEmail, String userDisplayName, String title, DateTime createTimestamp, String text) {
        this.id = id;
        this.userEmail = userEmail;
        this.userDisplayName = userDisplayName;
        this.title = title;
        this.createTimestamp = createTimestamp;
        this.text = text;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public DateTime getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(DateTime createTimestamp) {
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

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
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

        Post post = (Post) o;

        if (id != null ? !id.equals(post.id) : post.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Post{" +
                "id=" + id +
                ", userEmail='" + userEmail + '\'' +
                ", userDisplayName='" + userDisplayName + '\'' +
                ", title='" + title + '\'' +
                ", createTimestamp=" + createTimestamp +
                ", votes=" + votes +
                ", text='" + text + '\'' +
                '}';
    }
}
