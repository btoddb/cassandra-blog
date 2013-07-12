package com.btoddb.blog;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import java.util.List;
import java.util.UUID;

/**
 *
 *
 */
public class BlogService {
    private final BlogDao dao;

    public BlogService(BlogDao dao) {
        this.dao = dao;
    }

    public Comment createComment(String userEmail, UUID postId, String commentText) {
        User user = dao.findUser(userEmail);
        if ( null == user ) {
            throw new RuntimeException( "user with email, " + userEmail + ", does not exist!  Cannot create comment");
        }

        Comment comment = dao.saveComment(new Comment(TimeUUIDUtils.getUniqueTimeUUIDinMillis(), user.getEmail(), user.getName(),
                                            postId, System.currentTimeMillis(), commentText));
        return comment;
    }

//    public List<Comment> findCommentsByPostIdSortedByVote( UUID postId ) {
//
//    }
}
