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

    public static void displayComments(List<Comment> commentList) {
        System.out.println( "    =============");
        if ( null != commentList ) {
            for ( Comment c : commentList ) {
                System.out.println("    " + c);
                System.out.println( "    =============");
            }
        }
        else {
            System.out.println( "    EMPTY" );
            System.out.println( "    =============");
        }
    }

    public void displayUser(User user) {
        System.out.println( "=============");
        if ( null != user ) {
            System.out.println(user);
            List<Post> postList = dao.findPostsByUser(user.getEmail());
            displayPosts(postList);
        }
        else {
            System.out.println( "  EMPTY" );
            System.out.println( "  =============");
        }
    }

    public void displayPosts(List<Post> postList) {
        System.out.println( "  =============");
        if ( null != postList ) {
            for ( Post p : postList ) {
                System.out.println("  " + p);
                List<Comment> commentList = dao.findCommentsByPost(p.getId());
                displayComments(commentList);
            }
        }
        else {
            System.out.println( "  EMPTY" );
            System.out.println( "  =============");
        }
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
}
