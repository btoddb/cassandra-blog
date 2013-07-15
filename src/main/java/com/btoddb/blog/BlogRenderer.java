package com.btoddb.blog;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.UUID;

/**
 * Render Blog data to stdout.
 *
 */
public class BlogRenderer {
    private static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("MM-dd-YYYY");
    private static final DateTimeFormatter timeOnlyFormatter = DateTimeFormat.forPattern("HH:mm");

    private BlogDao dao;

    public BlogRenderer(BlogDao dao) {
        this.dao = dao;
    }

    public void displayComment(Comment c, String indent ) {
        if ( null == indent ) {
            indent = "";
        }

        System.out.println(indent + " ==> " + c.getUserDisplayName() + " said @ "
                + timeOnlyFormatter.print(c.getCreateTimestamp())
                + " on " + dateFormatter.print(c.getCreateTimestamp()) + " : (cid = " + c.getId() + ", pid = " + c.getPostId() +")");
        System.out.print(indent + "     ");
        if ( null != c.getVotes() && 0 < c.getVotes() ) {
            System.out.print("(" + c.getVotes() +
                    " " + (1 == c.getVotes() ? "vote" : "votes") + ") ");
        }
        System.out.println(c.getText() );
    }

    public void displayPost(Post p, boolean includeComments, String indent ) {
        if ( dao.postCommentsNeedSorting(p.getId())) {
            dao.sortCommentsByVotes(p.getId());
        }

        if ( null == indent ) {
            indent = "";
        }

        System.out.println(indent + " ==> " + p.getUserDisplayName() + " posted @ "
                + timeOnlyFormatter.print(p.getCreateTimestamp())
                + " on " + dateFormatter.print(p.getCreateTimestamp()) + " : (pid = " + p.getId() +")");
        System.out.print(indent + "     ");
        if ( null != p.getVotes() && 0 < p.getVotes() ) {
            System.out.print("(" + p.getVotes() +
                    " " + (1 == p.getVotes() ? "vote" : "votes") + ") ");
        }
        System.out.println("Title: " + p.getTitle() );
        System.out.println(indent + "     " + p.getText() );
        if ( includeComments ) {
            int numDashes = StringUtils.length(indent + "     Comments");
            System.out.println("     " + StringUtils.repeat("=", numDashes));
            System.out.println( indent + "     Comments");
            System.out.println("     " + StringUtils.repeat("=", numDashes));
            List<UUID> uuidList = dao.findCommentUUIDsByPostSortedByVotes(p.getId());
            List<Comment> commentList = dao.findCommentsByUUIDList(uuidList);
            if ( null != commentList && !commentList.isEmpty() ) {
                for ( Comment c : commentList ) {
                    displayComment(c, indent+"     " + "  ");
                }
            }
        }
    }

    public void displayUser(User user, boolean includePosts, String indent) {
        if ( null == indent ) {
            indent = "";
        }

        System.out.println(indent + user.getName() + " (" + user.getEmail() + ")" );

        if ( includePosts ) {
            List<Post> postList = dao.findPostsByUser(user.getEmail());
            if ( null != postList && !postList.isEmpty() ) {
                for ( Post p : postList ) {
                    displayPost(p, false, indent + "  ");
                }
            }
        }
    }
}
