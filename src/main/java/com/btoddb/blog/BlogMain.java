package com.btoddb.blog;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 *
 *
 */
public class BlogMain {
    public static final int MAX_SORT_DAYS = 30;
    private static BlogDao dao;
    private static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("YYYYMMdd:HHmmss");
    private static BlogService service;

    public static void main(String[] args) {
        if ( 1 > args.length ) {
            showUsage();
            return;
        }

        dao = new BlogDao();
        dao.init();

        service = new BlogService(dao);
        BlogRenderer renderer = new BlogRenderer();

        String command = args[0];

        if ( "show-user-posts".equalsIgnoreCase(command)) {
            checkArgs( "show-user-posts", args, 1);
            showUserPosts(args[1]);
        }
        else if ( "show-posts-by-range".equalsIgnoreCase(command)) {
            checkArgs("show-posts-by-range", args, 2);
            showPostsByTimeRange(args[1], args[2]);
        }
        else if ( "show-user-comments".equalsIgnoreCase(command)) {
            checkArgs("show-user-comments", args, 1);
            showCommentsByUser(args[1]);
        }
        else if ( "show-posts-recent".equalsIgnoreCase(command)) {
            checkArgs("show-posts-recent", args, 1);
            showRecentPosts(Integer.parseInt(args[1]));
        }
        else if ( "show-post-comments".equalsIgnoreCase(command)) {
            checkArgs("show-post-comments", args, 1);
            showCommentsByPost(UUID.fromString(args[1]));
        }
        else if ( "show-top-posts".equalsIgnoreCase(command)) {
            checkArgs("show-top-posts", args, 1);
            showTopPosts(Integer.parseInt(args[1]));
        }
        else if ( "vote".equalsIgnoreCase(command) ) {
            checkArgs("vote", args, 3);
            vote(args[1], args[2], UUID.fromString(args[3]));
        }
        else if ( "create-user".equalsIgnoreCase(command)) {
            checkArgs("create-user", args, 3);
            createUser(args[1], args[2], args[3]);
        }
        else if ( "create-post".equalsIgnoreCase(command)) {
            checkArgs("create-post", args, 3);
            createPost(args[1], args[2], args[3]);
        }
        else if ( "create-comment".equalsIgnoreCase(command) ) {
            checkArgs("create-comment", args, 3);
            Comment c = service.createComment(args[1], UUID.fromString(args[2]), args[3]);
            vote(args[1], "comment", c.getId());
            c = dao.findCommentsByUUIDList(Collections.singletonList(c.getId())).get(0);
            renderer.displayComment( c, null );
        }
        else {
            showUsage();
        }
    }

    private static void vote(String userEmail, String type, UUID uuid) {
        dao.vote(userEmail, Collections.singletonList(uuid));
        if ( "post".equalsIgnoreCase(type)) {
            service.displayPosts(Collections.singletonList(dao.findPost(uuid)));
        }
        else if ( "comment".equalsIgnoreCase(type)) {
//            displayPosts(Collections.singletonList(dao.findPostFromCommentId(uuid)));
        }
    }

    private static void createUser(String email, String password, String fullName) {
        User user = dao.saveUser(new User(email, password, fullName));
        service.displayUser(user);
    }

    private static void createPost(String userEmail, String title, String text) {
        User user = dao.findUser(userEmail);
        Post post = dao.savePost(new Post(TimeUUIDUtils.getUniqueTimeUUIDinMillis(), user.getEmail(), user.getName(), title, System.currentTimeMillis(), text));
        service.displayPosts(Collections.singletonList(post));
    }

    private static void showTopPosts(int number) {
        dao.sortPostsByVote(MAX_SORT_DAYS);
        List<Post> postList = dao.findPostsByVote(number);
        service.displayPosts(postList);
    }

    private static void showRecentPosts(int minutes) {
        DateTime end = new DateTime().withZone(DateTimeZone.forOffsetHours(0));
        DateTime start = end.minusMinutes(minutes);
        showPostsByTimeRange(start.getMillis(), end.getMillis());
    }

    private static void showPostsByTimeRange(String startAsStr, String endAsStr) {
        long start = dateFormatter.withZone(DateTimeZone.forOffsetHours(0)).parseMillis(startAsStr);
        long end = dateFormatter.withZone(DateTimeZone.forOffsetHours(0)).parseMillis(endAsStr);
        showPostsByTimeRange(start, end);
    }

    private static void showPostsByTimeRange(long start, long end) {
        List<Post> postList = dao.findPostsByTimeRange(start, end);
        System.out.println( String.format("Posts from %s to %s : ", new DateTime(start).withZone(DateTimeZone.forOffsetHours(0)), new DateTime(end).withZone(DateTimeZone.forOffsetHours(0)) ) );
        service.displayPosts(postList);
    }

    private static void showCommentsByPost(UUID postId) {
        Post post = dao.findPost(postId);
        service.displayPosts(Collections.singletonList(post));
    }

    private static void showCommentsByUser(String userEmail) {
        System.out.println( "=============");
        User user = dao.findUser(userEmail);
        List<Comment> commentList = dao.findCommentsByUser(user.getEmail());
        System.out.println( "All Comments for : " + dao.findUser(user.getEmail()));
        BlogService.displayComments(commentList);
    }

    private static void showUserPosts(String userEmail) {
        User user = dao.findUser(userEmail);
        System.out.println( "Posts for User : " + user );
        List<Post> postList = dao.findPostsByUser(user.getEmail());
        service.displayPosts(postList);
    }

    private static void checkArgs(String command, String[] args, int numRequiredParams) {
        if ( 1+numRequiredParams != args.length ) {
            System.out.println("'" + command +"' command requires " + numRequiredParams + " param(s)");
            System.exit(1);
        }
    }

    private static void showUsage() {
        System.out.println();
        System.out.println( "usage: BlogMain <command> [<params>]" );
        System.out.println();
        System.out.println( "  commands:" );
        System.out.println( "    posts-by-range <start-time> <end-time> (start/end time in format MMDDYYYY-HHMMSS as GMT)" );
        System.out.println( "    user-posts <user-email>" );
        System.out.println( "    user-comments <user-email>" );
        System.out.println( "    post-comments <post-id>" );
        System.out.println( "    top-posts <number-of-days>" );
        System.out.println();
    }

}
