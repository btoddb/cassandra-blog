package com.btoddb.blog;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.commons.lang.time.StopWatch;

import java.util.List;
import java.util.UUID;

/**
 *
 *
 */
public class BlogMain {
    private static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("MMddYYYY:HHmmss");

    private static BlogService service;
    private static BlogRenderer renderer;

    public static void main(String[] args) {
        //
        // gotta at least have the command
        //

        if ( 1 > args.length ) {
            showUsage();
            return;
        }

        //
        // a little dependency injection here
        //

        BlogDao dao = new BlogDao();
        dao.init();

        service = new BlogService(dao);
        renderer = new BlogRenderer(dao);

        //
        // parse command and process
        //

        for ( int i=0;i < 10;i++ ) {
            StopWatch sw = new StopWatch();
            sw.start();

            try {
                processCommand(args);
            }
            finally {
                System.out.println("execution duration = " + sw.getTime() + "ms");
            }
        }
    }

    private static void processCommand(String[] args) {
        String command = args[0];
        if ( "show-posts-by-range".equalsIgnoreCase(command)) {
            checkArgs("show-posts-by-range", args, 2);
            DateTime start = dateFormatter.withZone(DateTimeZone.forOffsetHours(0)).parseDateTime(args[1]);
            DateTime end = dateFormatter.withZone(DateTimeZone.forOffsetHours(0)).parseDateTime(args[2]);
            List<Post> postList = service.findPostsByTimeRange(start, end);
            System.out.println( String.format("Posts from %s to %s : ", start, end) );
            if ( null != postList && !postList.isEmpty()) {
                for ( Post p : postList ) {
                    renderer.displayPost(p, false, null);
                }
            }
        }
        else if ( "show-user-comments".equalsIgnoreCase(command)) {
            checkArgs("show-user-comments", args, 1);
            User user = service.findUser(args[1]);
            if ( null != user ) {
                System.out.print( "All Comments for : " );
                renderer.displayUser(user, false, null);
                List<Comment> commentList = service.findCommentsByUser(user.getEmail());
                if ( null != commentList && !commentList.isEmpty()) {
                    for ( Comment c : commentList ) {
                        renderer.displayComment(c, "  ");
                    }
                }
            }
        }
        else if ( "show-user".equalsIgnoreCase(command)) {
            checkArgs("show-user", args, 1);
            User u = service.findUser(args[1]);
            renderer.displayUser(u, true, null);
        }
        else if ( "show-post".equalsIgnoreCase(command)) {
            checkArgs("show-post", args, 1);
            Post p = service.findPost( UUID.fromString(args[1]) );
            renderer.displayPost(p, true, null);
        }
        else if ( "show-comment".equalsIgnoreCase(command)) {
            checkArgs("show-comment", args, 1);
            Comment c = service.findComment(UUID.fromString(args[1]));
            renderer.displayComment(c, null);
        }
        else if ( "show-recent-posts".equalsIgnoreCase(command)) {
            checkArgs("show-recent-posts", args, 1);
            service.findRecentPosts(Integer.parseInt(args[1]));
        }
        else if ( "show-top-posts".equalsIgnoreCase(command)) {
            checkArgs("show-top-posts", args, 1);
            List<Post> postList = service.findTopPosts(Integer.parseInt(args[1]));
            if ( null != postList && !postList.isEmpty() ) {
                for ( Post p : postList ) {
                    renderer.displayPost(p, false, null);
                }
            }
        }
        else if ( "create-user".equalsIgnoreCase(command)) {
            checkArgs("create-user", args, 3);
            User u = service.createUser(args[1], args[2], args[3]);
            renderer.displayUser(u, false, null);
        }
        else if ( "create-post".equalsIgnoreCase(command)) {
            checkArgs("create-post", args, 3);
            Post p = service.createPost(args[1], args[2], args[3]);
            renderer.displayPost(p, false, null);
        }
        else if ( "create-comment".equalsIgnoreCase(command) ) {
            checkArgs("create-comment", args, 3);
            Comment c = service.createComment(args[1], UUID.fromString(args[2]), args[3]);
            renderer.displayComment(c, null);
        }
        else if ( "vote".equalsIgnoreCase(command) ) {
            checkArgs("vote", args, 3);
            String type = args[2];
            UUID uuid = UUID.fromString(args[3]);
            service.vote(args[1], type, uuid);
            if ( "comment".equalsIgnoreCase(type)) {
                renderer.displayComment(service.findComment(uuid), null);
            }
            else if ( "post".equalsIgnoreCase(type)) {
                renderer.displayPost(service.findPost(uuid), false, null);
            }
        }
        else {
            showUsage();
        }
    }
    private static void showUsage() {
        System.out.println();

        System.out.println( "usage: BlogMain <command> [<params>]" );
        System.out.println();
        System.out.println( "  commands:" );
        System.out.println( "    create-user <user-email> <password> <name>" );
        System.out.println( "    create-post <user-email> <title> <text>");
        System.out.println( "    create-comment <user-email> <post-id> <text>" );
        System.out.println( "    show-user <user-email>" );
        System.out.println( "    show-post <post-id>" );
        System.out.println( "    show-comment <comment-id>" );
        System.out.println( "    show-posts-by-range <start-time> <end-time> (start/end time in format MMDDYYYY:HHMMSS as GMT)" );
        System.out.println( "    show-user-comments <user-email>" );
        System.out.println( "    show-top-posts <number-of-posts>" );
        System.out.println( "    show-recent-posts <minutes>" );
        System.out.println( "    vote <user-email> <type> <id> (type = POST or COMMENT)" );

        System.out.println();
    }

    private static void checkArgs(String command, String[] args, int numRequiredParams) {
        if ( 1+numRequiredParams != args.length ) {
            System.out.println();
            System.out.println("*** ERROR ***  '" + command +"' command requires " + numRequiredParams + " param(s)");
            showUsage();
            System.exit(1);
        }
    }

}
