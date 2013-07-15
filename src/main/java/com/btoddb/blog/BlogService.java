package com.btoddb.blog;

import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.UUID;

/**
 * Service layer responsible for pre/post processing of data to/from DAO.
 *
 */
public class BlogService {
    public static final int MAX_SORT_DAYS = 30;

    private final BlogDao dao;

    public BlogService(BlogDao dao) {
        this.dao = dao;
    }

    /**
     * Create a new Comment.  Reads the User record before saving to get the user's name.
     *
     * @param userEmail User's email
     * @param postId Post ID
     * @param commentText Complete comment text
     * @return Comment record just saved
     */
    public Comment createComment(String userEmail, UUID postId, String commentText) {
        User user = dao.findUser(userEmail);
        if ( null == user ) {
            throw new RuntimeException( "user with email, " + userEmail + ", does not exist!  Cannot create comment");
        }

        return dao.saveComment(new Comment(TimeUUIDUtils.getUniqueTimeUUIDinMillis(), user.getEmail(), user.getName(),
                                            postId, System.currentTimeMillis(), commentText));
    }

    /**
     * Create a new User.
     *
     * @param email User's email
     * @param password User's password
     * @param fullName User's full name
     * @return User record just saved
     */
    public User createUser(String email, String password, String fullName) {
        return dao.saveUser(new User(email, password, fullName));
    }

    /**
     * Create a new Post.  Reads the User record before saving to get the user's name.
     *
     * @param userEmail User's email
     * @param title Post Title
     * @param text complete Post text
     * @return Post record just created
     */
    public Post createPost(String userEmail, String title, String text) {
        User user = dao.findUser(userEmail);
        return dao.savePost(new Post(TimeUUIDUtils.getUniqueTimeUUIDinMillis(), user.getEmail(), user.getName(), title, new DateTime(), text));
    }

    /**
     * Retrieve Posts for the given time range.
     *
     * @param start Start time in GMT
     * @param end End time in GMT
     * @return List of Post records
     */
    public List<Post> findPostsByTimeRange(DateTime start, DateTime end) {
        return dao.findPostsByTimeRange(start, end);
    }

    /**
     * Find recent Posts over the last 'minutes'
     *
     * @param minutes number of minutes
     */
    public void findRecentPosts(int minutes) {
        DateTime end = new DateTime().withZone(DateTimeZone.forOffsetHours(0));
        DateTime start = end.minusMinutes(minutes);
        findPostsByTimeRange(start, end);
    }

    /**
     * Find the top 'number' of Posts, sorted by vote.
     *
     * @param number Number of Posts to retrieve
     * @return list of Post records
     */
    public List<Post> findTopPosts(int number) {
        dao.sortPostsByVote(MAX_SORT_DAYS);
        return dao.findPostsByVote(number);
    }

    /**
     * Vote on a Post.  A User can only vote once per Comment or Post.
     *
     * @param userEmail User's email
     * @param uuid Post ID
     */
    public void voteOnPost(String userEmail, UUID uuid) {
        if ( null == dao.findUserVote(userEmail, uuid) ) {
            dao.voteOnPost(userEmail, uuid);
        }
    }

    /**
     * Vote on a Comment.  A User can only vote once per Comment or Post.
     *
     * @param userEmail User's email
     * @param uuid Comment ID
     */
    public void voteOnComment(String userEmail, UUID uuid) {
        if ( null == dao.findUserVote(userEmail, uuid) ) {
            dao.voteOnComment(userEmail, uuid);
        }
    }

    /**
     * Retrieve User record by User Email.
     *
     * @param userEmail User's email
     * @return User record
     */
    public User findUser(String userEmail) {
        return dao.findUser(userEmail);
    }

    /**
     * Retrieve Comments by User Email.
     *
     * @param userEmail User's email
     * @return List of Comment records
     */
    public List<Comment> findCommentsByUser(String userEmail) {
        return dao.findCommentsByUser(userEmail);
    }

    /**
     * Retreive Post by ID.
     *
     * @param postId Post ID
     * @return Post record
     */
    public Post findPost(UUID postId) {
        return dao.findPost(postId);
    }

    /**
     * Retrieve Comment by ID.
     *
     * @param commentId Comment ID
     * @return Comment record
     */
    public Comment findComment(UUID commentId) {
        return dao.findComment(commentId);
    }
}
