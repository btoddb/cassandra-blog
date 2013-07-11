package com.btoddb.blog;

import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.*;
import me.prettyprint.hom.EntityManagerImpl;
import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 *
 */
public class BlogDao {
    private static final Logger logger = LoggerFactory.getLogger(BlogDao.class);

    private static final String KEYSPACE_NAME = "blog_btoddb";
    private static final String CASS_HOST = "localhost";
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern("YYYYMMdd:HH");
    private static final byte[] POSTS_BY_VOTE_KEY = "posts-sorted".getBytes();

    private Cluster cluster;
    private Keyspace keyspace;
    private EntityManagerImpl entityManager;

    private static final String CF_USERS = "users";
    private static final String USER_COL_PASS = "password";
    private static final String USER_COL_NAME = "name";

    private static final String CF_COMMENTS = "comments";

    private static final String CF_POSTS = "posts";

    private static final String CF_USER_POSTS = "user_posts";

    private static final String CF_USER_VOTES = "user_votes";

    private static final String CF_POSTS_BY_TIME = "posts_by_time";

    private static final String CF_POSTS_BY_VOTE = "posts_sorted_by_vote";

    private static final String CF_USER_COMMENTS = "user_comments";

    private static final String CF_POST_COMMENTS = "post_comments";

    private static final String CF_VOTES = "votes";


    public void init() {
		initHector();
	}

    public User saveUser( User user ) {
        StopWatch sw = new StopWatch();
        sw.start();
        // this simple save could easily be done with HOM (Hector Object Mapper)
        Mutator<String> m = HFactory.createMutator(keyspace, StringSerializer.get());
        m.addInsertion(user.getEmail(), CF_USERS, HFactory.createColumn(USER_COL_PASS, user.getPassword(), StringSerializer.get(), StringSerializer.get()));
        m.addInsertion(user.getEmail(), CF_USERS, HFactory.createColumn(USER_COL_NAME, user.getName(), StringSerializer.get(), StringSerializer.get()));
        m.execute();
        logger.info("saveUser duration = " + sw.getTime() + "ms");
        return user;
    }

    public Post savePost( Post post ) {
        StopWatch sw = new StopWatch();
        sw.start();
        Mutator<byte[]> m = HFactory.createMutator(keyspace, BytesArraySerializer.get());

        // insert row for Post - EntityManager handles mapping POJO to Cassandra row
        entityManager.persist(Collections.singleton(post), m);

        // insert one-to-many for user->post : these are sorted by TimeUUID (chrono + unique)
        m.addInsertion(StringSerializer.get().toBytes(post.getUserEmail()), CF_USER_POSTS, HFactory.createColumn(post.getId(), EMPTY_BYTES));

        // insert TimeUUID post ID to track order the posts were entered
        DateTime dt = calculatePostTimeGranularity(post.getCreateTimestamp());
        m.addInsertion(StringSerializer.get().toBytes(hourFormatter.print(dt)), CF_POSTS_BY_TIME, HFactory.createColumn(post.getId(), EMPTY_BYTES));

        // send the batch
        m.execute();
        logger.info("duration = " + sw.getTime() + "ms");

        return post;
    }

    private DateTime calculatePostTimeGranularity(long timestamp) {
        return new DateTime(timestamp).withZone(DateTimeZone.forOffsetHours(0)).hourOfDay().roundFloorCopy();
    }

    public Comment saveComment( Comment comment ) {
        StopWatch sw = new StopWatch();
        sw.start();
        Mutator<byte[]> m = HFactory.createMutator(keyspace, BytesArraySerializer.get());

        // insert row for comment - EntityManager handles mapping POJO to Cassandra row
        entityManager.persist(Collections.singleton(comment), m);

        // insert one-to-many for user->comments and post->comments : these are sorted by TimeUUID (chrono + unique)
        m.addInsertion(StringSerializer.get().toBytes(comment.getUserEmail()), CF_USER_COMMENTS, HFactory.createColumn(comment.getId(), EMPTY_BYTES));
        m.addInsertion(UUIDSerializer.get().toBytes(comment.getPostId()), CF_POST_COMMENTS, HFactory.createColumn(comment.getId(), EMPTY_BYTES));

        // send the batch
        m.execute();
        logger.info("duration = " + sw.getTime() + "ms");

        return comment;
    }

    public User findUser( String email ) {
        StopWatch sw = new StopWatch();
        sw.start();
        SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        q.setColumnFamily(CF_USERS);
        q.setKey(email);
        q.setRange(null, null, false, 100);
        QueryResult<ColumnSlice<String, String>> qr = q.execute();

        ColumnSlice<String, String> slice = qr.get();

        if ( null == slice || slice.getColumns().isEmpty() ) {
            return null;
        }

        User user = new User();
        user.setEmail(email);
        user.setPassword(slice.getColumnByName(USER_COL_PASS).getValue());
        user.setName(slice.getColumnByName(USER_COL_NAME).getValue());

        logger.info("duration = " + sw.getTime() + "ms");
        return user;
    }

    public Post findPost( UUID postId ) {
        Post p = entityManager.find(Post.class, postId);
        Map<UUID, Long> voteMap = findVotes(Collections.singletonList(postId));
        p.setVotes( voteMap.get(postId));
        return p;
    }

    public List<UUID> findPostUUIDsByUser( String userEmail ) {
        StopWatch sw = new StopWatch();
        sw.start();
        SliceQuery<String, UUID, byte[]> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(), UUIDSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_USER_POSTS);
        q.setKey(userEmail);
        q.setRange(null, null, false, 10);

        ColumnSliceIterator<String, UUID, byte[]> iter = new ColumnSliceIterator<String, UUID, byte[]>(q, null, (UUID)null, false);
        List<UUID> uuidList = new LinkedList<UUID>();
        while ( iter.hasNext() ) {
            HColumn<UUID, byte[]> col = iter.next();
            uuidList.add(col.getName());
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return uuidList;
    }

    public List<UUID> findPostUUIDsByTimeRange( long start, long end ) {
        // this method assumes the number of keys for the range is "not too big" so as to blow
        // out thrift's frame buffer or cause cassandra to take too long and "time out"
        DateTime firstRow = calculatePostTimeGranularity(start);
        DateTime lastRow = calculatePostTimeGranularity(end);

        MultigetSliceQuery<String, UUID, byte[]> q = HFactory.createMultigetSliceQuery(keyspace, StringSerializer.get(), UUIDSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_POSTS_BY_TIME);
        DateTime current = firstRow;
        List<String> rowKeys = new LinkedList<String>();
        while ( current.isBefore(lastRow) || current.isEqual(lastRow) ) {
            rowKeys.add(hourFormatter.print(current));
            current = current.plusHours(1);
        }
        q.setKeys(rowKeys);
        q.setRange(null, null, false, 1000); // this is an assumption that there will not be more than 1000 posts in one hour

        QueryResult<Rows<String, UUID, byte[]>> qr = q.execute();
        Rows<String, UUID, byte[]> rows = qr.get();
        if ( null == rows || 0 == rows.getCount() ) {
            return null;
        }

        List<UUID> uuidList = new LinkedList<UUID>();
        for ( Row<String, UUID, byte[]> row : rows ) {
            ColumnSlice<UUID, byte[]> slice = row.getColumnSlice();
            for ( HColumn<UUID, byte[]> col : slice.getColumns() ) {
                long t = TimeUUIDUtils.getTimeFromUUID(col.getName());
                if ( t > end ) {
                    break;
                }

                if ( t >= start ) {
                    uuidList.add(col.getName());
                }
            }
        }

        return uuidList;
    }

    public List<Post> findPostsByUser( String userEmail ) {
        StopWatch sw = new StopWatch();
        sw.start();
        List<UUID> uuidList = findPostUUIDsByUser(userEmail);
        if ( uuidList.isEmpty() ) {
            return null;
        }

        return findPostsByUUIDList( uuidList, true );
    }

    public List<Post> findPostsByTimeRange(long start, long end) {
        List<UUID> uuidList = findPostUUIDsByTimeRange(start, end);
        if ( uuidList.isEmpty() ) {
            return null;
        }

        return findPostsByUUIDList( uuidList, true );
    }

    public void sortPostsByVote(int days) {
        StopWatch sw = new StopWatch();
        sw.start();
        Mutator<byte[]> m = HFactory.createMutator(keyspace, BytesArraySerializer.get());

        // delete the old row first, then we'll add the new
        m.addDeletion(POSTS_BY_VOTE_KEY, CF_POSTS_BY_VOTE);

        // calc date range, end with yesterday and start 'days' prior
        DateTime start = new DateTime().minusDays(days).hourOfDay().roundFloorCopy();
//        DateTime end = start.plusDays(days-1);
        DateTime end = new DateTime();
        List<UUID> uuidList = findPostUUIDsByTimeRange(start.getMillis(), end.getMillis());


        // find votes, then sort them
        if ( null != uuidList && !uuidList.isEmpty() ) {
            Map<UUID, Long> voteMap = findVotes(uuidList);
//            TreeMap<UUID, Long> sortedMap = new TreeMap<UUID, Long>(new VoteComparator(voteMap));
//            sortedMap.putAll(voteMap);

            // now write to CF
            for (Map.Entry<UUID, Long> entry : voteMap.entrySet() ) {
                Composite colName = new Composite(entry.getValue(), entry.getKey());
                m.addInsertion(POSTS_BY_VOTE_KEY, CF_POSTS_BY_VOTE, HFactory.createColumn(colName, EMPTY_BYTES));
            }
        }

        // send the batch
        m.execute();
        logger.info("duration = " + sw.getTime() + "ms");
    }

    private List<Post> findPostsByUUIDList(List<UUID> uuidList, boolean includeVotes) {
        StopWatch sw = new StopWatch();
        sw.start();
        MultigetSliceQuery<UUID, String, byte[]> q = HFactory.createMultigetSliceQuery(keyspace, UUIDSerializer.get(), StringSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_POSTS);
        q.setRange(null, null, false, 100);
        q.setKeys(uuidList);
        QueryResult<Rows<UUID, String, byte[]>> qr = q.execute();
        Rows<UUID, String, byte[]> rows = qr.get();
        if ( null == rows || 0 == rows.getCount()) {
            return null;
        }
        LinkedHashMap<UUID, Post> postMap = new LinkedHashMap<UUID, Post>();
        for ( Row<UUID, String, byte[]> row : rows) {
            postMap.put(row.getKey(), entityManager.find(Post.class, row.getKey(), row.getColumnSlice()));
        }

        // gotta do it this way to preserve ordering from the original UUID List
        List<Post> postList = new LinkedList<Post>();
        for ( UUID uuid : uuidList ) {
            postList.add(postMap.get(uuid));
        }

        if ( includeVotes ) {
            Map<UUID, Long> voteMap = findVotes(uuidList);
            for (Post post : postList ) {
                Long votes = voteMap.get(post.getId());
                if ( null != votes ) {
                    post.setVotes(votes);
                }
            }
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return postList;
    }

    public List<UUID> findCommentUUIDsByUser( String userEmail ) {
        StopWatch sw = new StopWatch();
        sw.start();
        SliceQuery<String, UUID, byte[]> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(), UUIDSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_USER_COMMENTS);
        q.setKey(userEmail);
        q.setRange(null, null, false, 10);

        ColumnSliceIterator<String, UUID, byte[]> iter = new ColumnSliceIterator<String, UUID, byte[]>(q, null, (UUID)null, false);
        List<UUID> uuidList = new LinkedList<UUID>();
        while ( iter.hasNext() ) {
            HColumn<UUID, byte[]> col = iter.next();
            uuidList.add(col.getName());
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return uuidList;
    }

    public List<Comment> findCommentsByUser( String userEmail ) {
        StopWatch sw = new StopWatch();
        sw.start();
        List<UUID> uuidList = findCommentUUIDsByUser(userEmail);
        if ( uuidList.isEmpty() ) {
            return null;
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return findCommentsByUUIDList(uuidList);
    }

    public List<Comment> findCommentsByPost( UUID postId ) {
        StopWatch sw = new StopWatch();
        sw.start();
        List<UUID> uuidList = findCommentUUIDsByPost(postId);
        if ( uuidList.isEmpty() ) {
            return null;
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return findCommentsByUUIDList(uuidList);
    }

    public List<Comment> findCommentsByUUIDList( List<UUID> uuidList ) {
        StopWatch sw = new StopWatch();
        sw.start();
        MultigetSliceQuery<UUID, String, byte[]> q = HFactory.createMultigetSliceQuery(keyspace, UUIDSerializer.get(), StringSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_COMMENTS);
        q.setRange(null, null, false, 100);
        q.setKeys(uuidList);
        QueryResult<Rows<UUID, String, byte[]>> qr = q.execute();
        Rows<UUID, String, byte[]> rows = qr.get();
        if ( null == rows || 0 == rows.getCount()) {
            return null;
        }

        List<Comment> commentList = new LinkedList<Comment>();
        for ( Row<UUID, String, byte[]> row : rows) {
            commentList.add(entityManager.find(Comment.class, row.getKey(), row.getColumnSlice()));
        }

        Map<UUID, Long> voteMap = findVotes(uuidList);
        for (Comment comment : commentList ) {
            Long votes = voteMap.get(comment.getId());
            if ( null != votes ) {
                comment.setVotes(votes);
            }
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return commentList;
    }

    public List<UUID> findCommentUUIDsByPost( UUID postId ) {
        StopWatch sw = new StopWatch();
        sw.start();
        SliceQuery<UUID, UUID, byte[]> q = HFactory.createSliceQuery(keyspace, UUIDSerializer.get(), UUIDSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_POST_COMMENTS);
        q.setKey(postId);
        q.setRange(null, null, false, 10);

        ColumnSliceIterator<UUID, UUID, byte[]> iter = new ColumnSliceIterator<UUID, UUID, byte[]>(q, null, (UUID)null, false);
        List<UUID> uuidList = new LinkedList<UUID>();
        while ( iter.hasNext() ) {
            HColumn<UUID, byte[]> col = iter.next();
            uuidList.add(col.getName());
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return uuidList;
    }

    public void vote( String userEmail, List<UUID> uuidList ) {
        StopWatch sw = new StopWatch();
        sw.start();

//        // make sure user hasn't already voted
//        List<UUID> readyList = new LinkedList<UUID>();
//        for ( )

        Mutator<byte[]> m = HFactory.createMutator(keyspace, BytesArraySerializer.get());

        for ( UUID uuid : uuidList ) {
            m.addCounter( UUIDSerializer.get().toBytes(uuid), CF_VOTES, HFactory.createCounterColumn("v", 1));
            m.addInsertion(StringSerializer.get().toBytes(userEmail), CF_USER_VOTES, HFactory.createColumn(uuid, EMPTY_BYTES));
        }

        logger.info("duration = " + sw.getTime() + "ms");
        m.execute();
    }

    public Map<UUID, Long> findVotes( List<UUID> uuidList ) {
        StopWatch sw = new StopWatch();
        sw.start();
        MultigetSliceCounterQuery<UUID, String> q = HFactory.createMultigetSliceCounterQuery(keyspace, UUIDSerializer.get(), StringSerializer.get());
        q.setColumnFamily(CF_VOTES);
        q.setKeys(uuidList);
        q.setRange(null, null, false, 100);
        QueryResult<CounterRows<UUID, String>> qr = q.execute();
        CounterRows<UUID, String> rows = qr.get();
        if ( null == rows || 0 == rows.getCount()) {
            return null;
        }

        Map<UUID, Long> voteList = new HashMap<UUID, Long>();

        for ( CounterRow<UUID, String> row : rows ) {
            CounterSlice<String> slice = row.getColumnSlice();
            if ( null != slice && !slice.getColumns().isEmpty()) {
                voteList.put( row.getKey(), slice.getColumnByName("v").getValue());
            }
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return voteList;
    }

	public void initHector() {
		cluster = HFactory.getOrCreateCluster("training-cluster", CASS_HOST + ":9160");
		keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cluster);
        entityManager = new EntityManagerImpl(keyspace, "com.btoddb.blog" );
	}

    public List<Post> findPostsByVote(int number) {
        StopWatch sw = new StopWatch();
        sw.start();
        SliceQuery<byte[], Composite, byte[]> q = HFactory.createSliceQuery(keyspace, BytesArraySerializer.get(), CompositeSerializer.get(), BytesArraySerializer.get());
        q.setColumnFamily(CF_POSTS_BY_VOTE);
        q.setKey(POSTS_BY_VOTE_KEY);
        q.setRange(null, null, false, number);

        ColumnSliceIterator<byte[], Composite, byte[]> iter = new ColumnSliceIterator<byte[], Composite, byte[]>(q, null, (Composite)null, false);
        List<UUID> uuidList = new LinkedList<UUID>();
        Map<UUID, Long> voteMap = new HashMap<UUID, Long>();
        while ( iter.hasNext() ) {
            HColumn<Composite, byte[]> col = iter.next();
            ByteBuffer bb = (ByteBuffer)col.getName().get(1);
            UUID uuid = UUIDSerializer.get().fromByteBuffer(bb);
            uuidList.add( uuid );
            voteMap.put( uuid, LongSerializer.get().fromByteBuffer((ByteBuffer)col.getName().get(0)));
        }

        List<Post> postList = findPostsByUUIDList(uuidList, false);
        for ( Post p : postList ) {
            p.setVotes(voteMap.get(p.getId()));
        }

        logger.info("duration = " + sw.getTime() + "ms");
        return postList;
    }


    class VoteComparator implements Comparator<UUID> {
        Map<UUID, Long> theMap;

        public VoteComparator(Map<UUID, Long> theMap) {
            this.theMap = theMap;
        }

        @Override
        public int compare(UUID k1, UUID k2) {
            Long v1 = theMap.get(k1);
            Long v2 = theMap.get(k2);
            if ( v1 >= v2 ) {
                return 1;
            }
            else if ( v1 < v2 ) {
                return -1;
            }
            else {
                return k1.compareTo(k2);
            }
        }
    }
}
