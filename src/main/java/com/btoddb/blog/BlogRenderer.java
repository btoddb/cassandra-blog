package com.btoddb.blog;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 *
 */
public class BlogRenderer {
    private static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("MM:dd:YYYY");
    private static final DateTimeFormatter timeOnlyFormatter = DateTimeFormat.forPattern("HH:mm");

    public void displayComment(Comment c, String indent ) {
        if ( null == indent ) {
            indent = "";
        }

        System.out.println(indent + " ==> " + c.getUserDisplayName() + " said @ "
                + timeOnlyFormatter.print(c.getCreateTimestamp())
                + " on " + dateFormatter.print(c.getCreateTimestamp()) + " : (" + c.getId() + ")");
        System.out.print(indent + "     ");
        if ( null != c.getVotes() && 0 < c.getVotes() ) {
            System.out.print("(" + c.getVotes() +
                    " " + (1 == c.getVotes() ? "vote" : "votes") + ") ");
        }
        System.out.println(c.getText() );
    }
}
