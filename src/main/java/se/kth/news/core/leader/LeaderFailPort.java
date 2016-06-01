package se.kth.news.core.leader;

import se.sics.kompics.PortType;

/**
 * Created by Michael on 01/06/2016.
 */
public class LeaderFailPort  extends PortType {
    {
        indication(LeaderFail.class);
        request(LeaderFail.class);
    }
}
