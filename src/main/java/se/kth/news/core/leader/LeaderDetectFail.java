package se.kth.news.core.leader;

import se.sics.ktoolbox.util.network.KAddress;

/**
 * Created by Michael on 01/06/2016.
 */
public class LeaderDetectFail {
    private KAddress address;
    public boolean leader;

    public LeaderDetectFail(KAddress leader) {
        this.address = leader;
    }

    public KAddress getAddress() {
        return address;
    }

    public void setAddress(KAddress address) {
        this.address = address;
    }
}
