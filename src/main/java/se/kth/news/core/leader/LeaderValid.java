package se.kth.news.core.leader;


import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.update.View;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael on 26/05/2016.
 */
public class LeaderValid {
    public boolean newBranch = false;
    public boolean toLeader;
    private KAddress address;
    private View leaderView;
    public List<KAddress> myNeighbours = new ArrayList<>();
    public List<KAddress> myAlreadyDone = null;

    public LeaderValid(boolean tL, KAddress kA, View lV){
        toLeader = tL;
        address = kA;
        this.leaderView = lV;
    }

    public View getLeaderView() {
        return leaderView;
    }

    public KAddress getAddress() {
        return address;
    }

    public void setAddress(KAddress address) {
        this.address = address;
    }

    public LeaderValid clone(){
        LeaderValid lV = new LeaderValid(toLeader, address, leaderView);
        lV.newBranch = newBranch;
        return lV;
    }

}
