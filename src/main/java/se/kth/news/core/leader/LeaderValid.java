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
    private List<View> verified;

    public LeaderValid(boolean tL, KAddress kA, View lV){
        toLeader = tL;
        address = kA;
        this.leaderView = lV;
        verified = new ArrayList<>();
        verified.add(lV);
    }

    public View getLeaderView() {
        return leaderView;
    }

    public boolean isInVerified(View v){
        return verified.contains(v);
    }
    public void addVerified(View v){

    }

    public KAddress getAddress() {
        return address;
    }

    public void setAddress(KAddress address) {
        this.address = address;
    }
}
