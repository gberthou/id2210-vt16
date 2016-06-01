/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.news.core.leader;

import java.util.*;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.core.news.util.NewsViewComparator;
import se.sics.kompics.*;
import se.sics.kompics.network.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.Container;
import se.sics.ktoolbox.util.update.View;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeaderSelectComp extends ComponentDefinition {

    private static int messageCountForLeaderElection = 0;

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Negative<LeaderSelectPort> leaderPort = provides(LeaderSelectPort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    //*******************************INTERNAL_STATE*****************************
    private Comparator viewComparator;

    private List<Container> gradientNeighbours;
    private NewsView localNewsView = null;

    private KAddress temporaryLeader = null;

    /**
     * Save the node already verif in a previous verification
     */
    private List<KAddress> alreadyVerif = new ArrayList<>();


    /**
     * For node who think to be the leader.
     * Save nodes not verified yet.
     */
    private List<KAddress> verifInProgress = new ArrayList<>();

    /**
     * If the node was leader on the prévious selection
     */
    private boolean isLeader = false;

    public LeaderSelectComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        viewComparator = new NewsViewComparator();

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleLeader, leaderPort);
        subscribe(handleLeaderValidation, networkPort);
        subscribe(handleLeader, leaderPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...LeaderSelectComp", logPrefix);
        }
    };
    
    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
        localNewsView = (NewsView) sample.selfView;
        gradientNeighbours = sample.gradientNeighbours;
        }
    };

    /**
     * Handle by NewsComp when the view is stable
     */
    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
            if (localNewsView != null && gradientNeighbours != null) {
                LeaderValid lV = new LeaderValid(false, selfAdr, localNewsView);

                boolean wantsToBeLeader = true;
                temporaryLeader = null;
                alreadyVerif.clear();
                verifInProgress.clear();
                if(isLeader){
                    LeaderUpdate lU = new LeaderUpdate((KAddress) gradientNeighbours.get(0).getSource());
                    trigger(lU, leaderPort);
                }


                for (Container c : gradientNeighbours) {
                    // Check if all neighbours are inferior, and so if I can be a leader
                    if (viewComparator.compare(c.getContent(), localNewsView) > 0) {

                        wantsToBeLeader = false;
                        break;
                    }
                    alreadyVerif.add((KAddress) c.getSource());
                }
                if (wantsToBeLeader) {
                    temporaryLeader = selfAdr;
                    // Ask all my neighbours if no one are superior than me
                    alreadyVerif.add(selfAdr);
                    for (Container c : gradientNeighbours) {
                        verifInProgress.add((KAddress) c.getSource());
                        sendLeaderValidation(false, false, selfAdr, localNewsView, (KAddress) c.getSource(), false);
                    }
                }
            }
        }
    };

    private void sendLeaderValidation(boolean nB, boolean tL, KAddress address, View v, KAddress dst, boolean addAlreadyVerif){

        LeaderValid lV = new LeaderValid(tL, address, v);
        lV.newBranch = nB;
        for (Container c : gradientNeighbours) {
            lV.myNeighbours.add((KAddress) c.getSource());
        }
        if(addAlreadyVerif) {
            lV.myAlreadyDone = alreadyVerif;
        }

        KHeader header = new BasicHeader(selfAdr, dst, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, lV);


        trigger(msg, networkPort);
    }

    /**
     * Handle by handleGradientSample when a node think it is the leader.
     */
    ClassMatchedHandler handleLeaderValidation =
        new ClassMatchedHandler<LeaderValid, KContentMsg<?, KHeader<?>, LeaderValid>>() {
            @Override
            public void handle(LeaderValid content, KContentMsg<?, KHeader<?>, LeaderValid> container) {
                //LOG.info("Select: " + selfAdr + "---" + temporaryLeader + "---");
                //Count the number of messages before the election of the leader is done
                //messageCountForLeaderElection++;
                if(localNewsView == null)
                    return;

                LOG.info("YOLO" + selfAdr + "----" + temporaryLeader);
                if (!content.toLeader) {
                    if(temporaryLeader != null && (temporaryLeader.sameHostAs(content.getAddress())))
                        return;

                    //Compare my view with the one of the node who think to be the leader
                    if(viewComparator.compare(content.getLeaderView(), localNewsView) > 0){
                        if(temporaryLeader != null){
                            sendLeaderValidation(true, true, content.getAddress(), content.getLeaderView(), temporaryLeader, false);
                            return;
                        }
                        temporaryLeader = content.getAddress();
                        sendLeaderValidation(true, true, selfAdr, content.getLeaderView(), content.getAddress(), false);
                    }
                    else{
                        if(temporaryLeader == null){
                            temporaryLeader = selfAdr;
                        }
                        sendLeaderValidation(false, true, temporaryLeader, localNewsView, content.getAddress(), false);
                    }
                }
                else{
                    if(temporaryLeader == null || !temporaryLeader.sameHostAs(selfAdr)){
                        return;
                    }
                    if(content.newBranch) {
                        if(!verifInProgress.contains(content.getAddress()) && !alreadyVerif.contains(content.getAddress())){
                            if(viewComparator.compare(content.getLeaderView(), localNewsView) > 0){
                                temporaryLeader = content.getAddress();
                                sendLeaderValidation(true, true, selfAdr, content.getLeaderView(), temporaryLeader, true);
                                return;
                            }
                        }

                        alreadyVerif.add(content.getAddress());
                        verifInProgress.remove(content.getAddress());
                        if(content.myAlreadyDone != null){
                            alreadyVerif.addAll(content.myAlreadyDone);
                        }
                        for (KAddress ka : content.myNeighbours) {
                            if (!alreadyVerif.contains(ka) && !verifInProgress.contains(ka)) {
                                verifInProgress.add(ka);
                                sendLeaderValidation(true, false, selfAdr, localNewsView, ka, false);
                            }
                        }
                    }
                    else{
                        temporaryLeader = content.getAddress();
                        sendLeaderValidation(true, true, selfAdr, content.getLeaderView(), temporaryLeader, true);
                    }

                    //If the list is empty, the node is a leader.
                    if(verifInProgress.isEmpty()){
                        LOG.info("Leader elected in :" + messageCountForLeaderElection + " messages!");
                        isLeader = true;
                        LeaderUpdate lU = new LeaderUpdate(selfAdr);
                        trigger(lU, leaderPort);
                    }
                }
            }
        };

    public static class Init extends se.sics.kompics.Init<LeaderSelectComp> {

        public final KAddress selfAdr;
        public final Comparator viewComparator;

        public Init(KAddress selfAdr, Comparator viewComparator) {
            this.selfAdr = selfAdr;
            this.viewComparator = viewComparator;
        }
    }
}
