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
import sun.rmi.runtime.Log;

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
    private List<Container> gradientFingers;
    private NewsView localNewsView;

    private Boolean wantsToBeLeader = true;
    private Boolean stable = false;

    /**
     * Save the node already verif in a previous verification
     */
    private List<KAddress> alreadyVerif = new ArrayList<>();


    /**
     * For node who think to be the leader.
     * Save nodes not verified yet.
     */
    private List<KAddress> verifInProgress = new ArrayList<>();



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
            gradientFingers = sample.gradientFingers;

            wantsToBeLeader = true;
            if (stable) {

                LeaderValid lV = new LeaderValid(false, selfAdr, localNewsView);

                for (Container c : gradientNeighbours){
                    // Check if all neighbours are inferior, and so if I can be a leader
                    if(viewComparator.compare(c.getContent(), localNewsView) > 0){
                        wantsToBeLeader = false;
                        break;
                    }
                    alreadyVerif.add((KAddress) c.getSource());
                }
                if (wantsToBeLeader) {
                    // Ask all my neighbours if no one are superior than me
                    for (Container c : gradientNeighbours) {
                        verifInProgress.add((KAddress) c.getSource());
                        KAddress partner = (KAddress) c.getSource();
                        //LOG.info(c.getSource().toString());
                        KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, lV);
                        trigger(msg, networkPort);
                    }
                }
            }
        }
    };

    /**
     * Handle by NewsComp when the view is stable
     */
    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
            stable = event.leaderAdr.equals(selfAdr);
        }
    };

    /**
     * Handle by handleGradientSample when a node think it is the leader.
     */
    ClassMatchedHandler handleLeaderValidation =
        new ClassMatchedHandler<LeaderValid, KContentMsg<?, KHeader<?>, LeaderValid>>() {
            @Override
            public void handle(LeaderValid content, KContentMsg<?, KHeader<?>, LeaderValid> container) {
                //Count the number of messages before the election of the leader is done
                messageCountForLeaderElection++;
                if(localNewsView == null)
                    return;
                
                if (!content.toLeader) {

                    KAddress leaderAdress = content.getAddress();
                    //Compare my view with the one of the node who think to be the leader
                    if(viewComparator.compare(content.getLeaderView(), localNewsView) > 0){
                        // if I am lower, valid than I am inferior to the leader
                        KHeader header = new BasicHeader(selfAdr, leaderAdress, Transport.UDP);
                        content.toLeader = true;
                        content.setAddress(selfAdr);
                        content.newBranch = false;
                        KContentMsg msg = new BasicContentMsg(header, content.clone());
                        trigger(msg, networkPort);
                    }

                    List<KAddress> tempToSend = new ArrayList<>();
                    content.toLeader = true;
                    content.newBranch = true;
                    // Verif if one of my neighbour aren't verified yet
                    for (Container c : gradientNeighbours) {
                        if (!alreadyVerif.contains(c.getSource())) {
                            // if not add it to the verified one and save it in the list to send later
                            alreadyVerif.add((KAddress) c.getSource());
                            tempToSend.add((KAddress) c.getSource());

                            KHeader header = new BasicHeader(selfAdr, leaderAdress, Transport.UDP);
                            content.setAddress((KAddress) c.getSource());
                            KContentMsg msg = new BasicContentMsg(header, content.clone());

                            trigger(msg, networkPort);
                        }
                    }

                    content.toLeader = false;
                    content.setAddress(leaderAdress);
                    for (KAddress adr : tempToSend) {
                        KHeader header = new BasicHeader(selfAdr, adr, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, content.clone());
                        trigger(msg, networkPort);
                    }
                }
                else{
                    // if it's a new branch/node, add it to the list verifInProgress
                    if(content.newBranch){
                        if(alreadyVerif.contains(content.getAddress())) {
                            verifInProgress.add(content.getAddress());
                        }
                    }
                    //else if it's not a new branch/node, remove it from the list verifInProgress
                    else{
                        if(verifInProgress.contains(content.getAddress())){
                            verifInProgress.remove(content.getAddress());
                            alreadyVerif.add(content.getAddress());
                        }
                    }
                    //If the list is empty, the node is a leader.
                    if(verifInProgress.isEmpty()){
                        LOG.info("Leader elected in :" + messageCountForLeaderElection + " messages!");
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
