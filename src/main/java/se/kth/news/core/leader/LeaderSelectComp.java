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

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class LeaderSelectComp extends ComponentDefinition {

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

    private List<KAddress> verifInProgress = new ArrayList<>();

    private int maxNewsCountFromLeader = 0;

    public LeaderSelectComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        viewComparator = new NewsViewComparator();

        subscribe(handleStart, control);
        subscribe(handleGradientSample, gradientPort);
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
            wantsToBeLeader = true;
            if (stable) {
                gradientNeighbours = sample.gradientNeighbours;
                gradientFingers = sample.gradientFingers;
                localNewsView = (NewsView) sample.selfView;
                LeaderValid lV = new LeaderValid(false, selfAdr, localNewsView);

                for (Container c : gradientNeighbours){
                    if(viewComparator.compare(c.getContent(), localNewsView) > 0){
                        wantsToBeLeader = false;
                        break;
                    }
                    lV.addVerified((View) c.getContent());
                }
                if (wantsToBeLeader) {
                    for (Container c : gradientNeighbours) {
                        verifInProgress.add((KAddress) c.getSource());
                        KAddress partner = (KAddress) c.getSource();
                        KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, lV);
                        trigger(msg, networkPort);
                    }
                }
            }
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
            LOG.info("HANDLELEADER");
            stable = event.leaderAdr.equals(selfAdr);
        }
    };

    ClassMatchedHandler handleLeaderValidation =
        new ClassMatchedHandler<LeaderValid, KContentMsg<?, KHeader<?>, LeaderValid>>() {
            @Override
            public void handle(LeaderValid content, KContentMsg<?, KHeader<?>, LeaderValid> container) {
                LOG.info("handleLeaderValidation");
                if (localNewsView == null)
                    return;
                
                if (!content.toLeader) {
                    if(viewComparator.compare(content.getLeaderView(), localNewsView) < 0){
                        KHeader header = new BasicHeader(selfAdr, content.getAddress(), Transport.UDP);
                        content.toLeader = true;
                        content.newBranch = false;
                        KContentMsg msg = new BasicContentMsg(header, content);
                        trigger(msg, networkPort);
                    }
                    List<KAddress> tempToSend = new ArrayList<>();
                    KAddress leaderAdress = content.getAddress();
                    for (Container c : gradientNeighbours) {
                        if (!content.isInVerified((View) c.getContent())) {
                            content.addVerified((View) c.getContent());
                            tempToSend.add((KAddress) c.getSource());

                            KHeader header = new BasicHeader(selfAdr, content.getAddress(), Transport.UDP);
                            content.toLeader = true;
                            content.newBranch = true;
                            content.setAddress(selfAdr);
                            KContentMsg msg = new BasicContentMsg(header, content);
                            trigger(msg, networkPort);


                            header = new BasicHeader(selfAdr, (KAddress) c.getSource(), Transport.UDP);
                            content.toLeader = false;
                            content.setAddress(leaderAdress);
                            msg = new BasicContentMsg(header, content);
                            trigger(msg, networkPort);
                        }
                    }

                    content.toLeader = false;
                    for (KAddress adr : tempToSend) {
                        KHeader header = new BasicHeader(selfAdr, adr, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, content);
                        trigger(msg, networkPort);
                    }
                }
                else{
                    if(content.newBranch){
                        verifInProgress.add(content.getAddress());
                    }
                    else{
                        if(verifInProgress.contains(content.getAddress())){
                            verifInProgress.remove(content.getAddress());
                        }
                    }
                    if(verifInProgress.isEmpty()){
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
