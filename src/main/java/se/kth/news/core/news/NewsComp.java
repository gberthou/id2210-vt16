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
package se.kth.news.core.news;

import java.util.ArrayList;
import java.util.HashMap;

import se.kth.news.core.news.util.NewsViewComparator;
import se.kth.news.sim.ScenarioGen;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.play.NewsFlood;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.AgingAdrContainer;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import java.util.UUID;
import se.kth.news.play.NewsSummary;
import se.sics.ktoolbox.gradient.util.GradientContainer;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NewsComp extends ComponentDefinition {

    private static int STABLEROUND = 4;
    private static int NewsIntID = 0;
    
    private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
    private String logPrefix = " ";

    private NewsViewComparator comparator = new NewsViewComparator();

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeaderSelectPort> leaderPort = requires(LeaderSelectPort.class);
    Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
    //*******************************EXTERNAL_STATE*****************************
    private KAddress selfAdr;
    private Identifier gradientOId;
    //*******************************INTERNAL_STATE*****************************
    private NewsView localNewsView;
    private int intID;
    private CroupierSample<NewsView> nodesSample;
    private HashMap<Integer, Integer> knownNews; // key: news id/content, content: ttl
    
    // The following attributes are only related to the node that will issue the news 
    private UUID timerId;
    private int issuedNews;

    private List<KAddress> stableGradientSample = new ArrayList<>();
    private int roundsToStability = STABLEROUND;
    private boolean leader = true;

    private int maxNewsCountFromLeader = 0;

    public NewsComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        gradientOId = init.gradientOId;

        subscribe(handleStart, control);
        subscribe(handleCroupierSample, croupierPort);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleLeader, leaderPort);
        subscribe(handleNewsFlood, networkPort);
        subscribe(handleNewsSummary, networkPort);
        subscribe(handleCheck, timerPort);
        
        intID = NewsIntID++;
        nodesSample = null;
        knownNews = new HashMap<>();
        
        issuedNews = 0;
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            updateLocalNewsView();
            
            if(intID == 1) { // ID of the node that will initiate the news
                schedulePeriodicCheck();
            }
        }
    };
    
    @Override
    public void tearDown() {
        if(intID == 1) {
            trigger(new CancelPeriodicTimeout(timerId), timerPort);
        }
        
    }

    Handler<CheckTimeout> handleCheck = new Handler<CheckTimeout>() {
        @Override
        public void handle(CheckTimeout event) {
            // Create a new news and make it propagate
            if(nodesSample != null && issuedNews < ScenarioGen.NEWS_MAXCOUNT) {
                NewsFlood nf = new NewsFlood();
                knownNews.put(nf.GetMessage(), nf.GetTTL());
                issuedNews++;
                
                GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                String fieldName = "simulation.infectedNodesForNews" + nf.GetMessage();
                gv.setValue(fieldName, 1); // The node that issues the news actually knows it

                for(Identifier id : nodesSample.publicSample.keySet()) {
                    KAddress partner = nodesSample.publicSample.get(id).getSource();
                    KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, nf);
                    trigger(msg, networkPort);
                }
            }
        }
    };

    private void updateLocalNewsView() {
        localNewsView = new NewsView(selfAdr.getId(), knownNews.size());
        LOG.debug("{}informing overlays of new view", logPrefix);
        trigger(new OverlayViewUpdate.Indication<>(gradientOId, false, localNewsView.copy()), viewUpdatePort);
    }

    Handler handleCroupierSample = new Handler<CroupierSample<NewsView>>() {
        @Override
        public void handle(CroupierSample<NewsView> castSample) {
            if (castSample.publicSample.isEmpty()) {
                return;
            }

            nodesSample = castSample;
            if(nodesSample == null)
                return;
            
            for(Integer key : knownNews.keySet()) {
                Integer ttl = knownNews.get(key);
                int msgCount = 0;
                if(ttl > 0) { // Propagate
                    NewsFlood nf = new NewsFlood(ttl - 1, key);

                    for(Identifier id : nodesSample.publicSample.keySet()) {
                        KAddress partner = nodesSample.publicSample.get(id).getSource();
                        KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, nf);
                        trigger(msg, networkPort);
                        ++msgCount;
                    }
                    
                    // Decrement ttl
                    knownNews.put(key, ttl - 1);
                }
                
                // Send notificaton to the global view
                GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                String fieldMessageCount = "simulation.messageCountForNews" + key;

                Integer messageCount = gv.getValue(fieldMessageCount, Integer.class) + msgCount;
                gv.setValue(fieldMessageCount, messageCount);
            }
        }
    };

    Handler handleGradientSample = new Handler<TGradientSample>() {
        @Override
        public void handle(TGradientSample sample) {
            if(roundsToStability != 0) {
                List<KAddress> temp = new ArrayList<>();
                for (KAddress adr : stableGradientSample) {
                    temp.add(adr);
                }
                stableGradientSample.clear();
                boolean stable = true;
                int minRank = Integer.MAX_VALUE;
                for (Object o : sample.gradientFingers) {
                    GradientContainer container = (GradientContainer) o;
                    stableGradientSample.add(container.getSource());
                    if (!temp.contains(container.getSource())) {
                        stable = false;
                        break;
                    }
                    if (comparator.compare((NewsView) container.getContent(), localNewsView) >= 0) {
                        leader = false;
                    }
                }
                if (stable && --roundsToStability == 0) {
                    if (leader) {
                        for (AgingAdrContainer<KAddress, NewsView> val : nodesSample.publicSample.values()) {
                            LeaderUpdate lU = new LeaderUpdate(selfAdr, localNewsView);
                            KAddress partner = val.getSource();
                            KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, new NewsFlood());
                            trigger(msg, leaderPort);
                        }
                    }
                }
            }

                    if(leader) { // Let's say that leader has id 0
                        /* TODO: Move this (task 3.1) */
                        // When the current node is sure it is the leader, it notifies its neighbours
                        // of its news
                        int msgCount = 0;
                        for(KAddress address: stableGradientSample) {
                            KHeader header = new BasicHeader(selfAdr, address, Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, new NewsSummary(knownNews.size()));
                            trigger(msg, networkPort);
                            ++msgCount;
                        }
                    }
                }

    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {

            LeaderUpdate lU = new LeaderUpdate(event.leaderAdr, event.view);
            if(leader){
                //maxNewsCountFromLeader++;
                if(comparator.compare(localNewsView, event.view) > 0) {
                    lU = new LeaderUpdate(selfAdr, localNewsView);
                    KAddress partner = event.leaderAdr;
                    KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, lU);
                    trigger(msg, leaderPort);
                }
                else{
                    leader = false;
                }
            }
            for (AgingAdrContainer<KAddress, NewsView> val : nodesSample.publicSample.values()) {
                KAddress partner = val.getSource();
                KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                KContentMsg msg = new BasicContentMsg(header, lU);
                trigger(msg, leaderPort);
            }
        }
    };


    
    ClassMatchedHandler handleNewsFlood =
    new ClassMatchedHandler<NewsFlood, KContentMsg<?, KHeader<?>, NewsFlood>>() {

        @Override
        public void handle(NewsFlood content, KContentMsg<?, KHeader<?>, NewsFlood> container) {
            //LOG.info("{}received newsflood from:{} ({})", logPrefix, container.getHeader().getSource(), content.GetMessage());

            boolean unknown = !knownNews.containsKey(content.GetMessage());
            if(unknown) { // The news was unknown until now
                GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                String fieldInfectedNodes = "simulation.infectedNodesForNews" + content.GetMessage();

                Integer infectedNodes = gv.getValue(fieldInfectedNodes, Integer.class) + 1;
                gv.setValue(fieldInfectedNodes, infectedNodes);
                
                Integer totalKnownNews = gv.getValue("simulation.totalKnownNews", Integer.class) + 1;
                gv.setValue("simulation.totalKnownNews", totalKnownNews);

                // Record news
                knownNews.put(content.GetMessage(), content.GetTTL());
            }
        }
    };
    
    ClassMatchedHandler handleNewsSummary =
    new ClassMatchedHandler<NewsSummary, KContentMsg<?, KHeader<?>, NewsSummary>>() {

        @Override
        public void handle(NewsSummary content, KContentMsg<?, KHeader<?>, NewsSummary> container) {
            LOG.info("{}received newssummary from:{} ({})", logPrefix, container.getHeader().getSource(), content.GetNewsCount());
            
            if(stableGradientSample != null
            && !leader
            && content.GetNewsCount() > maxNewsCountFromLeader) { // Check if this news notification isn't already known
                int msgCount = 0;
                
                // Send to all neighbours
                for(KAddress address: stableGradientSample) {
                    KHeader header = new BasicHeader(selfAdr, address, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, content);
                    trigger(msg, networkPort);
                    ++msgCount;
                }
                
                // Update internal data
                maxNewsCountFromLeader = content.GetNewsCount();
            }
        }
    };

    public static class Init extends se.sics.kompics.Init<NewsComp> {

        public final KAddress selfAdr;
        public final Identifier gradientOId;

        public Init(KAddress selfAdr, Identifier gradientOId) {
            this.selfAdr = selfAdr;
            this.gradientOId = gradientOId;
        }
    }
    
    private void schedulePeriodicCheck() {
        final int PERIOD = 10000;
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(PERIOD, PERIOD);
        CheckTimeout timeout = new CheckTimeout(spt);
        spt.setTimeoutEvent(timeout);
        trigger(spt, timerPort);
        timerId = timeout.getTimeoutId();
    }

    public static class CheckTimeout extends Timeout {

        public CheckTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
