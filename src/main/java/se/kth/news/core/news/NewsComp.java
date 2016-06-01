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

import se.kth.news.core.leader.*;
import se.kth.news.core.news.util.NewsViewComparator;
import se.kth.news.sim.ScenarioGen;

import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import se.sics.ktoolbox.util.other.Container;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import java.util.UUID;
import se.kth.news.play.NewsFloodGradient;
import se.kth.news.play.NewsSummary;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NewsComp extends ComponentDefinition {

    private static int STABLEROUND = 4;
    private static int ROUNDTOCHECKLEADER = 10;
    private static int NewsIntID = 0;
    public static KAddress CURRENTLEADER;
    
    private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
    private String logPrefix = " ";

    //*******************************CONNECTIONS********************************
    Positive<Timer> timerPort = requires(Timer.class);
    Positive<Network> networkPort = requires(Network.class);
    Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
    Positive<GradientPort> gradientPort = requires(GradientPort.class);
    Positive<LeaderSelectPort> leaderPort = requires(LeaderSelectPort.class);
    Positive<LeaderFailPort> leaderFailPort = requires(LeaderFailPort.class);
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
    private final int ISSUE_NEWS_PERIOD = 10000;
    private SchedulePeriodicTimeout sptIssueNews = new SchedulePeriodicTimeout(ISSUE_NEWS_PERIOD, ISSUE_NEWS_PERIOD);
    private UUID timerIssueNewsId;
    private int issuedNews;

    private List<Container> stableGradientSample = new ArrayList<>();
    private List<Container> stableFingerSample = new ArrayList<>();
    private int roundsToStability = STABLEROUND;
    private boolean leader = false;
    private KAddress adrLeader;
    
    private final int LEADER_NEWS_DISSEMINATION_PERIOD = 20000;
    private SchedulePeriodicTimeout sptLeaderNews = new SchedulePeriodicTimeout(LEADER_NEWS_DISSEMINATION_PERIOD, LEADER_NEWS_DISSEMINATION_PERIOD);
    private UUID timerLeaderNewsId;
    private TreeSet<Integer> knownSummaries = new TreeSet<>();
    
    private boolean networkHasLeader = false;
    private int issuedNewsAsLeader = 0;

    private int leaderFailIn = ROUNDTOCHECKLEADER;

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
        subscribe(handleNewsFloodGradient, networkPort);
        subscribe(handleNewsSummary, networkPort);
        subscribe(handleLeaderFail, networkPort);
        subscribe(handleCheck, timerPort);
        
        intID = NewsIntID++;
        nodesSample = null;
        knownNews = new HashMap<>();
        
        issuedNews = 0;
        if(intID == 1){
            CURRENTLEADER = selfAdr;
        }
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
        LOG.info("{}starting...", logPrefix);
        updateLocalNewsView();

        schedulePeriodicCheck();
        }
    };
    
    @Override
    public void tearDown() {
        trigger(new CancelPeriodicTimeout(timerIssueNewsId), timerPort);
        trigger(new CancelPeriodicTimeout(timerLeaderNewsId), timerPort);
    }

    Handler<CheckTimeout> handleCheck = new Handler<CheckTimeout>() {
        @Override
        public void handle(CheckTimeout event) {
            if(event.spt == sptIssueNews
            && ((networkHasLeader && leader) || (!networkHasLeader && intID == 1))) {
                // Create a new news and make it propagate

                if(!networkHasLeader) { // When there is no leader, node 1 send the news
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
                } else if(stableGradientSample != null){ // When there is a leader, the leader sends the news
                    if(nodesSample != null && issuedNewsAsLeader < ScenarioGen.NEWS_MAXCOUNT) {
                        NewsFloodGradient nf = new NewsFloodGradient();
                        knownNews.put(nf.GetMessage(), 0);
                        issuedNewsAsLeader++;

                        GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                        String fieldName = "simulation.infectedNodesForNewsGradient" + nf.GetMessage();
                        gv.setValue(fieldName, 1); // The node that issues the news actually knows it
                        
                        ArrayList<KAddress> mottagare = new ArrayList<>();
                        for(Container c : stableGradientSample)
                            mottagare.add((KAddress) c.getSource());
                        for(Container c : stableFingerSample)
                            mottagare.add((KAddress) c.getSource());
                        
                        for(KAddress partner : mottagare) {
                            
                            KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                            KContentMsg msg = new BasicContentMsg(header, nf);
                            trigger(msg, networkPort);
                        }
                    }
                }
            } else if(event.spt == sptLeaderNews && leader) {
                GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                gv.setValue("simulation.roundCountForNewsSummary" + NewsSummary.NewsSummaryID, 1);
                
                // When the current node is sure it is the leader, it notifies its neighbours
                // of its news
                for(Container c : stableGradientSample) {
                    KHeader header = new BasicHeader(selfAdr, (KAddress) c.getSource(), Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, new NewsSummary(knownNews.size(), NewsSummary.NewsSummaryID));
                    trigger(msg, networkPort);
                }
                NewsSummary.NewsSummaryID++;
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
                if(key >= NewsFloodGradient.NEWSFLOOD_GRADIENT_BEGIN)
                    continue;
                
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
        if(NewsFlood.NewsFlood_msg < ScenarioGen.NEWS_MAXCOUNT)
            return;

        GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
        String fieldName = "simulation.roundCountForGradientStabilisation";
        gv.setValue(fieldName, gv.getValue(fieldName, Integer.class) + 1);

        List<Container> tempG = stableGradientSample;
        List<Container> tempF = stableFingerSample;
        stableGradientSample.clear();
        stableFingerSample.clear();
        boolean stable = true;
        Container container;
        for (Object o : sample.gradientNeighbours) {
            container = (Container) o;
            stableGradientSample.add(container);
            if (!tempG.contains(container)) {
                stable = false;
            }
        }
        for (Object o : sample.gradientFingers) {
            container = (Container) o;
            stableFingerSample.add(container);
            if (!tempF.contains(container)) {
                stable = false;
            }
        }
        if(!stable){
            gv.setValue(fieldName, 0);
            roundsToStability = STABLEROUND;
        }
        else{
            if(roundsToStability>=0)
                roundsToStability--;
        }
        if(roundsToStability == 0){
            LOG.info("STABLE:" + gv.getValue(fieldName, Integer.class));
            LeaderUpdate lU = new LeaderUpdate(leader, selfAdr);
            trigger(lU, leaderPort);
        }

        if(adrLeader != null){
            if(--leaderFailIn <= 0){
                sendLeaderFail(selfAdr, true, adrLeader);
            }
            if(leaderFailIn <= 10){
                LeaderFail lF = new LeaderFail();
                trigger(lF, leaderFailPort);
            }
        }
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
        boolean wasLeader = leader;
        if(event.leaderAdr != selfAdr){
            adrLeader = event.leaderAdr;
        }
        leader = event.leader;
        if(wasLeader)
            return;

        if(leader) {
            LOG.info("And the Leader is {}", event.leaderAdr);
            networkHasLeader = true;
            CURRENTLEADER = selfAdr;
        }
        }
    };



    ClassMatchedHandler handleLeaderFail =
            new ClassMatchedHandler<LeaderDetectFail, KContentMsg<?, KHeader<?>, LeaderDetectFail>>() {
                @Override
                public void handle(LeaderDetectFail content, KContentMsg<?, KHeader<?>, LeaderDetectFail> container) {
                    if(content.getAddress() == null){
                        adrLeader = null;
                        return;
                    }
                    if(leader){
                        sendLeaderFail(selfAdr, false, content.getAddress());
                    }
                    else{
                        if(content.leader){
                            sendLeaderFail(null, false, content.getAddress());
                        }
                        else {
                            leaderFailIn = ROUNDTOCHECKLEADER;
                        }
                    }
                }
            };

    private void sendLeaderFail(KAddress ka, boolean l, KAddress dst){
        LeaderDetectFail lF = new LeaderDetectFail(ka);
        lF.leader = l;
        KHeader header = new BasicHeader(selfAdr, dst, Transport.UDP);
        KContentMsg msg = new BasicContentMsg(header, lF);
        trigger(msg, networkPort);
    }

    
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
                updateLocalNewsView();

            }
        }
    };
    
    ClassMatchedHandler handleNewsFloodGradient =
    new ClassMatchedHandler<NewsFloodGradient, KContentMsg<?, KHeader<?>, NewsFloodGradient>>() {

        @Override
        public void handle(NewsFloodGradient content, KContentMsg<?, KHeader<?>, NewsFloodGradient> container) {
            GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
            boolean writeToGlobalView = content.GetMessage() < NewsFloodGradient.NEWSFLOOD_GRADIENT_BEGIN + ScenarioGen.NEWS_MAXCOUNT;
            
            if(writeToGlobalView) {
                String fieldMessages = "simulation.messageCountForNewsGradient" + content.GetMessage();
                Integer messageCount = gv.getValue(fieldMessages, Integer.class) + 1;
                gv.setValue(fieldMessages, messageCount);
            }
            
            boolean unknown = !knownNews.containsKey(content.GetMessage());
            if(unknown) { // The news was unknown until now
                if(writeToGlobalView) {
                    String fieldInfectedNodes = "simulation.infectedNodesForNewsGradient" + content.GetMessage();
                    Integer infectedNodes = gv.getValue(fieldInfectedNodes, Integer.class) + 1;
                    gv.setValue(fieldInfectedNodes, infectedNodes);

                    Integer totalKnownNews = gv.getValue("simulation.totalKnownNewsGradient", Integer.class) + 1;
                    gv.setValue("simulation.totalKnownNewsGradient", totalKnownNews);
                }

                // Record news
                knownNews.put(content.GetMessage(), 0);
                updateLocalNewsView();
                
                // Transmit news to neighbours
                ArrayList<KAddress> mottagare = new ArrayList<>();
                for(Container c : stableGradientSample) {
                    KAddress partner = (KAddress)c.getSource();
                    if(new NewsViewComparator().compare((NewsView) c.getContent(), localNewsView) < 0
                    && !mottagare.contains(partner)
                    && !container.getSource().equals(partner)) {
                        mottagare.add(partner);
                    }
                }
                for(Container c : stableFingerSample) {
                    KAddress partner = (KAddress)c.getSource();
                    if(new NewsViewComparator().compare((NewsView) c.getContent(), localNewsView) < 0
                    && !mottagare.contains(partner)
                    && !container.getSource().equals(partner)) {
                        mottagare.add(partner);
                    }
                }
                
                for(KAddress partner : mottagare) {
                    KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, content);
                    trigger(msg, networkPort);
                }
            }
        }
    };
    
    ClassMatchedHandler handleNewsSummary =
    new ClassMatchedHandler<NewsSummary, KContentMsg<?, KHeader<?>, NewsSummary>>() {

        @Override
        public void handle(NewsSummary content, KContentMsg<?, KHeader<?>, NewsSummary> container) {
            //LOG.info("{}received newssummary from:{} ({})", logPrefix, container.getHeader().getSource(), content.GetNewsCount());
            
            networkHasLeader = true;
            if(stableGradientSample != null
            && !leader) {
                if(!knownSummaries.contains(content.GetId())) {
                    knownSummaries.add(content.GetId());
                
                    // Send to all neighbours
                    ArrayList<KAddress> mottagare = new ArrayList<>();
                    for(Container c : stableGradientSample) {
                        if(new NewsViewComparator().compare((NewsView) c.getContent(), localNewsView) < 0
                        && !mottagare.contains((KAddress)c.getSource())) {
                            mottagare.add((KAddress) c.getSource());
                        }
                    }
                    
                    for(Container c : stableFingerSample) {
                        if(new NewsViewComparator().compare((NewsView) c.getContent(), localNewsView) < 0
                        && !mottagare.contains((KAddress)c.getSource())) {
                            mottagare.add((KAddress) c.getSource());
                        }
                    }
                    for(KAddress partner : mottagare) {
                        KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                        KContentMsg msg = new BasicContentMsg(header, content);
                        trigger(msg, networkPort);
                    }

                    if(mottagare.size() > 0) {
                        // Update globalview (add a round to the current leader)
                        GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                        String fieldName = "simulation.roundCountForNewsSummary" + content.GetId();
                        gv.setValue(fieldName, gv.getValue(fieldName, Integer.class) + 1);
                    }
                }
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
        CheckTimeout timeoutIssueNews = new CheckTimeout(sptIssueNews);
        sptIssueNews.setTimeoutEvent(timeoutIssueNews);
        trigger(sptIssueNews, timerPort);
        timerIssueNewsId = timeoutIssueNews.getTimeoutId();

        CheckTimeout timeoutLeaderNews = new CheckTimeout(sptLeaderNews);
        sptLeaderNews.setTimeoutEvent(timeoutLeaderNews);
        trigger(sptLeaderNews, timerPort);
        timerLeaderNewsId = timeoutLeaderNews.getTimeoutId();
    }

    public static class CheckTimeout extends Timeout {
        public final SchedulePeriodicTimeout spt;

        public CheckTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
            this.spt = spt;
        }
    }
}
