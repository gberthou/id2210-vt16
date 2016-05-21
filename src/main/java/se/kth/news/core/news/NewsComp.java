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

import java.util.HashMap;
import se.kth.news.sim.ScenarioGen;

import java.util.Iterator;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.play.Ping;
import se.kth.news.play.Pong;
import se.kth.news.play.NewsFlood;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
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
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import java.util.UUID;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NewsComp extends ComponentDefinition {

    private static int NewsIntID = 0;
    
    private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
    private String logPrefix = " ";

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
            
            /*
            Iterator<Identifier> it = castSample.publicSample.keySet().iterator();
            KAddress partner = castSample.publicSample.get(it.next()).getSource();
            KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
            KContentMsg msg = new BasicContentMsg(header, new Ping());
            trigger(msg, networkPort);
            */
            
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
        }
    };

    Handler handleLeader = new Handler<LeaderUpdate>() {
        @Override
        public void handle(LeaderUpdate event) {
        }
    };
    
    ClassMatchedHandler handleNewsFlood
            = new ClassMatchedHandler<NewsFlood, KContentMsg<?, KHeader<?>, NewsFlood>>() {

                @Override
                public void handle(NewsFlood content, KContentMsg<?, KHeader<?>, NewsFlood> container) {
                    LOG.info("{}received newsflood from:{} ({})", logPrefix, container.getHeader().getSource(), content.GetMessage());
                    
                    boolean unknown = !knownNews.containsKey(content.GetMessage());
                    if(unknown) { // The news was unknown until now
                        GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                        String fieldInfectedNodes = "simulation.infectedNodesForNews" + content.GetMessage();

                        Integer infectedNodes = gv.getValue(fieldInfectedNodes, Integer.class) + 1;
                        gv.setValue(fieldInfectedNodes, infectedNodes);
                        
                        // Record news
                        knownNews.put(content.GetMessage(), content.GetTTL());
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
