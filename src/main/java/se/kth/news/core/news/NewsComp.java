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
    private TreeSet<Integer> knownNews; // key: news id/content

    public NewsComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        gradientOId = init.gradientOId;

        subscribe(handleStart, control);
        subscribe(handleCroupierSample, croupierPort);
        subscribe(handleGradientSample, gradientPort);
        subscribe(handleLeader, leaderPort);
        subscribe(handlePing, networkPort);
        subscribe(handlePong, networkPort);
        subscribe(handleNewsFlood, networkPort);
        
        intID = NewsIntID++;
        nodesSample = null;
        knownNews = new TreeSet<>();
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            updateLocalNewsView();
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
            
            if(nodesSample == null && intID == ScenarioGen.NETWORK_SIZE-1) { // ID of the infected node
                NewsFlood nf = new NewsFlood();
                knownNews.add(nf.GetMessage());
                
                for(Identifier id : castSample.publicSample.keySet()) {
                    KAddress partner = castSample.publicSample.get(id).getSource();
                    KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                    KContentMsg msg = new BasicContentMsg(header, nf);
                    trigger(msg, networkPort);
                }
            }
            
            nodesSample = castSample;
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

    ClassMatchedHandler handlePing
            = new ClassMatchedHandler<Ping, KContentMsg<?, ?, Ping>>() {

                @Override
                public void handle(Ping content, KContentMsg<?, ?, Ping> container) {
                    //LOG.info("{}received ping from:{}", logPrefix, container.getHeader().getSource());
                    trigger(container.answer(new Pong()), networkPort);
                }
            };

    ClassMatchedHandler handlePong
            = new ClassMatchedHandler<Pong, KContentMsg<?, KHeader<?>, Pong>>() {

                @Override
                public void handle(Pong content, KContentMsg<?, KHeader<?>, Pong> container) {
                    //LOG.info("{}received pong from:{}", logPrefix, container.getHeader().getSource());
                }
            };
    
    ClassMatchedHandler handleNewsFlood
            = new ClassMatchedHandler<NewsFlood, KContentMsg<?, KHeader<?>, NewsFlood>>() {

                @Override
                public void handle(NewsFlood content, KContentMsg<?, KHeader<?>, NewsFlood> container) {
                    LOG.info("{}received newsflood from:{} ({})", logPrefix, container.getHeader().getSource(), content.GetMessage());
                    
                    if(knownNews.add(content.GetMessage())){ // The news was unknown until now
                        int ttl = content.GetTTL();
                        if(ttl > 0) { // Propagate
                            NewsFlood nf = new NewsFlood(ttl - 1, content.GetMessage());

                            if(nodesSample != null) {
                                for(Identifier id : nodesSample.publicSample.keySet()) {
                                    KAddress partner = nodesSample.publicSample.get(id).getSource();
                                    KHeader header = new BasicHeader(selfAdr, partner, Transport.UDP);
                                    KContentMsg msg = new BasicContentMsg(header, nf);
                                    trigger(msg, networkPort);
                                }
                            }
                        }
                        
                        // Inform the global view
                        GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
                        Integer infectedNodes = gv.getValue("simulation.infectedNodes", Integer.class) + 1;
                        gv.setValue("simulation.infectedNodes", infectedNodes);
                        
                        LOG.info("Total infected nodes: {}", infectedNodes);
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
}
