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
package se.kth.news.sim;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.play.NewsFloodGradient;
import se.kth.news.play.NewsSummary;
import static se.kth.news.sim.ScenarioGen.NEWS_MAXCOUNT;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

public class SimulationObserver extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationObserver.class);
    
    Positive<Timer> timer = requires(Timer.class);
    Positive<Network> network = requires(Network.class);

    private UUID timerId;
    private int round;

    public SimulationObserver(Init init) {
        round = 0;
        
        subscribe(handleStart, control);
        subscribe(handleCheck, timer);
    }

    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            schedulePeriodicCheck();
        }
    };

    @Override
    public void tearDown() {
        trigger(new CancelPeriodicTimeout(timerId), timer);
    }

    Handler<CheckTimeout> handleCheck = new Handler<CheckTimeout>() {
        @Override
        public void handle(CheckTimeout event) {
            GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
            
            ++round;
            
            LOG.info("### T1 ({})", round);
            Integer sumInfected = 0;
            Integer sumMessages = 0;
            for(int i = 0; i < NEWS_MAXCOUNT; ++i)
            {
                Integer infected = gv.getValue("simulation.infectedNodesForNews" + i, Integer.class);
                Integer msgCount = gv.getValue("simulation.messageCountForNews" + i, Integer.class);
                
                sumInfected += infected;
                sumMessages += msgCount;
            }
            
            Integer totalKnownNews = gv.getValue("simulation.totalKnownNews", Integer.class);
            
            float avgInfected = sumInfected / (float)NEWS_MAXCOUNT;
            float avgMessages = sumMessages/ (float)NEWS_MAXCOUNT;
            float avgTotalKnownNews = totalKnownNews / (float) ScenarioGen.NETWORK_SIZE;
            
            LOG.info("Avg infected: {}/{}", avgInfected, ScenarioGen.NETWORK_SIZE);
            LOG.info("Avg messages: {}", avgMessages);
            LOG.info("Avg knowledge: {} news per node", avgTotalKnownNews);
            
            LOG.info("### /T1\n");

            LOG.info("### T2 ");
            LOG.info("Message Count:" + gv.getValue("simulation.roundCountForLeaderElection", Integer.class));
            LOG.info("Round Count:" + gv.getValue("simulation.roundCountForGradientStabilisation", Integer.class)+"\n");
            
            LOG.info("### T3 ({})", round);
            Integer maxRounds = 0;
            Integer sumRounds = 0;
            
            int summariesToAggregate = Math.min(NewsSummary.NewsSummaryID, 10);
            for(int i = 0; i < summariesToAggregate; ++i) {
                Integer rounds = gv.getValue("simulation.roundCountForNewsSummary" + i, Integer.class);
                //LOG.info("-> {}", rounds);
                if(rounds > maxRounds)
                    maxRounds = rounds;
                sumRounds += rounds;
            }
            
            float avgRounds = sumRounds / (float) summariesToAggregate;
            LOG.info("Avg rounds: {}", avgRounds);
            LOG.info("Max rounds: {}", maxRounds);
            
            Integer sumInfectedGradient = 0;
            Integer sumMessagesGradient = 0;
            for(int i = 0; i < NEWS_MAXCOUNT; ++i)
            {
                Integer infected = gv.getValue("simulation.infectedNodesForNewsGradient" + (NewsFloodGradient.NEWSFLOOD_GRADIENT_BEGIN + i), Integer.class);
                Integer msgCount = gv.getValue("simulation.messageCountForNewsGradient" + (NewsFloodGradient.NEWSFLOOD_GRADIENT_BEGIN + i), Integer.class);
                
                sumInfectedGradient += infected;
                sumMessagesGradient += msgCount;
            }
            
            Integer totalKnownNewsGradient = gv.getValue("simulation.totalKnownNewsGradient", Integer.class);
            
            float avgInfectedGradient = sumInfectedGradient / (float)NEWS_MAXCOUNT;
            float avgMessagesGradient = sumMessagesGradient / (float)NEWS_MAXCOUNT;
            float avgTotalKnownNewsGradient = totalKnownNewsGradient / (float) ScenarioGen.NETWORK_SIZE;
            
            LOG.info("");
            LOG.info("Avg infected: {}/{}", avgInfectedGradient, ScenarioGen.NETWORK_SIZE);
            LOG.info("Avg messages: {}", avgMessagesGradient);
            LOG.info("Avg knowledge: {} news per node", avgTotalKnownNewsGradient);
            
            LOG.info("### /T3\n");
        }
    };

    public static class Init extends se.sics.kompics.Init<SimulationObserver> {

        public Init() {
        }
    }

    private void schedulePeriodicCheck() {
        final long PERIOD = 500;
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(PERIOD, PERIOD);
        CheckTimeout timeout = new CheckTimeout(spt);
        spt.setTimeoutEvent(timeout);
        trigger(spt, timer);
        timerId = timeout.getTimeoutId();
    }

    public static class CheckTimeout extends Timeout {

        public CheckTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}