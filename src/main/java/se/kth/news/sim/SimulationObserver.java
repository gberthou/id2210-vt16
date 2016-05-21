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

    public SimulationObserver(Init init) {
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
            Integer sumInfected = 0;
            Integer sumMessages = 0;
            
            LOG.info("\n###");
            for(int i = 0; i < NEWS_MAXCOUNT; ++i)
            {
                Integer infected = gv.getValue("simulation.infectedNodesForNews" + i, Integer.class);
                Integer msgCount = gv.getValue("simulation.messageCountForNews" + i, Integer.class);
                
                sumInfected += infected;
                sumMessages += msgCount;
                
                LOG.info("News {}: {} infected", i, infected);
                LOG.info("         {} messages", msgCount);
            }
            
            float avgInfected = sumInfected / (float)NEWS_MAXCOUNT;
            float avgMessages = sumMessages/ (float)NEWS_MAXCOUNT;
            LOG.info("Avg infected: {}", avgInfected);
            LOG.info("Avg messages: {}", avgMessages);
            
            LOG.info("\n###");
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