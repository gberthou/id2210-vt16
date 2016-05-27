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
package se.kth.news.play;

/**
 *
 * @author gautier
 */
public class NewsFloodGradient {
    public static final int NEWSFLOOD_GRADIENT_BEGIN = 1000;
    private static int NewsFlood_msg = NEWSFLOOD_GRADIENT_BEGIN;
    private final int message;
    
    public NewsFloodGradient() {
        message = NewsFlood_msg++;
    }
    
    public NewsFloodGradient(int msg) {
        message = msg;
    }
    
    public int GetMessage() {
        return message;
    }
}
