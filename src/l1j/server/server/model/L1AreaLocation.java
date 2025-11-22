/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 * 02111-1307, USA.
 *
 * http://www.gnu.org/copyleft/gpl.html
 */
package l1j.server.server.model;

import java.util.HashMap;
import java.util.Map;

public class L1AreaLocation {

    public static final int MAGE_QUEST_30_MAP_ID = 201;

    private static final L1MapArea MageQuest30MpRegenArea = new L1MapArea(32848, 32906, 
            32881, 32939, MAGE_QUEST_30_MAP_ID);

    private static final Map<Integer, L1MapArea> _areas = new HashMap<Integer, L1MapArea>();

    static {
        _areas.put(MAGE_QUEST_30_MAP_ID, MageQuest30MpRegenArea);
    }

    private L1AreaLocation() {
    }   

    public static boolean isInArea(L1Location loc) {
        for (Map.Entry<Integer, L1MapArea> entry : _areas.entrySet()) {
            if (entry.getValue().contains(loc)) {
                return true;
            }
        }
        
        return false;
    }
}
