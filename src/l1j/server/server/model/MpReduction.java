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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import l1j.server.server.model.Instance.L1PcInstance;

public class MpReduction implements Runnable {

	private static Logger _log = LoggerFactory.getLogger(MpReduction.class
			.getName());
	private final int _curPoint = 4;
	private final L1PcInstance _pc;
	private int _regenPoint = 0;

    public MpReduction(L1PcInstance pc) {
        _pc = pc;
    }

    public void decreaseMp() {
		int baseMpr = 0;
		int wis = _pc.getWis();

        if (wis < 15) {
            baseMpr = 2;
        } else if (wis < 17) {
            baseMpr = 1;
        }

		baseMpr += 3 - _pc.getOriginalMpr();
		
		int mpPluss = 18 - _pc.getWis();
		if (mpPluss > 6) {
			mpPluss = 6;
		}
		int mpr = baseMpr + mpPluss;
		int newMp = _pc.getCurrentMp() - mpr;
		if (newMp < 0) {
			newMp = 0;
		}
		_pc.setCurrentMp(newMp);
	}

	@Override
	public void run() {
		try {
			if (_pc.isDead()) {
				return;
			}

			_regenPoint += _curPoint;

			if (64 <= _regenPoint) {
				_regenPoint = 0;
				decreaseMp();
			}
		} catch (Throwable e) {
			_log.warn(e.getLocalizedMessage(), e);
		}
	}

}
