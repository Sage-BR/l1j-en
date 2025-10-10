/**
 * GPLv2. See License.
 */
package l1j.server.server.model;

import java.util.List;

import l1j.server.server.datatables.HouseTable;
import l1j.server.server.datatables.NpcSpawnTable;
import l1j.server.server.templates.L1House;
import l1j.server.server.types.Point;

public class L1HouseLocation {

	private static final int[] TELEPORT_LOC_MAPID = { 4, 4, 4, 350, };

	private static final Point[] TELEPORT_LOC_GIRAN = {
			new Point(33419, 32810), new Point(33343, 32723), // Warehousing and
																// storage depot
																// pets
			new Point(33553, 32712), new Point(32702, 32842), }; // Atonement
																	// envoy,
																	// Ilan
																	// market

	private static final Point[] TELEPORT_LOC_HEINE = {
			new Point(33604, 33236), new Point(33649, 33413), // Warehousing and
																// storage depot
																// pets
			new Point(33553, 32712), new Point(32702, 32842), }; // Atonement
																	// envoy,
																	// Ilan
																	// market

	private static final Point[] TELEPORT_LOC_ADEN = { new Point(33966, 33253),
			new Point(33921, 33177), // Warehousing and storage depot pets
			new Point(33553, 32712), new Point(32702, 32842), }; // Atonement
																	// envoy,
																	// Ilan
																	// market

	private static final Point[] TELEPORT_LOC_GLUDIN = {
			new Point(32628, 32807), new Point(32623, 32729),
			new Point(33553, 32712), new Point(32702, 32842), };

	private static final List<Integer> _houseIds = HouseTable.getHouseIdList();

	private static final int ADEN = 4;

	private L1HouseLocation() {
	}

	private static boolean inBox(int x, int y, int minX, int maxX, int minY,
			int maxY) {
		return x >= minX && x <= maxX && y >= minY && y <= maxY;
	}

	/**
	 * Loc given a specified number of coordinates determine the safe house.
	 * 
	 * @param houseId
	 * @param cha
	 * @return
	 */
	public static boolean isInHouseLoc(int houseId, int x, int y, short mapid) {
		switch (houseId) {
		case 262145:
			return mapid == 5068 || mapid == ADEN
					&& inBox(x, y, 33368, 33375, 32651, 32654)
					|| inBox(x, y, 33373, 33375, 32566, 32657);
		case 262146:
			return mapid == 5069 || mapid == ADEN
					&& inBox(x, y, 33381, 33387, 32653, 32656);
		case 262147:
			return mapid == 5070 || mapid == ADEN
					&& inBox(x, y, 33392, 33404, 32650, 32656);
		case 262148:
			return mapid == 5071 || mapid == ADEN
					&& inBox(x, y, 33427, 33430, 32656, 32662);
		case 262149:
			return mapid == 5072
					|| mapid == ADEN
					&& (inBox(x, y, 33439, 33445, 32665, 32667) || inBox(x, y,
							33442, 33445, 32668, 32672));
		case 262150:
			return mapid == 5073 || mapid == ADEN
					&& inBox(x, y, 33454, 33466, 32648, 32654);
		case 262151:
			return mapid == 5074 || mapid == ADEN
					&& inBox(x, y, 33476, 33479, 32665, 32671);
		case 262152:
			return mapid == 5075
					|| mapid == ADEN
					&& (inBox(x, y, 33471, 33477, 32678, 32680) || inBox(x, y,
							33474, 33477, 32681, 32685));
		case 262153:
			return mapid == 5076
					|| mapid == ADEN
					&& (inBox(x, y, 33453, 33460, 32694, 32697) || inBox(x, y,
							33458, 33460, 32698, 32700));
		case 262154:
			return mapid == 5077 || mapid == ADEN
					&& inBox(x, y, 33421, 33433, 32685, 32691);
		case 262155:
			return mapid == 5078
					|| mapid == ADEN
					&& (inBox(x, y, 33409, 33415, 32674, 32676) || inBox(x, y,
							33412, 33415, 32677, 32681));
		case 262156:
			return mapid == 5079
					|| mapid == ADEN
					&& (inBox(x, y, 33414, 33421, 32703, 32706) || inBox(x, y,
							33419, 33421, 32707, 32709));
		case 262157:
			return mapid == 5080 || mapid == ADEN
					&& inBox(x, y, 33372, 33384, 32692, 32698);
		case 262158:
			return mapid == 5081 || mapid == ADEN
					&& inBox(x, y, 33362, 33365, 32681, 32687);
		case 262159:
			return mapid == 5082
					|| mapid == ADEN
					&& (inBox(x, y, 33360, 33366, 32669, 32671) || inBox(x, y,
							33363, 33366, 32672, 32676));
		case 262160:
			return mapid == 5083
					|| mapid == ADEN
					&& (inBox(x, y, 33341, 33347, 32660, 32662) || inBox(x, y,
							33344, 33347, 32663, 32667));
		case 262161:
			return mapid == 5084 || mapid == ADEN
					&& inBox(x, y, 33345, 33348, 32672, 32678);
		case 262162:
			return mapid == 5085 || mapid == ADEN
					&& inBox(x, y, 33338, 33350, 32704, 32711);
		case 262163:
			return mapid == 5086
					|| mapid == ADEN
					&& (inBox(x, y, 33349, 33356, 32728, 32731) || inBox(x, y,
							33354, 33356, 32732, 32734));
		case 262164:
			return mapid == 5087
					|| mapid == ADEN
					&& (inBox(x, y, 33366, 33372, 32713, 32715) || inBox(x, y,
							33369, 33372, 32716, 32720));
		case 262165:
			return mapid == 5088 || mapid == ADEN
					&& inBox(x, y, 33380, 33383, 32712, 32718);
		case 262166:
			return mapid == 5089 || mapid == ADEN
					&& inBox(x, y, 33401, 33413, 32733, 32739);
		case 262167:
			return mapid == 5090
					|| mapid == ADEN
					&& (inBox(x, y, 33424, 33430, 32717, 32719) || inBox(x, y,
							33427, 33430, 32720, 32724));
		case 262168:
			return mapid == 5091 || mapid == ADEN
					&& inBox(x, y, 33448, 33451, 32729, 32735);
		case 262169:
			return mapid == 5092 || mapid == ADEN
					&& inBox(x, y, 33404, 33407, 32754, 32760);
		case 262170:
			return mapid == 5093 || mapid == ADEN
					&& inBox(x, y, 33363, 33375, 32755, 32761);
		case 262171:
			return mapid == 5094
					|| mapid == ADEN
					&& (inBox(x, y, 33351, 33357, 32774, 32776) || inBox(x, y,
							33354, 33357, 32777, 32781));
		case 262172:
			return mapid == 5095 || mapid == ADEN
					&& inBox(x, y, 33355, 33361, 32787, 32790);
		case 262173:
			return mapid == 5096
					|| mapid == ADEN
					&& (inBox(x, y, 33366, 33373, 32786, 32789) || inBox(x, y,
							33371, 33373, 32790, 32792));
		case 262174:
			return mapid == 5097 || mapid == ADEN
					&& inBox(x, y, 33383, 33386, 32773, 32779);
		case 262175:
			return mapid == 5098
					|| mapid == ADEN
					&& (inBox(x, y, 33397, 33404, 32788, 32791) || inBox(x, y,
							33402, 33404, 32792, 32794));
		case 262176:
			return mapid == 5099
					|| mapid == ADEN
					&& (inBox(x, y, 33479, 33486, 32788, 32791) || inBox(x, y,
							33484, 33486, 32792, 32794));
		case 262177:
			return mapid == 5100 || mapid == ADEN
					&& inBox(x, y, 33498, 33501, 32801, 32807);
		case 262178:
			return mapid == 5101 || mapid == ADEN
					&& inBox(x, y, 33379, 33385, 32802, 32805);
		case 262179:
			return mapid == 5102 || mapid == ADEN
					&& inBox(x, y, 33373, 33385, 32822, 32829);
		case 262180:
			return mapid == 5103 || mapid == ADEN
					&& inBox(x, y, 33398, 33401, 32810, 32816);
		case 262181:
			return mapid == 5104
					|| mapid == ADEN
					&& (inBox(x, y, 33397, 33403, 32821, 32823) || inBox(x, y,
							33400, 33403, 32824, 32828));
		case 262182:
			return mapid == 5105
					|| mapid == ADEN
					&& (inBox(x, y, 33431, 33438, 32838, 32841) || inBox(x, y,
							33436, 33438, 32842, 32844));
		case 262183:
			return mapid == 5106 || mapid == ADEN
					&& inBox(x, y, 33456, 33462, 32838, 32841);
		case 262184:
			return mapid == 5107
					|| mapid == ADEN
					&& (inBox(x, y, 33385, 33392, 32845, 32848) || inBox(x, y,
							33390, 33392, 32849, 32851));
		case 262185:
			return mapid == 5108
					|| mapid == ADEN
					&& (inBox(x, y, 33399, 33405, 32859, 32861) || inBox(x, y,
							33402, 33405, 32862, 32866));
		case 262186:
			return mapid == 5109 || mapid == ADEN
					&& inBox(x, y, 33414, 33417, 32850, 32856);
		case 262187:
			return mapid == 5110 || mapid == ADEN
					&& inBox(x, y, 33372, 33384, 32867, 32873);
		case 262188:
			return mapid == 5111 || mapid == ADEN
					&& inBox(x, y, 33425, 33437, 32865, 32871);
		case 262189:
			return mapid == 5112
					|| mapid == ADEN
					&& (inBox(x, y, 33443, 33449, 32869, 32871) || inBox(x, y,
							33446, 33449, 32872, 32876));
		case 327681:
			return mapid == 5113
					|| mapid == ADEN
					&& (inBox(x, y, 33599, 33601, 33213, 33214) || inBox(x, y,
							33602, 33610, 33213, 33218));
		case 327682:
			return mapid == 5114 || mapid == ADEN
					&& inBox(x, y, 33627, 33632, 33206, 33209);
		case 327683:
			return mapid == 5115
					|| mapid == ADEN
					&& (inBox(x, y, 33626, 33627, 33225, 33227) || inBox(x, y,
							33628, 33632, 33221, 33230));
		case 327684:
			return mapid == 5116
					|| mapid == ADEN
					&& (inBox(x, y, 33628, 33636, 33241, 33244)
							|| inBox(x, y, 33632, 33635, 33245, 33250) || inBox(
								x, y, 33634, 33634, 33251, 33252));
		case 327685:
			return mapid == 5117 || mapid == ADEN
					&& inBox(x, y, 33616, 33621, 33262, 33265);
		case 327686:
			return mapid == 5118
					|| mapid == ADEN
					&& (inBox(x, y, 33570, 33580, 33228, 33232) || inBox(x, y,
							33574, 33576, 33233, 33234));
		case 327687:
			return mapid == 5119
					|| mapid == ADEN
					&& (inBox(x, y, 33583, 33588, 33305, 33314) || inBox(x, y,
							33587, 33588, 33315, 33316));
		case 327688:
			return mapid == 5120
					|| mapid == ADEN
					&& (inBox(x, y, 33577, 33578, 33337, 33337)
							|| inBox(x, y, 33579, 33588, 33335, 33339) || inBox(
								x, y, 33585, 33588, 33340, 33343));
		case 327689:
			return mapid == 5121
					|| mapid == ADEN
					&& (inBox(x, y, 33615, 33623, 33374, 33377)
							|| inBox(x, y, 33619, 33622, 33378, 33383) || inBox(
								x, y, 33621, 33621, 33384, 33385));
		case 327690:
			return mapid == 5122
					|| mapid == ADEN
					&& (inBox(x, y, 33624, 33625, 33397, 33399) || inBox(x, y,
							33626, 33630, 33393, 33403));
		case 327691:
			return mapid == 5123
					|| mapid == ADEN
					&& (inBox(x, y, 33621, 33622, 33444, 33444)
							|| inBox(x, y, 33622, 33632, 33442, 33446) || inBox(
								x, y, 33629, 33632, 33447, 33450));
		case 524289:
			return mapid == ADEN && inBox(x, y, 32559, 32566, 32669, 32676);
		case 524290:
			return mapid == ADEN
					&& (inBox(x, y, 32548, 32556, 32705, 32716) || inBox(x, y,
							32547, 32547, 32710, 32716));
		case 524291:
			return mapid == ADEN && inBox(x, y, 32537, 32544, 32781, 32791);
		case 524292:
			return mapid == ADEN && inBox(x, y, 32550, 32560, 32780, 32787);
		case 524293:
			return mapid == ADEN
					&& (inBox(x, y, 32535, 32543, 32807, 32818) || inBox(x, y,
							32534, 32534, 32812, 32818));
		case 524294:
			return mapid == ADEN && inBox(x, y, 32553, 32560, 32814, 32821);
		default:
			return false;
		}
	}

	public static int[] getHouseLoc(int houseId) {
		int[] location = { 0, 0, ADEN };
		L1House house = HouseTable.getInstance().getHouseTable(houseId);
		L1Spawn keeperloc = NpcSpawnTable.getInstance().findSpawnByHousekeeperId(house.getKeeperId());
		int x = keeperloc.getLocX();
		int y = keeperloc.getLocY();

		switch (keeperloc.getHeading()) {
			case 2:
			case 4:
				x += 1;
				y += 1;
				break;
			case 6:
			case 8:
				x -= 1;
				y -= 1;
				break;
			default:
				x += 1;
				y -= 1;
				break;
		}
		location[0] = x;
		location[1] = y;

		return location;
	}

	public static int[] getBasementLoc(int houseId) {
		int[] loc = new int[3];
		if (houseId >= 262145 && houseId <= 262189) {
			loc[0] = 32780;
			loc[1] = 32815;
			loc[2] = houseId - 257077;
		} else if (houseId >= 327681 && houseId <= 327691) {
			loc[0] = 32772;
			loc[1] = 32814;
			loc[2] = houseId - 322568;
		} else if (houseId >= 524289 && houseId <= 524294) {
			loc = getHouseLoc(houseId);
		}
		return loc;
	}

	public static boolean isInHouse(int locx, int locy, short mapid) {
		for (Integer houseId : _houseIds)
			if (isInHouseLoc(houseId, locx, locy, mapid))
				return true;
		return false;
	}

	public static int[] getHouseTeleportLoc(int houseId, int number) {
		int[] loc = new int[3];
		if (houseId >= 262145 && houseId <= 262189) {
			loc[0] = TELEPORT_LOC_GIRAN[number].getX();
			loc[1] = TELEPORT_LOC_GIRAN[number].getY();
			loc[2] = TELEPORT_LOC_MAPID[number];
		} else if (houseId >= 327681 && houseId <= 327691) {
			loc[0] = TELEPORT_LOC_HEINE[number].getX();
			loc[1] = TELEPORT_LOC_HEINE[number].getY();
			loc[2] = TELEPORT_LOC_MAPID[number];
		} else if (houseId >= 458753 && houseId <= 458819) {
			loc[0] = TELEPORT_LOC_ADEN[number].getX();
			loc[1] = TELEPORT_LOC_ADEN[number].getY();
			loc[2] = TELEPORT_LOC_MAPID[number];
		} else if (houseId >= 524289 && houseId <= 524294) {
			loc[0] = TELEPORT_LOC_GLUDIN[number].getX();
			loc[1] = TELEPORT_LOC_GLUDIN[number].getY();
			loc[2] = TELEPORT_LOC_MAPID[number];
		}
		return loc;
	}
}
