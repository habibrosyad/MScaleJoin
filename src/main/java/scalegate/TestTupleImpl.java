/*  Copyright (C) 2015  Ioannis Nikolakopoulos,
 * 			Daniel Cederman,
 * 			Vincenzo Gulisano,
 * 			Marina Papatriantafilou,
 * 			Philippas Tsigas
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact: Ioannis (aka Yiannis) Nikolakopoulos ioaniko@chalmers.se
 *  	     Vincenzo Gulisano vincenzo.gulisano@chalmers.se
 *
 */

package scalegate;

public class TestTupleImpl implements Comparable<ScaleGateTuple>, ScaleGateTuple {

    private long timestamp;

    TestTupleImpl(long ts) {
        this.timestamp = ts;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(ScaleGateTuple o) {

        if (this.timestamp == o.getTimestamp()) {
            return 0;
        } else {
            return this.timestamp > o.getTimestamp() ? 1 : -1;
        }
    }

}
