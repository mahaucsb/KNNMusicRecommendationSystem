/**
 * Copyright 2012-2013 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: maha alabduljalil <maha (at) cs.ucsb.edu>
 * @Since Aug 20, 2012
 */

package edu.ucsb.cs.knn.types;

import java.util.ArrayList;

import edu.ucsb.cs.knn.inverted.InvertedMain;

/**
 * @author Maha
 * 
 */
public class SortedArrayList<T> extends ArrayList<T> {

	@Override
	public boolean add(T value) {
		super.add(value);
		Comparable<T> cmp = (Comparable<T>) value;

		for (int i = size() - 1; i > 0 && cmp.compareTo(get(i - 1)) < 0; i--) {
			T tmp = get(i);
			set(i, get(i - 1));
			set(i - 1, tmp);
		}
		if (size() > InvertedMain.K)
			super.remove(InvertedMain.K);
		return true;
	}
}
