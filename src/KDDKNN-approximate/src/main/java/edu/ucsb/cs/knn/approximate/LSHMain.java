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
 * @Since Aug 21, 2012
 */

package edu.ucsb.cs.knn.approximate;

/**
 * 1) You know how normal hashes are designed to avoid collisions?<br>
 * Locality-sensitive hashes are designed to cause collisions. In fact, the more
 * similar the input data is, the more similar the resulting hashes will be,
 * with a small and predictable error rate.
 * 
 * This makes them a very useful tool for large scale data mining, as a
 * component in:<br>
 * <br>
 * 
 * - fuzzy-matching database records or similar documents.<br>
 * - nearest-neighbours clustering and classification. <br>
 * - duplicate detection content recommendation. <br>
 * <br>
 * 
 * 2) LSH is used with:<br>
 * - minhash for jaccard distance<br>
 * - random projections for Euclidean and cosine distance.<br>
 * <br>
 * 
 * 3) Plot x-value = real sim vs. y-value = estimated sim.<br>
 * <br>
 * 
 * 4) for Cosine,<br>
 * assume vectors [2,0,4,...] of f frequencies for f features. Then randomly
 * pick an f-length vector then decide if others have dot product &lt 0 --> 1 or
 * &gt 0 --> -1. Then what? and them to get one signature?
 * 
 */
public class LSHMain {

}
