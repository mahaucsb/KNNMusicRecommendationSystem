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
 * @Since Oct 2, 2012
 */

package edu.ucsb.cs.knn.hybrid;

import org.apache.hadoop.mapred.JobConf;

import edu.ucsb.cs.knn.core.ForwardMapRunner;

/**
 * @author Maha
 */
public class HybridMapRunner extends ForwardMapRunner {

	@Override
	public void configure(JobConf job) {
		mapper = new HybridMapper();
	}
}
