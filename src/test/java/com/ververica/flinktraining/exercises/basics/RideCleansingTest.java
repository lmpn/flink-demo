/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.basics;

import com.ververica.flinktraining.exercises.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.testing.TaxiRideTestBase;
import com.google.common.collect.Lists;
import com.ververica.flinktraining.use_cases.basics.RideCleansingSolution;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RideCleansingTest extends TaxiRideTestBase<TaxiRide> {

	static Testable javaExercise = () -> StreamingTaxi.main(new String[]{});


	@Test
	public void queryTest() throws Exception {
		TaxiFare f1 = testfare(1,10,10,10);
		TaxiFare f2 = testfare(2,10,10,10);
		TaxiFare f3 = testfare(1,10,10,10);

		TestFareSource source = new TestFareSource(f1,f2,f3);

		assertEquals(Lists.newArrayList(), results(source));
	}

	private TaxiRide testRide(float startLon, float startLat, float endLon, float endLat) {
		return new TaxiRide(1L, true, new DateTime(0), new DateTime(0),
				startLon, startLat, endLon, endLat, (short)1, 0, 0);
	}

	private TaxiFare testfare(long id,float tip, float tolls, float totalFare) {
		return  new TaxiFare(1,1,id,new DateTime(0),"CSH", tip,tolls,totalFare);
	}

	protected List<?> results(TestFareSource source) throws Exception {
		Testable javaSolution = () -> RideCleansingSolution.main(new String[]{});
		return runApp(source, new TestSink<>(), javaExercise, javaSolution);
	}

}