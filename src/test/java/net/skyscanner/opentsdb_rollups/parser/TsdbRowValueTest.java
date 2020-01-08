/**
 * Copyright 2020 Skyscanner Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.skyscanner.opentsdb_rollups.parser;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TsdbRowValueTest {

    private long testWriteTime, testTimestamp;
    private List<Double> testValues;

    @Before
    public void setup() {
        testWriteTime = 1561024800;
        testTimestamp = 1560172880;

        testValues = new ArrayList<>();
        testValues.add(1.0);
        testValues.add(2.0);
    }


    @Test
    public void testAddValues() {
        TsdbRowValue rowVal = new TsdbRowValue();

        rowVal.addValues(testWriteTime, testTimestamp, testValues);
        assertEquals(testValues, rowVal.getValues());
    }

    @Test
    public void testAddMultipleValues() {
        TsdbRowValue rowVal = new TsdbRowValue();
        List<Double> moreValues = new ArrayList<>();
        moreValues.add(3.0);
        moreValues.add(4.0);

        rowVal.addValues(testWriteTime, testTimestamp, testValues);
        rowVal.addValues(testWriteTime, testTimestamp + 1, moreValues);

        List<Double> joinedValues = new ArrayList<>(testValues);
        joinedValues.addAll(moreValues);
        assertEquals(joinedValues, rowVal.getValues());
    }


    @Test
    public void testValuesOverwrite() {
        TsdbRowValue rowVal = new TsdbRowValue();
        List<Double> overWriteValues = new ArrayList<>();
        overWriteValues.add(10.0);
        overWriteValues.add(11.0);

        assertNotEquals(overWriteValues, testValues);

        rowVal.addValues(testWriteTime, testTimestamp, testValues);
        assertEquals(testValues, rowVal.getValues());

        rowVal.addValues(testWriteTime + 1, testTimestamp, overWriteValues);
        assertEquals(overWriteValues, rowVal.getValues());
    }


    @Test
    public void testNotOverwrite() {
        TsdbRowValue rowVal = new TsdbRowValue();
        List<Double> overWriteValues = new ArrayList<>();
        overWriteValues.add(10.0);
        overWriteValues.add(11.0);

        assertNotEquals(overWriteValues, testValues);

        rowVal.addValues(testWriteTime, testTimestamp, testValues);
        assertEquals(testValues, rowVal.getValues());

        rowVal.addValues(testWriteTime - 1, testTimestamp, overWriteValues);
        assertEquals(testValues, rowVal.getValues());
    }

}
