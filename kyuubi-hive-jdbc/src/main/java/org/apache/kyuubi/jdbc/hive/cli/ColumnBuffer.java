/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.jdbc.hive.cli;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.hive.service.rpc.thrift.*;

/** ColumnBuffer */
public class ColumnBuffer extends AbstractList {

  private static final int DEFAULT_SIZE = 100;

  private final TTypeId type;

  private BitSet nulls;

  private int size;
  private boolean[] boolVars;
  private byte[] byteVars;
  private short[] shortVars;
  private int[] intVars;
  private long[] longVars;
  private double[] doubleVars;
  private List<String> stringVars;
  private List<ByteBuffer> binaryVars;

  public ColumnBuffer(TTypeId type, BitSet nulls, Object values) {
    this.type = type;
    this.nulls = nulls;
    if (type == TTypeId.BOOLEAN_TYPE) {
      boolVars = (boolean[]) values;
      size = boolVars.length;
    } else if (type == TTypeId.TINYINT_TYPE) {
      byteVars = (byte[]) values;
      size = byteVars.length;
    } else if (type == TTypeId.SMALLINT_TYPE) {
      shortVars = (short[]) values;
      size = shortVars.length;
    } else if (type == TTypeId.INT_TYPE) {
      intVars = (int[]) values;
      size = intVars.length;
    } else if (type == TTypeId.BIGINT_TYPE) {
      longVars = (long[]) values;
      size = longVars.length;
    } else if (type == TTypeId.DOUBLE_TYPE || type == TTypeId.FLOAT_TYPE) {
      doubleVars = (double[]) values;
      size = doubleVars.length;
    } else if (type == TTypeId.BINARY_TYPE) {
      binaryVars = (List<ByteBuffer>) values;
      size = binaryVars.size();
    } else if (type == TTypeId.STRING_TYPE) {
      stringVars = (List<String>) values;
      size = stringVars.size();
    } else {
      throw new IllegalStateException("invalid union object");
    }
  }

  public ColumnBuffer(TTypeId type) {
    nulls = new BitSet();
    switch (type) {
      case BOOLEAN_TYPE:
        boolVars = new boolean[DEFAULT_SIZE];
        break;
      case TINYINT_TYPE:
        byteVars = new byte[DEFAULT_SIZE];
        break;
      case SMALLINT_TYPE:
        shortVars = new short[DEFAULT_SIZE];
        break;
      case INT_TYPE:
        intVars = new int[DEFAULT_SIZE];
        break;
      case BIGINT_TYPE:
        longVars = new long[DEFAULT_SIZE];
        break;
      case FLOAT_TYPE:
        type = TTypeId.FLOAT_TYPE;
        doubleVars = new double[DEFAULT_SIZE];
        break;
      case DOUBLE_TYPE:
        type = TTypeId.DOUBLE_TYPE;
        doubleVars = new double[DEFAULT_SIZE];
        break;
      case BINARY_TYPE:
        binaryVars = new ArrayList<ByteBuffer>();
        break;
      default:
        type = TTypeId.STRING_TYPE;
        stringVars = new ArrayList<String>();
    }
    this.type = type;
  }

  public ColumnBuffer(TColumn colValues) {
    if (colValues.isSetBoolVal()) {
      type = TTypeId.BOOLEAN_TYPE;
      nulls = toBitset(colValues.getBoolVal().getNulls());
      boolVars = Booleans.toArray(colValues.getBoolVal().getValues());
      size = boolVars.length;
    } else if (colValues.isSetByteVal()) {
      type = TTypeId.TINYINT_TYPE;
      nulls = toBitset(colValues.getByteVal().getNulls());
      byteVars = Bytes.toArray(colValues.getByteVal().getValues());
      size = byteVars.length;
    } else if (colValues.isSetI16Val()) {
      type = TTypeId.SMALLINT_TYPE;
      nulls = toBitset(colValues.getI16Val().getNulls());
      shortVars = Shorts.toArray(colValues.getI16Val().getValues());
      size = shortVars.length;
    } else if (colValues.isSetI32Val()) {
      type = TTypeId.INT_TYPE;
      nulls = toBitset(colValues.getI32Val().getNulls());
      intVars = Ints.toArray(colValues.getI32Val().getValues());
      size = intVars.length;
    } else if (colValues.isSetI64Val()) {
      type = TTypeId.BIGINT_TYPE;
      nulls = toBitset(colValues.getI64Val().getNulls());
      longVars = Longs.toArray(colValues.getI64Val().getValues());
      size = longVars.length;
    } else if (colValues.isSetDoubleVal()) {
      type = TTypeId.DOUBLE_TYPE;
      nulls = toBitset(colValues.getDoubleVal().getNulls());
      doubleVars = Doubles.toArray(colValues.getDoubleVal().getValues());
      size = doubleVars.length;
    } else if (colValues.isSetBinaryVal()) {
      type = TTypeId.BINARY_TYPE;
      nulls = toBitset(colValues.getBinaryVal().getNulls());
      binaryVars = colValues.getBinaryVal().getValues();
      size = binaryVars.size();
    } else if (colValues.isSetStringVal()) {
      type = TTypeId.STRING_TYPE;
      nulls = toBitset(colValues.getStringVal().getNulls());
      stringVars = colValues.getStringVal().getValues();
      size = stringVars.size();
    } else {
      throw new IllegalStateException("invalid union object");
    }
  }

  /**
   * Get a subset of this ColumnBuffer, starting from the 1st value.
   *
   * @param end index after the last value to include
   */
  public ColumnBuffer extractSubset(int end) {
    BitSet subNulls = nulls.get(0, end);
    if (type == TTypeId.BOOLEAN_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(boolVars, 0, end));
      boolVars = Arrays.copyOfRange(boolVars, end, size);
      nulls = nulls.get(end, size);
      size = boolVars.length;
      return subset;
    }
    if (type == TTypeId.TINYINT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(byteVars, 0, end));
      byteVars = Arrays.copyOfRange(byteVars, end, size);
      nulls = nulls.get(end, size);
      size = byteVars.length;
      return subset;
    }
    if (type == TTypeId.SMALLINT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(shortVars, 0, end));
      shortVars = Arrays.copyOfRange(shortVars, end, size);
      nulls = nulls.get(end, size);
      size = shortVars.length;
      return subset;
    }
    if (type == TTypeId.INT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(intVars, 0, end));
      intVars = Arrays.copyOfRange(intVars, end, size);
      nulls = nulls.get(end, size);
      size = intVars.length;
      return subset;
    }
    if (type == TTypeId.BIGINT_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, Arrays.copyOfRange(longVars, 0, end));
      longVars = Arrays.copyOfRange(longVars, end, size);
      nulls = nulls.get(end, size);
      size = longVars.length;
      return subset;
    }
    if (type == TTypeId.DOUBLE_TYPE || type == TTypeId.FLOAT_TYPE) {
      ColumnBuffer subset =
          new ColumnBuffer(type, subNulls, Arrays.copyOfRange(doubleVars, 0, end));
      doubleVars = Arrays.copyOfRange(doubleVars, end, size);
      nulls = nulls.get(end, size);
      size = doubleVars.length;
      return subset;
    }
    if (type == TTypeId.BINARY_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, binaryVars.subList(0, end));
      binaryVars = binaryVars.subList(end, binaryVars.size());
      nulls = nulls.get(end, size);
      size = binaryVars.size();
      return subset;
    }
    if (type == TTypeId.STRING_TYPE) {
      ColumnBuffer subset = new ColumnBuffer(type, subNulls, stringVars.subList(0, end));
      stringVars = stringVars.subList(end, stringVars.size());
      nulls = nulls.get(end, size);
      size = stringVars.size();
      return subset;
    }
    throw new IllegalStateException("invalid union object");
  }

  @VisibleForTesting
  BitSet getNulls() {
    return nulls;
  }

  private static final byte[] MASKS =
      new byte[] {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80};

  private static BitSet toBitset(byte[] nulls) {
    BitSet bitset = new BitSet();
    int bits = nulls.length * 8;
    for (int i = 0; i < bits; i++) {
      bitset.set(i, (nulls[i / 8] & MASKS[i % 8]) != 0);
    }
    return bitset;
  }

  private static byte[] toBinary(BitSet bitset) {
    byte[] nulls = new byte[1 + (bitset.length() / 8)];
    for (int i = 0; i < bitset.length(); i++) {
      nulls[i / 8] |= bitset.get(i) ? MASKS[i % 8] : 0;
    }
    return nulls;
  }

  public TTypeId getType() {
    return type;
  }

  @Override
  public Object get(int index) {
    if (nulls.get(index)) {
      return null;
    }
    switch (type) {
      case BOOLEAN_TYPE:
        return boolVars[index];
      case TINYINT_TYPE:
        return byteVars[index];
      case SMALLINT_TYPE:
        return shortVars[index];
      case INT_TYPE:
        return intVars[index];
      case BIGINT_TYPE:
        return longVars[index];
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
        return doubleVars[index];
      case STRING_TYPE:
        return stringVars.get(index);
      case BINARY_TYPE:
        return binaryVars.get(index).array();
    }
    return null;
  }

  @Override
  public int size() {
    return size;
  }

  public TColumn toTColumn() {
    TColumn value = new TColumn();
    ByteBuffer nullMasks = ByteBuffer.wrap(toBinary(nulls));
    switch (type) {
      case BOOLEAN_TYPE:
        value.setBoolVal(
            new TBoolColumn(Booleans.asList(Arrays.copyOfRange(boolVars, 0, size)), nullMasks));
        break;
      case TINYINT_TYPE:
        value.setByteVal(
            new TByteColumn(Bytes.asList(Arrays.copyOfRange(byteVars, 0, size)), nullMasks));
        break;
      case SMALLINT_TYPE:
        value.setI16Val(
            new TI16Column(Shorts.asList(Arrays.copyOfRange(shortVars, 0, size)), nullMasks));
        break;
      case INT_TYPE:
        value.setI32Val(
            new TI32Column(Ints.asList(Arrays.copyOfRange(intVars, 0, size)), nullMasks));
        break;
      case BIGINT_TYPE:
        value.setI64Val(
            new TI64Column(Longs.asList(Arrays.copyOfRange(longVars, 0, size)), nullMasks));
        break;
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
        value.setDoubleVal(
            new TDoubleColumn(Doubles.asList(Arrays.copyOfRange(doubleVars, 0, size)), nullMasks));
        break;
      case STRING_TYPE:
        value.setStringVal(new TStringColumn(stringVars, nullMasks));
        break;
      case BINARY_TYPE:
        value.setBinaryVal(new TBinaryColumn(binaryVars, nullMasks));
        break;
    }
    return value;
  }

  private static final ByteBuffer EMPTY_BINARY = ByteBuffer.allocate(0);
  private static final String EMPTY_STRING = "";

  public void addValue(Object field) {
    addValue(this.type, field);
  }

  public void addValue(TTypeId type, Object field) {
    switch (type) {
      case BOOLEAN_TYPE:
        nulls.set(size, field == null);
        boolVars()[size] = field == null ? true : (Boolean) field;
        break;
      case TINYINT_TYPE:
        nulls.set(size, field == null);
        byteVars()[size] = field == null ? 0 : (Byte) field;
        break;
      case SMALLINT_TYPE:
        nulls.set(size, field == null);
        shortVars()[size] = field == null ? 0 : (Short) field;
        break;
      case INT_TYPE:
        nulls.set(size, field == null);
        intVars()[size] = field == null ? 0 : (Integer) field;
        break;
      case BIGINT_TYPE:
        nulls.set(size, field == null);
        longVars()[size] = field == null ? 0 : (Long) field;
        break;
      case FLOAT_TYPE:
        nulls.set(size, field == null);
        doubleVars()[size] = field == null ? 0 : new Double(field.toString());
        break;
      case DOUBLE_TYPE:
        nulls.set(size, field == null);
        doubleVars()[size] = field == null ? 0 : (Double) field;
        break;
      case BINARY_TYPE:
        nulls.set(binaryVars.size(), field == null);
        binaryVars.add(field == null ? EMPTY_BINARY : ByteBuffer.wrap((byte[]) field));
        break;
      default:
        nulls.set(stringVars.size(), field == null);
        stringVars.add(field == null ? EMPTY_STRING : String.valueOf(field));
        break;
    }
    size++;
  }

  private boolean[] boolVars() {
    if (boolVars.length == size) {
      boolean[] newVars = new boolean[size << 1];
      System.arraycopy(boolVars, 0, newVars, 0, size);
      return boolVars = newVars;
    }
    return boolVars;
  }

  private byte[] byteVars() {
    if (byteVars.length == size) {
      byte[] newVars = new byte[size << 1];
      System.arraycopy(byteVars, 0, newVars, 0, size);
      return byteVars = newVars;
    }
    return byteVars;
  }

  private short[] shortVars() {
    if (shortVars.length == size) {
      short[] newVars = new short[size << 1];
      System.arraycopy(shortVars, 0, newVars, 0, size);
      return shortVars = newVars;
    }
    return shortVars;
  }

  private int[] intVars() {
    if (intVars.length == size) {
      int[] newVars = new int[size << 1];
      System.arraycopy(intVars, 0, newVars, 0, size);
      return intVars = newVars;
    }
    return intVars;
  }

  private long[] longVars() {
    if (longVars.length == size) {
      long[] newVars = new long[size << 1];
      System.arraycopy(longVars, 0, newVars, 0, size);
      return longVars = newVars;
    }
    return longVars;
  }

  private double[] doubleVars() {
    if (doubleVars.length == size) {
      double[] newVars = new double[size << 1];
      System.arraycopy(doubleVars, 0, newVars, 0, size);
      return doubleVars = newVars;
    }
    return doubleVars;
  }
}
