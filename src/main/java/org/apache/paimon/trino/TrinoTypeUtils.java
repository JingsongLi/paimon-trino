/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;


import java.util.List;
import java.util.stream.Collectors;

/** Trino type from Flink Type. */
public class TrinoTypeUtils {

    public static Type fromFlinkType(DataType type) {
        return type.accept(FlinkToTrinoTypeVistor.INSTANCE);
    }

    private static class FlinkToTrinoTypeVistor extends DataTypeDefaultVisitor<Type> {

        private static final FlinkToTrinoTypeVistor INSTANCE = new FlinkToTrinoTypeVistor();

        @Override
        public Type visit(CharType charType) {
            return io.trino.spi.type.CharType.createCharType(
                    Math.min(io.trino.spi.type.CharType.MAX_LENGTH, charType.getLength()));
        }

        @Override
        public Type visit(VarCharType varCharType) {
            return VarcharType.createVarcharType(
                    Math.min(VarcharType.MAX_LENGTH, varCharType.getLength()));
        }

        @Override
        public Type visit(BooleanType booleanType) {
            return io.trino.spi.type.BooleanType.BOOLEAN;
        }

        @Override
        public Type visit(BinaryType binaryType) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(VarBinaryType varBinaryType) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(DecimalType decimalType) {
            return io.trino.spi.type.DecimalType.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public Type visit(TinyIntType tinyIntType) {
            return TinyintType.TINYINT;
        }

        @Override
        public Type visit(SmallIntType smallIntType) {
            return SmallintType.SMALLINT;
        }

        @Override
        public Type visit(IntType intType) {
            return IntegerType.INTEGER;
        }

        @Override
        public Type visit(BigIntType bigIntType) {
            return BigintType.BIGINT;
        }

        @Override
        public Type visit(FloatType floatType) {
            return RealType.REAL;
        }

        @Override
        public Type visit(DoubleType doubleType) {
            return io.trino.spi.type.DoubleType.DOUBLE;
        }

        @Override
        public Type visit(DateType dateType) {
            return io.trino.spi.type.DateType.DATE;
        }

        @Override
        public Type visit(TimeType timeType) {
            return io.trino.spi.type.TimeType.TIME_MILLIS;
        }

        @Override
        public Type visit(TimestampType timestampType) {
            return io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
        }

        @Override
        public Type visit(LocalZonedTimestampType localZonedTimestampType) {
            return TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
        }

        @Override
        public Type visit(ArrayType arrayType) {
            DataType elementType = arrayType.getElementType();
            return new io.trino.spi.type.ArrayType(elementType.accept(this));
        }

        @Override
        public Type visit(MultisetType multisetType) {
            return new MapType(multisetType.getElementType(), new IntType()).accept(this);
        }

        @Override
        public Type visit(MapType mapType) {
            return new io.trino.spi.type.MapType(
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this),
                    new TypeOperators());
        }

        @Override
        public Type visit(RowType rowType) {
            List<io.trino.spi.type.RowType.Field> fields =
                    rowType.getFields().stream()
                            .map(
                                    field ->
                                            io.trino.spi.type.RowType.field(
                                                    field.name(), field.type().accept(this)))
                            .collect(Collectors.toList());
            return io.trino.spi.type.RowType.from(fields);
        }

        @Override
        protected Type defaultMethod(DataType logicalType) {
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }
}
