package com.nickkbright.drill.udf;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

public class AGG_FUNCTIONS {

    @FunctionTemplate(name = "CHAR_COUNTER", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE, desc= "Returns total number of chars from VarChar row")
    public static class CHAR_COUNTER implements DrillAggFunc{

        
        @Param
        NullableVarCharHolder input;

        @Workspace
        IntHolder counter;

        @Output
        IntHolder out;

        public void setup() {
            counter = new IntHolder();
            counter.value = 0;
        }

        @Override
        public void add() {

            String varcharString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers
                    .toStringFromUTF8(input.start, input.end, input.buffer);
            counter.value += varcharString.length();

        }

        @Override
        public void output() {
            out.value = counter.value;
        }

        @Override
        public void reset() {
            counter.value = 0;
        }

    }

    @FunctionTemplate(name = "SECOND_MIN", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE, desc = "Returns second minimum value from BigInt row")
    public static class MySecondMin implements DrillAggFunc {
        @Param
        NullableBigIntHolder in;
        @Workspace
        BigIntHolder min;
        @Workspace
        BigIntHolder secondMin;
        @Output
        BigIntHolder out;

        public void setup() {
            min = new BigIntHolder();
            secondMin = new BigIntHolder();
            min.value = Long.MAX_VALUE;
            secondMin.value = Long.MAX_VALUE;
        }

        @Override
        public void add() {

            if (in.value < min.value) {
                secondMin.value = min.value;
                min.value = in.value;
            }

        }

        @Override
        public void output() {
            out.value = secondMin.value;
        }

        @Override
        public void reset() {
            min.value = Long.MAX_VALUE;
            secondMin.value = Long.MAX_VALUE;
        }

    }

}
