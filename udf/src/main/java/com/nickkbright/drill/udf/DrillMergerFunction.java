package com.nickkbright.drill.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

@FunctionTemplate(
        name = "merger",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)

public class DrillMergerFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder first_string;

    @Param
    NullableVarCharHolder second_string;

    @Output
    VarCharHolder output;

    @Inject
    DrillBuf buffer;

    public void setup() {

    }

    public void eval() {
        String first_value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(first_string.start, first_string.end, first_string.buffer);
        String second_value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(second_string.start, second_string.end, second_string.buffer);
        String output_value = first_value + " " + second_value;
        output.buffer = buffer;
        output.start = 0;
        output.end = output_value.getBytes().length;
        buffer.setBytes(0, output_value.getBytes());
    }
}
