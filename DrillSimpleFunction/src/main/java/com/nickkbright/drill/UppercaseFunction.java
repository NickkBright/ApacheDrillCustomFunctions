package com.nickkbright.drill;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;
import java.util.Calendar;

@FunctionTemplate(
        name = "mask",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class UppercaseFunction implements DrillSimpleFunc {
    @Param
    NullableVarCharHolder input;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;



    public void setup() {
    }

    public void eval() {

        String stringValue = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);




            String outputValue = stringValue.toUpperCase();
            out.buffer = buffer;
            out.start = 0;
            out.end = outputValue.getBytes().length;
            buffer.setBytes(out.start, outputValue.getBytes());
    }
}
