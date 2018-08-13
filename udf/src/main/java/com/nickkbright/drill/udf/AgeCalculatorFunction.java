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
        name="GET_AGE",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)

public class AgeCalculatorFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder date;
   
    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;
    public void setup() {

    }
    public void eval() {
        String date_string = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(date.start, date.end, date.buffer);
            int year_value = java.util.Calendar.getInstance().get(java.util.Calendar.YEAR);
            int month_value = java.util.Calendar.getInstance().get(java.util.Calendar.MONTH);
            int day_value = java.util.Calendar.getInstance().get(java.util.Calendar.DAY_OF_MONTH);
            int age;

            String[] date_parts = date_string.split("-");
            age = year_value - Integer.parseInt(date_parts[0]);
            if ((month_value - Integer.parseInt(date_parts[1]) < 0))
                age--;
            else
                if (day_value == Integer.parseInt(date_parts[1]))
                    if ((day_value - Integer.parseInt(date_parts[2]) < 0))
                        age--;

        String output_string = age + " years";
        out.buffer = buffer;
        out.start = 0;
        out.end = output_string.getBytes().length;
        buffer.setBytes(0, output_string.getBytes());
    }
}
