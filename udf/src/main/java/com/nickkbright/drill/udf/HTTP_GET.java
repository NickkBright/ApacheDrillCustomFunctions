package com.nickkbright.drill.udf;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;

import javax.inject.Inject;

@FunctionTemplate(name = "HTTP_GET", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL, desc = "Function HTTP_GET. Input: Constant NullableVarChar with URL to data. Output: NullableVarChar with data from http-request. If http-request fails, output will send null value")
public class HTTP_GET implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder address;

    @Output
    NullableVarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {

    }

    public void eval() {
        String outputValue = null;
        String addressString = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers
                .getStringFromVarCharHolder(address);

        try {
            java.net.URL site = new java.net.URL(addressString);
            java.net.URLConnection con = site.openConnection();
            java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(con.getInputStream()));
            outputValue = org.apache.commons.io.IOUtils.toString(reader);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            out.isSet = 0;
            return;
        }

        out.isSet = 1;
        out.buffer = buffer.reallocIfNeeded(outputValue.getBytes().length);
        out.start = 0;
        out.end = outputValue.getBytes().length;
        out.buffer.setBytes(0, outputValue.getBytes());

    }

}
