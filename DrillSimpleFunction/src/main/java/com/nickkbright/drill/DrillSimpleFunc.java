package com.nickkbright.drill;

import org.apache.drill.exec.expr.DrillFunc;

public interface DrillSimpleFunc extends DrillFunc {
    void setup();
    void eval();
}
