package org.apache.drill.contrib.function;

/*
 * Copyright 2001-2004 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;


public class DrillSynFunctions {
  public  DrillSynFunctions() {
  }

  @FunctionTemplate(name = "is_syn_flood",
      scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE,
      nulls = FunctionTemplate.NullHandling.INTERNAL)

  public static class SynFloodFunction implements DrillAggFunc {
    @Param
    NullableBigIntHolder sessionID;

    @Param
    NullableIntHolder syn;

    @Param
    NullableIntHolder ack;

    @Output
    BitHolder out;

    @Workspace
    IntHolder connectionStatus;

    @Override
    public void setup() {
      connectionStatus.value = 0;
    }

    @Override
    public void add() {
      if( syn.value == 1 && connectionStatus.value == 0 ){
        //New connection attempt  SYN received and awaiting SYN/ACK
        connectionStatus.value = 1;
      } else if( connectionStatus.value == 1 && ack.value == 1 && syn.value == 1){
        //Connection status of 2 indicates syn/ack has been received and are awaiting the final ack
        connectionStatus.value = 2;
      } else if( connectionStatus.value == 2 && syn.value == 0 && ack.value == 1) {
        //ACK received, connection established
        connectionStatus.value = 3;
      }
    }

    @Override
    public void output() {
      if( connectionStatus.value == 2) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }

    @Override
    public void reset() {
      connectionStatus.value = 0;
    }

  }

}