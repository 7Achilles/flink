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

package org.apache.flink.cep.pattern;

/** Type enum of time interval corresponds to the maximum time gap between events. */
// 匹配事件的时间间隔的类型
public enum WithinType {
    // Interval corresponds to the maximum time gap between the previous and current event.
    // 前一个事件和当前事件之间的最大时间间隔
    PREVIOUS_AND_CURRENT,
    // Interval corresponds to the maximum time gap between the first and last event.
    // 第一个事件和最后一个事件之间的最大时间间隔
    FIRST_AND_LAST;
}
