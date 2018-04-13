/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expand.describe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by bingxing.wang on 2018/3/7.
 */
public class SchemaResult {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaResult.class);
    public final List<Map<String, String>> results;
    public final Set<String> columns;

    public SchemaResult(){
        this.results  = Lists.newArrayList();
        this.columns  = Sets.newLinkedHashSet();

        this.columns.add("COLUMN_NAME");
        this.columns.add("DATA_TYPE");
    }

}
