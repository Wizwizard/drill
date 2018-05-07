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


import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;

import java.util.Map;

/**
 * Created by bingxing.wang on 2018/3/7.
 */
public class AnalyzeSql {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AnalyzeSql.class);
    public static boolean isDescribe(String sql){
        String s = sql.replace("\n", "").trim().toLowerCase();
        return s.startsWith("describe");
    }

    // 当前仅限获取hdfs地址
    // 地址格式为 `/....`
    public static String getPath(String sql, DrillbitContext drillbitContext) throws Exception{
        String path = sql.replace("\n", "").trim().replaceAll(" +", " ").split(" ")[1];
        String[] paths = path.split("`");
        String[] workspace = paths[0].split("\\.");
        String location = ((FileSystemConfig)drillbitContext.getStorage().
                getPlugin(workspace[0]).
                getConfig()).workspaces.
                get(workspace[1]).
                getLocation();

        if(!paths[1].startsWith("/")){
            path = "/" + paths[1];
        } else {
            path = paths[1];
        }

        path = (location + path).replace("//", "/");

        logger.info("picasso: getPath: path:" + path);
        return path;
    }
}
