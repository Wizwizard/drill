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


package org.apache.drill.exec.expand.HttpClientUtils;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bingxing.wang on 2018/4/16.
 */
public class UserAuth {
    private static final String GROUP = "drill";
    private static final String GROUP_KEY = "group";
    private static final String EMAIL_KEY = "email";
    private static final String TOKEN_KEY = "token";
    private static final String GSID_KEY = "gsid";
    private static final String TOKENAUTH_URL = "https://sso.cootekservice.com/api/v1/tokenauth";


    public static void tokenAuth(String email, String token) throws IllegalAccessException{
        Gson gson = new Gson();
        Map<String, String> map = new HashMap<>();
        map.put(GROUP_KEY, GROUP);
        map.put(EMAIL_KEY, email);
        map.put(TOKEN_KEY, token);
        map.put(GSID_KEY, "");

        String data = gson.toJson(map);

        Map<String, String> res = gson.fromJson(HttpClientUtil.doPost(TOKENAUTH_URL, data, "utf-8"), Map.class);

        if(!res.get("status").equals("OK")){
            throw new IllegalAccessException(res.get("status"));
        }
    }
}
