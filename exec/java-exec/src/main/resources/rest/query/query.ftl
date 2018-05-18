<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#include "*/generic.ftl">
<#macro page_head>
    <#if model?? && model>
      <script src="/static/js/jquery.form.js"></script>
    </#if>
    <meta name="viewport" content="width=device-width,initial-scale=1,shrink-to-fit=no">
    <meta name="theme-color" content="#000000">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css">
    <script src="/static/js/jquery-3.2.1.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.js"></script>
      <script src="../static/js/ace-code-editor/ace.js" type="text/javascript" charset="utf-8"></script>
      <script src="../static/js/ace-code-editor/mode-sql.js" type="text/javascript" charset="utf-8"></script>
      <script src="../static/js/ace-code-editor/ext-language_tools.js" type="text/javascript" charset="utf-8"></script>
      <script src="../static/js/ace-code-editor/theme-sqlserver.js" type="text/javascript" charset="utf-8"></script>
      <script src="../static/js/ace-code-editor/snippets/sql.js" type="text/javascript" charset="utf-8"></script>
      <script src="../static/js/ace-code-editor/mode-snippets.js" type="text/javascript" charset="utf-8"></script>
    <link href="/static/css/main.d36c4d4d.css" rel="stylesheet">
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
   <noscript>You need to enable JavaScript to run this app.</noscript>
   <div id="root"></div>
   <script type="text/javascript" src="/static/js/main.0b431131.js"></script>

     <script>
     $(function(){
       ace.require("ace/ext/language_tools");
       var editor = ace.edit("query-editor-format");

         //picasso add
         window.setEditorValue = function(value){
           editor.getSession().setValue(value);
         };

       var queryText = $('input[name="query"]');
       //Hidden text input for form-submission
       editor.getSession().on("change", function () {
         queryText.val(editor.getSession().getValue());
         if(document.getElementById('query').onchange) {
           document.getElementById('query').onchange();//当Query内容改变时，手动触发input[name='query']的change事件（事件在query.jsx的componentDidMount中注册）
         }
});
       editor.setAutoScrollEditorIntoView(true);
       editor.setOption("maxLines", 15);
       editor.setOption("minLines", 15);
       editor.renderer.setShowGutter(true);
       editor.renderer.setOption('showLineNumbers', true);
       editor.renderer.setOption('showPrintMargin', false);
       editor.getSession().setMode("ace/mode/sql");
       editor.getSession().setTabSize(4);
       editor.getSession().setUseSoftTabs(true);
       editor.setTheme("ace/theme/sqlserver");
       editor.$blockScrolling = "Infinity";
       //CSS Formatting
       document.getElementById('query-editor-format').style.fontSize='14px';
       document.getElementById('query-editor-format').style.fontFamily='courier';
       document.getElementById('query-editor-format').style.lineHeight='1.5';
       document.getElementById('query-editor-format').style.width='98%';
       document.getElementById('query-editor-format').style.margin='auto';
       editor.setOptions({
         enableSnippets: true,
         enableBasicAutocompletion: true,
         enableLiveAutocompletion: true
       });
       });
     </script>
</#macro>

<@page_html/>
