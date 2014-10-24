[33mcommit 5fabc56ac3aae57e82fd1a598c8429906879b30a[m
Author: penger <gongpengllpp@sina.com>
Date:   Fri Oct 24 10:15:08 2014 +0800

    add a

[1mdiff --git a/a b/a[m
[1mnew file mode 100644[m
[1mindex 0000000..388d6e0[m
[1m--- /dev/null[m
[1m+++ b/a[m
[36m@@ -0,0 +1 @@[m
[32m+[m[32mocean[m[41m[m

[33mcommit 54ac4555a409a54f118496a9d610afb8f78235ea[m
Author: penger <gongpengllpp@sina.com>
Date:   Fri Oct 24 09:44:57 2014 +0800

    re init

[1mdiff --git a/.project b/.project[m
[1mnew file mode 100644[m
[1mindex 0000000..16bd08f[m
[1m--- /dev/null[m
[1m+++ b/.project[m
[36m@@ -0,0 +1,17 @@[m
[32m+[m[32m<?xml version="1.0" encoding="UTF-8"?>[m[41m[m
[32m+[m[32m<projectDescription>[m[41m[m
[32m+[m	[32m<name>z_hadoop</name>[m[41m[m
[32m+[m	[32m<comment></comment>[m[41m[m
[32m+[m	[32m<projects>[m[41m[m
[32m+[m	[32m</projects>[m[41m[m
[32m+[m	[32m<buildSpec>[m[41m[m
[32m+[m		[32m<buildCommand>[m[41m[m
[32m+[m			[32m<name>org.eclipse.jdt.core.javabuilder</name>[m[41m[m
[32m+[m			[32m<arguments>[m[41m[m
[32m+[m			[32m</arguments>[m[41m[m
[32m+[m		[32m</buildCommand>[m[41m[m
[32m+[m	[32m</buildSpec>[m[41m[m
[32m+[m	[32m<natures>[m[41m[m
[32m+[m		[32m<nature>org.eclipse.jdt.core.javanature</nature>[m[41m[m
[32m+[m	[32m</natures>[m[41m[m
[32m+[m[32m</projectDescription>[m[41m[m
[1mdiff --git a/.settings/org.eclipse.core.resources.prefs b/.settings/org.eclipse.core.resources.prefs[m
[1mnew file mode 100644[m
[1mindex 0000000..4824b80[m
[1m--- /dev/null[m
[1m+++ b/.settings/org.eclipse.core.resources.prefs[m
[36m@@ -0,0 +1,2 @@[m
[32m+[m[32meclipse.preferences.version=1[m[41m[m
[32m+[m[32mencoding/<project>=UTF-8[m[41m[m
[1mdiff --git a/.settings/org.eclipse.jdt.core.prefs b/.settings/org.eclipse.jdt.core.prefs[m
[1mnew file mode 100644[m
[1mindex 0000000..54e493c[m
[1m--- /dev/null[m
[1m+++ b/.settings/org.eclipse.jdt.core.prefs[m
[36m@@ -0,0 +1,11 @@[m
[32m+[m[32meclipse.preferences.version=1[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.codegen.inlineJsrBytecode=enabled[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.codegen.targetPlatform=1.6[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.codegen.unusedLocal=preserve[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.compliance=1.6[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.debug.lineNumber=generate[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.debug.localVariable=generate[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.debug.sourceFile=generate[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.problem.assertIdentifier=error[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.problem.enumIdentifier=error[m[41m[m
[32m+[m[32morg.eclipse.jdt.core.compiler.source=1.6[m[41m[m
[1mdiff --git a/build.xml b/build.xml[m
[1mnew file mode 100644[m
[1mindex 0000000..8b91176[m
[1m--- /dev/null[m
[1m+++ b/build.xml[m
[36m@@ -0,0 +1,80 @@[m
[32m+[m[32m<?xml version="1.0" encoding="UTF-8"?>[m[41m[m
[32m+[m[41m[m
[32m+[m[32m<project name="项目名称" basedir="." default="sshexec">[m[41m[m
[32m+[m	[32m<description>本配置文件供ANT编译项目、自动进行单元测试、打包并部署之用。</description>[m[41m[m
[32m+[m	[32m<description>默认操作(输入命令：ant)为编译源程序并发布运行。</description>[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m<!--属性设置-->[m[41m[m
[32m+[m	[32m<property environment="env" />[m[41m[m
[32m+[m	[32m<property file="build.properties" />[m[41m[m
[32m+[m	[32m<property name="src.dir" value="${basedir}/src" />[m[41m[m
[32m+[m	[32m<property name="java.lib.dir" value="${env.JAVA_HOME}/lib" />[m[41m[m
[32m+[m	[32m<property name="classes.dir" value="${basedir}/classes" />[m[41m[m
[32m+[m	[32m<property name="dist.dir" value="${basedir}/dist" />[m[41m[m
[32m+[m	[32m<property name="project.lib.dir" value="${basedir}/lib" />[m[41m[m
[32m+[m	[32m<property name="localpath.dir" value="${basedir}" />[m[41m[m
[32m+[m	[32m<property name="remote.host" value="192.168.114.137"/>[m[41m[m
[32m+[m	[32m<property name="remote.username" value="penger"/>[m[41m[m
[32m+[m	[32m<property name="remote.password" value="1"/>[m[41m[m
[32m+[m	[32m<property name="remote.home" value="/home/penger/software/gp"/>[m[41m[m
[32m+[m	[32m<!--每次需要知道的main类，写到这里-->[m[41m[m
[32m+[m	[32m<property name="main.class" value="atask.tasktwo.Task2"/>[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m<!-- 基本编译路径设置 -->[m[41m[m
[32m+[m	[32m<path id="compile.classpath">[m[41m[m
[32m+[m		[32m<fileset dir="${java.lib.dir}">[m[41m[m
[32m+[m			[32m<include name="tools.jar" />[m[41m[m
[32m+[m		[32m</fileset>[m[41m[m
[32m+[m		[32m<fileset dir="${project.lib.dir}">[m[41m[m
[32m+[m			[32m<include name="*.jar" />[m[41m[m
[32m+[m		[32m</fileset>[m[41m[m
[32m+[m	[32m</path>[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m<!-- 运行路径设置 -->[m[41m[m
[32m+[m	[32m<path id="run.classpath">[m[41m[m
[32m+[m		[32m<path refid="compile.classpath" />[m[41m[m
[32m+[m		[32m<pathelement location="${classes.dir}" />[m[41m[m
[32m+[m	[32m</path>[m[41m[m
[32m+[m	[32m<!-- 清理,删除临时目录 -->[m[41m[m
[32m+[m	[32m<target name="clean" description="清理,删除临时目录">[m[41m[m
[32m+[m		[32m<!--delete dir="${build.dir}" /-->[m[41m[m
[32m+[m		[32m<delete dir="${dist.dir}" />[m[41m[m
[32m+[m		[32m<delete dir="${classes.dir}" />[m[41m[m
[32m+[m		[32m<echo level="info">清理完毕</echo>[m[41m[m
[32m+[m	[32m</target>[m[41m[m
[32m+[m	[32m<!-- 初始化,建立目录,复制文件 -->[m[41m[m
[32m+[m	[32m<target name="init" depends="clean" description="初始化,建立目录,复制文件">[m[41m[m
[32m+[m		[32m<mkdir dir="${classes.dir}" />[m[41m[m
[32m+[m		[32m<mkdir dir="${dist.dir}" />[m[41m[m
[32m+[m	[32m</target>[m[41m[m
[32m+[m	[32m<!-- 编译源文件-->[m[41m[m
[32m+[m	[32m<target name="compile" depends="init" description="编译源文件">[m[41m[m
[32m+[m		[32m<javac srcdir="${src.dir}" destdir="${classes.dir}" source="1.6" target="1.6"  includeAntRuntime="false">[m[41m[m
[32m+[m			[32m<compilerarg line="-encoding UTF-8 "/>[m[41m [m
[32m+[m			[32m<classpath refid="compile.classpath" />[m[41m[m
[32m+[m		[32m</javac>[m[41m[m
[32m+[m	[32m</target>[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m<!-- 打包类文件 -->[m[41m[m
[32m+[m	[32m<target name="jar" depends="compile" description="打包类文件">[m[41m[m
[32m+[m		[32m<jar jarfile="${dist.dir}/jar.jar">[m[41m[m
[32m+[m			[32m<fileset dir="${classes.dir}" includes="**/*.*" />[m[41m[m
[32m+[m		[32m</jar>[m[41m[m
[32m+[m	[32m</target>[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m<!--上传到服务器[m[41m[m
[32m+[m	[32m**需要把lib目录下的jsch-0.1.51拷贝到$ANT_HOME/lib下，如果是Eclipse下的Ant环境必须在Window->Preferences->Ant->Runtime->Classpath中加入jsch-0.1.51。[m[41m[m
[32m+[m	[32m-->[m[41m[m
[32m+[m	[32m<target name="ssh" depends="jar">[m[41m[m
[32m+[m		[32m<scp file="${dist.dir}/jar.jar" todir="${remote.username}@${remote.host}:${remote.home}" password="${remote.password}" trust="true"/>[m[41m[m
[32m+[m	[32m</target>[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m<target name="sshexec" depends="ssh">[m[41m[m
[32m+[m	[32m      <sshexec host="${remote.host}" username="${remote.username}"  password="${remote.password}" trust="true" command=" source /etc/profile; hadoop jar ${remote.home}/jar.jar ${main.class} m.txt output"/>[m[41m[m
[32m+[m		[32m<!--[m[41m [m
[32m+[m	[32m      <sshexec host="${remote.host}" username="${remote.username}"  password="${remote.password}" trust="true" command=" mkdir abc;mkdir cdef"/>[m[41m[m
[32m+[m	[32m      <sshexec host="${remote.host}" username="${remote.username}"  password="${remote.password}" trust="true" command=" source /home/penger/.bashrc"/>[m[41m[m
[32m+[m	[32m      <sshexec host="${remote.host}" username="${remote.username}"  password="${remote.password}" trust="true" command="hadoop jar ${remote.home}/jar.jar ${main.class} m2.txt output"/>[m[41m[m
[32m+[m		[32m-->[m[41m[m
[32m+[m	[32m</target>[m[41m[m
[32m+[m[32m</project>[m
\ No newline at end of file[m
[1mdiff --git a/lib/asm-3.2.jar b/lib/asm-3.2.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..ca9f8d2[m
Binary files /dev/null and b/lib/asm-3.2.jar differ
[1mdiff --git a/lib/aspectjrt-1.6.11.jar b/lib/aspectjrt-1.6.11.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..14fe103[m
Binary files /dev/null and b/lib/aspectjrt-1.6.11.jar differ
[1mdiff --git a/lib/aspectjtools-1.6.11.jar b/lib/aspectjtools-1.6.11.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..7ecf21d[m
Binary files /dev/null and b/lib/aspectjtools-1.6.11.jar differ
[1mdiff --git a/lib/commons-beanutils-1.7.0.jar b/lib/commons-beanutils-1.7.0.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..b1b89c9[m
Binary files /dev/null and b/lib/commons-beanutils-1.7.0.jar differ
[1mdiff --git a/lib/commons-beanutils-core-1.8.0.jar b/lib/commons-beanutils-core-1.8.0.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..87c15f4[m
Binary files /dev/null and b/lib/commons-beanutils-core-1.8.0.jar differ
[1mdiff --git a/lib/commons-cli-1.2.jar b/lib/commons-cli-1.2.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..ce4b9ff[m
Binary files /dev/null and b/lib/commons-cli-1.2.jar differ
[1mdiff --git a/lib/commons-codec-1.4.jar b/lib/commons-codec-1.4.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..458d432[m
Binary files /dev/null and b/lib/commons-codec-1.4.jar differ
[1mdiff --git a/lib/commons-collections-3.2.1.jar b/lib/commons-collections-3.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..c35fa1f[m
Binary files /dev/null and b/lib/commons-collections-3.2.1.jar differ
[1mdiff --git a/lib/commons-configuration-1.6.jar b/lib/commons-configuration-1.6.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..2d4689a[m
Binary files /dev/null and b/lib/commons-configuration-1.6.jar differ
[1mdiff --git a/lib/commons-daemon-1.0.1.jar b/lib/commons-daemon-1.0.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..4e024a8[m
Binary files /dev/null and b/lib/commons-daemon-1.0.1.jar differ
[1mdiff --git a/lib/commons-digester-1.8.jar b/lib/commons-digester-1.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..1110f0a[m
Binary files /dev/null and b/lib/commons-digester-1.8.jar differ
[1mdiff --git a/lib/commons-el-1.0.jar b/lib/commons-el-1.0.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..608ed79[m
Binary files /dev/null and b/lib/commons-el-1.0.jar differ
[1mdiff --git a/lib/commons-httpclient-3.0.1.jar b/lib/commons-httpclient-3.0.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..cfc777c[m
Binary files /dev/null and b/lib/commons-httpclient-3.0.1.jar differ
[1mdiff --git a/lib/commons-io-2.1.jar b/lib/commons-io-2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..b5c7d69[m
Binary files /dev/null and b/lib/commons-io-2.1.jar differ
[1mdiff --git a/lib/commons-lang-2.4.jar b/lib/commons-lang-2.4.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..532939e[m
Binary files /dev/null and b/lib/commons-lang-2.4.jar differ
[1mdiff --git a/lib/commons-logging-1.1.1.jar b/lib/commons-logging-1.1.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..1deef14[m
Binary files /dev/null and b/lib/commons-logging-1.1.1.jar differ
[1mdiff --git a/lib/commons-logging-api-1.0.4.jar b/lib/commons-logging-api-1.0.4.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..ade9a13[m
Binary files /dev/null and b/lib/commons-logging-api-1.0.4.jar differ
[1mdiff --git a/lib/commons-math-2.1.jar b/lib/commons-math-2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..43b4b36[m
Binary files /dev/null and b/lib/commons-math-2.1.jar differ
[1mdiff --git a/lib/commons-net-3.1.jar b/lib/commons-net-3.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..b75f1a5[m
Binary files /dev/null and b/lib/commons-net-3.1.jar differ
[1mdiff --git a/lib/core-3.1.1.jar b/lib/core-3.1.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..ae0b635[m
Binary files /dev/null and b/lib/core-3.1.1.jar differ
[1mdiff --git a/lib/hadoop-ant-1.2.1.jar b/lib/hadoop-ant-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..9a8979a[m
Binary files /dev/null and b/lib/hadoop-ant-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-capacity-scheduler-1.2.1.jar b/lib/hadoop-capacity-scheduler-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..28569aa[m
Binary files /dev/null and b/lib/hadoop-capacity-scheduler-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-client-1.2.1.jar b/lib/hadoop-client-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..3ff0bcd[m
Binary files /dev/null and b/lib/hadoop-client-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-core-1.2.1.jar b/lib/hadoop-core-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..8dce7f9[m
Binary files /dev/null and b/lib/hadoop-core-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-examples-1.2.1.jar b/lib/hadoop-examples-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..b4a3ec4[m
Binary files /dev/null and b/lib/hadoop-examples-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-fairscheduler-1.2.1.jar b/lib/hadoop-fairscheduler-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..143b4ab[m
Binary files /dev/null and b/lib/hadoop-fairscheduler-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-minicluster-1.2.1.jar b/lib/hadoop-minicluster-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..62e2c8b[m
Binary files /dev/null and b/lib/hadoop-minicluster-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-test-1.2.1.jar b/lib/hadoop-test-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..2db4ecf[m
Binary files /dev/null and b/lib/hadoop-test-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-thriftfs-1.2.1.jar b/lib/hadoop-thriftfs-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..bbcb6b4[m
Binary files /dev/null and b/lib/hadoop-thriftfs-1.2.1.jar differ
[1mdiff --git a/lib/hadoop-tools-1.2.1.jar b/lib/hadoop-tools-1.2.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..2ad6459[m
Binary files /dev/null and b/lib/hadoop-tools-1.2.1.jar differ
[1mdiff --git a/lib/hsqldb-1.8.0.10.LICENSE.txt b/lib/hsqldb-1.8.0.10.LICENSE.txt[m
[1mnew file mode 100644[m
[1mindex 0000000..d45b9f8[m
[1m--- /dev/null[m
[1m+++ b/lib/hsqldb-1.8.0.10.LICENSE.txt[m
[36m@@ -0,0 +1,66 @@[m
[32m+[m[32m/* Copyright (c) 1995-2000, The Hypersonic SQL Group.[m[41m[m
[32m+[m[32m * All rights reserved.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Redistribution and use in source and binary forms, with or without[m[41m[m
[32m+[m[32m * modification, are permitted provided that the following conditions are met:[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Redistributions of source code must retain the above copyright notice, this[m[41m[m
[32m+[m[32m * list of conditions and the following disclaimer.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Redistributions in binary form must reproduce the above copyright notice,[m[41m[m
[32m+[m[32m * this list of conditions and the following disclaimer in the documentation[m[41m[m
[32m+[m[32m * and/or other materials provided with the distribution.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Neither the name of the Hypersonic SQL Group nor the names of its[m[41m[m
[32m+[m[32m * contributors may be used to endorse or promote products derived from this[m[41m[m
[32m+[m[32m * software without specific prior written permission.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"[m[41m[m
[32m+[m[32m * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE[m[41m[m
[32m+[m[32m * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE[m[41m[m
[32m+[m[32m * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,[m[41m [m
[32m+[m[32m * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,[m[41m [m
[32m+[m[32m * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,[m[41m [m
[32m+[m[32m * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;[m[41m[m
[32m+[m[32m * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND[m[41m[m
[32m+[m[32m * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT[m[41m[m
[32m+[m[32m * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS[m[41m[m
[32m+[m[32m * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * This software consists of voluntary contributions made by many individuals[m[41m [m
[32m+[m[32m * on behalf of the Hypersonic SQL Group.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * For work added by the HSQL Development Group:[m[41m[m
[32m+[m[32m *[m[41m [m
[32m+[m[32m * Copyright (c) 2001-2004, The HSQL Development Group[m[41m[m
[32m+[m[32m * All rights reserved.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Redistribution and use in source and binary forms, with or without[m[41m[m
[32m+[m[32m * modification, are permitted provided that the following conditions are met:[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Redistributions of source code must retain the above copyright notice, this[m[41m[m
[32m+[m[32m * list of conditions and the following disclaimer.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Redistributions in binary form must reproduce the above copyright notice,[m[41m[m
[32m+[m[32m * this list of conditions and the following disclaimer in the documentation[m[41m[m
[32m+[m[32m * and/or other materials provided with the distribution.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * Neither the name of the HSQL Development Group nor the names of its[m[41m[m
[32m+[m[32m * contributors may be used to endorse or promote products derived from this[m[41m[m
[32m+[m[32m * software without specific prior written permission.[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"[m[41m[m
[32m+[m[32m * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE[m[41m[m
[32m+[m[32m * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE[m[41m[m
[32m+[m[32m * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,[m[41m [m
[32m+[m[32m * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,[m[41m [m
[32m+[m[32m * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,[m[41m [m
[32m+[m[32m * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;[m[41m[m
[32m+[m[32m * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND[m[41m[m
[32m+[m[32m * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT[m[41m[m
[32m+[m[32m * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS[m[41m[m
[32m+[m[32m * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[1mdiff --git a/lib/hsqldb-1.8.0.10.jar b/lib/hsqldb-1.8.0.10.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..e010269[m
Binary files /dev/null and b/lib/hsqldb-1.8.0.10.jar differ
[1mdiff --git a/lib/jackson-core-asl-1.8.8.jar b/lib/jackson-core-asl-1.8.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..05f3353[m
Binary files /dev/null and b/lib/jackson-core-asl-1.8.8.jar differ
[1mdiff --git a/lib/jackson-mapper-asl-1.8.8.jar b/lib/jackson-mapper-asl-1.8.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..7c7cd21[m
Binary files /dev/null and b/lib/jackson-mapper-asl-1.8.8.jar differ
[1mdiff --git a/lib/jasper-compiler-5.5.12.jar b/lib/jasper-compiler-5.5.12.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..2a410b4[m
Binary files /dev/null and b/lib/jasper-compiler-5.5.12.jar differ
[1mdiff --git a/lib/jasper-runtime-5.5.12.jar b/lib/jasper-runtime-5.5.12.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..743d906[m
Binary files /dev/null and b/lib/jasper-runtime-5.5.12.jar differ
[1mdiff --git a/lib/jdeb-0.8.jar b/lib/jdeb-0.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..7f1f5c8[m
Binary files /dev/null and b/lib/jdeb-0.8.jar differ
[1mdiff --git a/lib/jersey-core-1.8.jar b/lib/jersey-core-1.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..871e163[m
Binary files /dev/null and b/lib/jersey-core-1.8.jar differ
[1mdiff --git a/lib/jersey-json-1.8.jar b/lib/jersey-json-1.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..2584c09[m
Binary files /dev/null and b/lib/jersey-json-1.8.jar differ
[1mdiff --git a/lib/jersey-server-1.8.jar b/lib/jersey-server-1.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..bab28ff[m
Binary files /dev/null and b/lib/jersey-server-1.8.jar differ
[1mdiff --git a/lib/jets3t-0.6.1.jar b/lib/jets3t-0.6.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..e4048dd[m
Binary files /dev/null and b/lib/jets3t-0.6.1.jar differ
[1mdiff --git a/lib/jetty-6.1.26.jar b/lib/jetty-6.1.26.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..2cbe07a[m
Binary files /dev/null and b/lib/jetty-6.1.26.jar differ
[1mdiff --git a/lib/jetty-util-6.1.26.jar b/lib/jetty-util-6.1.26.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..cd23752[m
Binary files /dev/null and b/lib/jetty-util-6.1.26.jar differ
[1mdiff --git a/lib/jsch-0.1.42.jar b/lib/jsch-0.1.42.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..c65eff0[m
Binary files /dev/null and b/lib/jsch-0.1.42.jar differ
[1mdiff --git a/lib/jsch-0.1.51.jar b/lib/jsch-0.1.51.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..725745f[m
Binary files /dev/null and b/lib/jsch-0.1.51.jar differ
[1mdiff --git a/lib/junit-4.5.jar b/lib/junit-4.5.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..7339216[m
Binary files /dev/null and b/lib/junit-4.5.jar differ
[1mdiff --git a/lib/kfs-0.2.2.jar b/lib/kfs-0.2.2.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..aa32e74[m
Binary files /dev/null and b/lib/kfs-0.2.2.jar differ
[1mdiff --git a/lib/kfs-0.2.LICENSE.txt b/lib/kfs-0.2.LICENSE.txt[m
[1mnew file mode 100644[m
[1mindex 0000000..d645695[m
[1m--- /dev/null[m
[1m+++ b/lib/kfs-0.2.LICENSE.txt[m
[36m@@ -0,0 +1,202 @@[m
[32m+[m
[32m+[m[32m                                 Apache License[m
[32m+[m[32m                           Version 2.0, January 2004[m
[32m+[m[32m                        http://www.apache.org/licenses/[m
[32m+[m
[32m+[m[32m   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION[m
[32m+[m
[32m+[m[32m   1. Definitions.[m
[32m+[m
[32m+[m[32m      "License" shall mean the terms and conditions for use, reproduction,[m
[32m+[m[32m      and distribution as defined by Sections 1 through 9 of this document.[m
[32m+[m
[32m+[m[32m      "Licensor" shall mean the copyright owner or entity authorized by[m
[32m+[m[32m      the copyright owner that is granting the License.[m
[32m+[m
[32m+[m[32m      "Legal Entity" shall mean the union of the acting entity and all[m
[32m+[m[32m      other entities that control, are controlled by, or are under common[m
[32m+[m[32m      control with that entity. For the purposes of this definition,[m
[32m+[m[32m      "control" means (i) the power, direct or indirect, to cause the[m
[32m+[m[32m      direction or management of such entity, whether by contract or[m
[32m+[m[32m      otherwise, or (ii) ownership of fifty percent (50%) or more of the[m
[32m+[m[32m      outstanding shares, or (iii) beneficial ownership of such entity.[m
[32m+[m
[32m+[m[32m      "You" (or "Your") shall mean an individual or Legal Entity[m
[32m+[m[32m      exercising permissions granted by this License.[m
[32m+[m
[32m+[m[32m      "Source" form shall mean the preferred form for making modifications,[m
[32m+[m[32m      including but not limited to software source code, documentation[m
[32m+[m[32m      source, and configuration files.[m
[32m+[m
[32m+[m[32m      "Object" form shall mean any form resulting from mechanical[m
[32m+[m[32m      transformation or translation of a Source form, including but[m
[32m+[m[32m      not limited to compiled object code, generated documentation,[m
[32m+[m[32m      and conversions to other media types.[m
[32m+[m
[32m+[m[32m      "Work" shall mean the work of authorship, whether in Source or[m
[32m+[m[32m      Object form, made available under the License, as indicated by a[m
[32m+[m[32m      copyright notice that is included in or attached to the work[m
[32m+[m[32m      (an example is provided in the Appendix below).[m
[32m+[m
[32m+[m[32m      "Derivative Works" shall mean any work, whether in Source or Object[m
[32m+[m[32m      form, that is based on (or derived from) the Work and for which the[m
[32m+[m[32m      editorial revisions, annotations, elaborations, or other modifications[m
[32m+[m[32m      represent, as a whole, an original work of authorship. For the purposes[m
[32m+[m[32m      of this License, Derivative Works shall not include works that remain[m
[32m+[m[32m      separable from, or merely link (or bind by name) to the interfaces of,[m
[32m+[m[32m      the Work and Derivative Works thereof.[m
[32m+[m
[32m+[m[32m      "Contribution" shall mean any work of authorship, including[m
[32m+[m[32m      the original version of the Work and any modifications or additions[m
[32m+[m[32m      to that Work or Derivative Works thereof, that is intentionally[m
[32m+[m[32m      submitted to Licensor for inclusion in the Work by the copyright owner[m
[32m+[m[32m      or by an individual or Legal Entity authorized to submit on behalf of[m
[32m+[m[32m      the copyright owner. For the purposes of this definition, "submitted"[m
[32m+[m[32m      means any form of electronic, verbal, or written communication sent[m
[32m+[m[32m      to the Licensor or its representatives, including but not limited to[m
[32m+[m[32m      communication on electronic mailing lists, source code control systems,[m
[32m+[m[32m      and issue tracking systems that are managed by, or on behalf of, the[m
[32m+[m[32m      Licensor for the purpose of discussing and improving the Work, but[m
[32m+[m[32m      excluding communication that is conspicuously marked or otherwise[m
[32m+[m[32m      designated in writing by the copyright owner as "Not a Contribution."[m
[32m+[m
[32m+[m[32m      "Contributor" shall mean Licensor and any individual or Legal Entity[m
[32m+[m[32m      on behalf of whom a Contribution has been received by Licensor and[m
[32m+[m[32m      subsequently incorporated within the Work.[m
[32m+[m
[32m+[m[32m   2. Grant of Copyright License. Subject to the terms and conditions of[m
[32m+[m[32m      this License, each Contributor hereby grants to You a perpetual,[m
[32m+[m[32m      worldwide, non-exclusive, no-charge, royalty-free, irrevocable[m
[32m+[m[32m      copyright license to reproduce, prepare Derivative Works of,[m
[32m+[m[32m      publicly display, publicly perform, sublicense, and distribute the[m
[32m+[m[32m      Work and such Derivative Works in Source or Object form.[m
[32m+[m
[32m+[m[32m   3. Grant of Patent License. Subject to the terms and conditions of[m
[32m+[m[32m      this License, each Contributor hereby grants to You a perpetual,[m
[32m+[m[32m      worldwide, non-exclusive, no-charge, royalty-free, irrevocable[m
[32m+[m[32m      (except as stated in this section) patent license to make, have made,[m
[32m+[m[32m      use, offer to sell, sell, import, and otherwise transfer the Work,[m
[32m+[m[32m      where such license applies only to those patent claims licensable[m
[32m+[m[32m      by such Contributor that are necessarily infringed by their[m
[32m+[m[32m      Contribution(s) alone or by combination of their Contribution(s)[m
[32m+[m[32m      with the Work to which such Contribution(s) was submitted. If You[m
[32m+[m[32m      institute patent litigation against any entity (including a[m
[32m+[m[32m      cross-claim or counterclaim in a lawsuit) alleging that the Work[m
[32m+[m[32m      or a Contribution incorporated within the Work constitutes direct[m
[32m+[m[32m      or contributory patent infringement, then any patent licenses[m
[32m+[m[32m      granted to You under this License for that Work shall terminate[m
[32m+[m[32m      as of the date such litigation is filed.[m
[32m+[m
[32m+[m[32m   4. Redistribution. You may reproduce and distribute copies of the[m
[32m+[m[32m      Work or Derivative Works thereof in any medium, with or without[m
[32m+[m[32m      modifications, and in Source or Object form, provided that You[m
[32m+[m[32m      meet the following conditions:[m
[32m+[m
[32m+[m[32m      (a) You must give any other recipients of the Work or[m
[32m+[m[32m          Derivative Works a copy of this License; and[m
[32m+[m
[32m+[m[32m      (b) You must cause any modified files to carry prominent notices[m
[32m+[m[32m          stating that You changed the files; and[m
[32m+[m
[32m+[m[32m      (c) You must retain, in the Source form of any Derivative Works[m
[32m+[m[32m          that You distribute, all copyright, patent, trademark, and[m
[32m+[m[32m          attribution notices from the Source form of the Work,[m
[32m+[m[32m          excluding those notices that do not pertain to any part of[m
[32m+[m[32m          the Derivative Works; and[m
[32m+[m
[32m+[m[32m      (d) If the Work includes a "NOTICE" text file as part of its[m
[32m+[m[32m          distribution, then any Derivative Works that You distribute must[m
[32m+[m[32m          include a readable copy of the attribution notices contained[m
[32m+[m[32m          within such NOTICE file, excluding those notices that do not[m
[32m+[m[32m          pertain to any part of the Derivative Works, in at least one[m
[32m+[m[32m          of the following places: within a NOTICE text file distributed[m
[32m+[m[32m          as part of the Derivative Works; within the Source form or[m
[32m+[m[32m          documentation, if provided along with the Derivative Works; or,[m
[32m+[m[32m          within a display generated by the Derivative Works, if and[m
[32m+[m[32m          wherever such third-party notices normally appear. The contents[m
[32m+[m[32m          of the NOTICE file are for informational purposes only and[m
[32m+[m[32m          do not modify the License. You may add Your own attribution[m
[32m+[m[32m          notices within Derivative Works that You distribute, alongside[m
[32m+[m[32m          or as an addendum to the NOTICE text from the Work, provided[m
[32m+[m[32m          that such additional attribution notices cannot be construed[m
[32m+[m[32m          as modifying the License.[m
[32m+[m
[32m+[m[32m      You may add Your own copyright statement to Your modifications and[m
[32m+[m[32m      may provide additional or different license terms and conditions[m
[32m+[m[32m      for use, reproduction, or distribution of Your modifications, or[m
[32m+[m[32m      for any such Derivative Works as a whole, provided Your use,[m
[32m+[m[32m      reproduction, and distribution of the Work otherwise complies with[m
[32m+[m[32m      the conditions stated in this License.[m
[32m+[m
[32m+[m[32m   5. Submission of Contributions. Unless You explicitly state otherwise,[m
[32m+[m[32m      any Contribution intentionally submitted for inclusion in the Work[m
[32m+[m[32m      by You to the Licensor shall be under the terms and conditions of[m
[32m+[m[32m      this License, without any additional terms or conditions.[m
[32m+[m[32m      Notwithstanding the above, nothing herein shall supersede or modify[m
[32m+[m[32m      the terms of any separate license agreement you may have executed[m
[32m+[m[32m      with Licensor regarding such Contributions.[m
[32m+[m
[32m+[m[32m   6. Trademarks. This License does not grant permission to use the trade[m
[32m+[m[32m      names, trademarks, service marks, or product names of the Licensor,[m
[32m+[m[32m      except as required for reasonable and customary use in describing the[m
[32m+[m[32m      origin of the Work and reproducing the content of the NOTICE file.[m
[32m+[m
[32m+[m[32m   7. Disclaimer of Warranty. Unless required by applicable law or[m
[32m+[m[32m      agreed to in writing, Licensor provides the Work (and each[m
[32m+[m[32m      Contributor provides its Contributions) on an "AS IS" BASIS,[m
[32m+[m[32m      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or[m
[32m+[m[32m      implied, including, without limitation, any warranties or conditions[m
[32m+[m[32m      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A[m
[32m+[m[32m      PARTICULAR PURPOSE. You are solely responsible for determining the[m
[32m+[m[32m      appropriateness of using or redistributing the Work and assume any[m
[32m+[m[32m      risks associated with Your exercise of permissions under this License.[m
[32m+[m
[32m+[m[32m   8. Limitation of Liability. In no event and under no legal theory,[m
[32m+[m[32m      whether in tort (including negligence), contract, or otherwise,[m
[32m+[m[32m      unless required by applicable law (such as deliberate and grossly[m
[32m+[m[32m      negligent acts) or agreed to in writing, shall any Contributor be[m
[32m+[m[32m      liable to You for damages, including any direct, indirect, special,[m
[32m+[m[32m      incidental, or consequential damages of any character arising as a[m
[32m+[m[32m      result of this License or out of the use or inability to use the[m
[32m+[m[32m      Work (including but not limited to damages for loss of goodwill,[m
[32m+[m[32m      work stoppage, computer failure or malfunction, or any and all[m
[32m+[m[32m      other commercial damages or losses), even if such Contributor[m
[32m+[m[32m      has been advised of the possibility of such damages.[m
[32m+[m
[32m+[m[32m   9. Accepting Warranty or Additional Liability. While redistributing[m
[32m+[m[32m      the Work or Derivative Works thereof, You may choose to offer,[m
[32m+[m[32m      and charge a fee for, acceptance of support, warranty, indemnity,[m
[32m+[m[32m      or other liability obligations and/or rights consistent with this[m
[32m+[m[32m      License. However, in accepting such obligations, You may act only[m
[32m+[m[32m      on Your own behalf and on Your sole responsibility, not on behalf[m
[32m+[m[32m      of any other Contributor, and only if You agree to indemnify,[m
[32m+[m[32m      defend, and hold each Contributor harmless for any liability[m
[32m+[m[32m      incurred by, or claims asserted against, such Contributor by reason[m
[32m+[m[32m      of your accepting any such warranty or additional liability.[m
[32m+[m
[32m+[m[32m   END OF TERMS AND CONDITIONS[m
[32m+[m
[32m+[m[32m   APPENDIX: How to apply the Apache License to your work.[m
[32m+[m
[32m+[m[32m      To apply the Apache License to your work, attach the following[m
[32m+[m[32m      boilerplate notice, with the fields enclosed by brackets "[]"[m
[32m+[m[32m      replaced with your own identifying information. (Don't include[m
[32m+[m[32m      the brackets!)  The text should be enclosed in the appropriate[m
[32m+[m[32m      comment syntax for the file format. We also recommend that a[m
[32m+[m[32m      file or class name and description of purpose be included on the[m
[32m+[m[32m      same "printed page" as the copyright notice for easier[m
[32m+[m[32m      identification within third-party archives.[m
[32m+[m
[32m+[m[32m   Copyright [yyyy] [name of copyright owner][m
[32m+[m
[32m+[m[32m   Licensed under the Apache License, Version 2.0 (the "License");[m
[32m+[m[32m   you may not use this file except in compliance with the License.[m
[32m+[m[32m   You may obtain a copy of the License at[m
[32m+[m
[32m+[m[32m       http://www.apache.org/licenses/LICENSE-2.0[m
[32m+[m
[32m+[m[32m   Unless required by applicable law or agreed to in writing, software[m
[32m+[m[32m   distributed under the License is distributed on an "AS IS" BASIS,[m
[32m+[m[32m   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.[m
[32m+[m[32m   See the License for the specific language governing permissions and[m
[32m+[m[32m   limitations under the License.[m
[1mdiff --git a/lib/log4j-1.2.14.jar b/lib/log4j-1.2.14.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..6251307[m
Binary files /dev/null and b/lib/log4j-1.2.14.jar differ
[1mdiff --git a/lib/log4j-1.2.15.jar b/lib/log4j-1.2.15.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..c930a6a[m
Binary files /dev/null and b/lib/log4j-1.2.15.jar differ
[1mdiff --git a/lib/log4j-over-slf4j-1.6.1.jar b/lib/log4j-over-slf4j-1.6.1.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..504541e[m
Binary files /dev/null and b/lib/log4j-over-slf4j-1.6.1.jar differ
[1mdiff --git a/lib/mockito-all-1.8.5.jar b/lib/mockito-all-1.8.5.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..4b0395e[m
Binary files /dev/null and b/lib/mockito-all-1.8.5.jar differ
[1mdiff --git a/lib/oro-2.0.8.jar b/lib/oro-2.0.8.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..23488d2[m
Binary files /dev/null and b/lib/oro-2.0.8.jar differ
[1mdiff --git a/lib/servlet-api-2.5-20081211.jar b/lib/servlet-api-2.5-20081211.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..b0537c4[m
Binary files /dev/null and b/lib/servlet-api-2.5-20081211.jar differ
[1mdiff --git a/lib/slf4j-api-1.4.3.jar b/lib/slf4j-api-1.4.3.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..b34fe8d[m
Binary files /dev/null and b/lib/slf4j-api-1.4.3.jar differ
[1mdiff --git a/lib/slf4j-log4j12-1.4.3.jar b/lib/slf4j-log4j12-1.4.3.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..70082fc[m
Binary files /dev/null and b/lib/slf4j-log4j12-1.4.3.jar differ
[1mdiff --git a/lib/xmlenc-0.52.jar b/lib/xmlenc-0.52.jar[m
[1mnew file mode 100644[m
[1mindex 0000000..ec568b4[m
Binary files /dev/null and b/lib/xmlenc-0.52.jar differ
[1mdiff --git a/resources/log4j.properties b/resources/log4j.properties[m
[1mnew file mode 100644[m
[1mindex 0000000..9a7fd19[m
[1m--- /dev/null[m
[1m+++ b/resources/log4j.properties[m
[36m@@ -0,0 +1,24 @@[m
[32m+[m[32m#  Logging level[m[41m[m
[32m+[m[32msolr.log=d:/logs/[m[41m[m
[32m+[m[32mlog4j.rootLogger=INFO, file, CONSOLE[m[41m[m
[32m+[m[41m[m
[32m+[m[32mlog4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender[m[41m[m
[32m+[m[41m[m
[32m+[m[32mlog4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout[m[41m[m
[32m+[m[32mlog4j.appender.CONSOLE.layout.ConversionPattern=%-4r [%t] %-5p %c %x \u2013 %m%n[m[41m[m
[32m+[m[41m[m
[32m+[m[32m#- size rotation with log cleanup.[m[41m[m
[32m+[m[32mlog4j.appender.file=org.apache.log4j.RollingFileAppender[m[41m[m
[32m+[m[32mlog4j.appender.file.MaxFileSize=4MB[m[41m[m
[32m+[m[32mlog4j.appender.file.MaxBackupIndex=9[m[41m[m
[32m+[m[41m[m
[32m+[m[32m#- File to log to and log format[m[41m[m
[32m+[m[32mlog4j.appender.file.File=${solr.log}/solr.log[m[41m[m
[32m+[m[32mlog4j.appender.file.layout=org.apache.log4j.PatternLayout[m[41m[m
[32m+[m[32mlog4j.appender.file.layout.ConversionPattern=%-5p - %d{yyyy-MM-dd HH:mm:ss.SSS}; %C; %m\n[m[41m[m
[32m+[m[41m[m
[32m+[m[32mlog4j.logger.org.apache.zookeeper=WARN[m[41m[m
[32m+[m[32mlog4j.logger.org.apache.hadoop=WARN[m[41m[m
[32m+[m[41m[m
[32m+[m[32m# set to INFO to enable infostream log messages[m[41m[m
[32m+[m[32mlog4j.logger.org.apache.solr.update.LoggingInfoStream=OFF[m[41m[m
[1mdiff --git a/src/atask/A.java b/src/atask/A.java[m
[1mnew file mode 100644[m
[1mindex 0000000..3e99217[m
[1m--- /dev/null[m
[1m+++ b/src/atask/A.java[m
[36m@@ -0,0 +1,10 @@[m
[32m+[m[32mpackage atask;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class A {[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) {[m[41m[m
[32m+[m		[32m// TODO Auto-generated method stub[m[41m[m
[32m+[m		[32mSystem.out.println("阿");[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/atask/task.txt b/src/atask/task.txt[m
[1mnew file mode 100644[m
[1mindex 0000000..2300442[m
[1m--- /dev/null[m
[1m+++ b/src/atask/task.txt[m
[36m@@ -0,0 +1,79 @@[m
[32m+[m[32m1.InputFormat是用于处理各种数据源的。下面实现自定义的InputFormat，数据源是来自于内存。[m[41m[m
[32m+[m[32m1.1 在程序的job.setInputFormatClass(MySelfInputFormat.class);[m[41m[m
[32m+[m[32m1.2 实现InputFormat extends InputFormat<k,v>，实现其中的2个方法，分别是getSplits(..)和createRecordReader(..)[m[41m[m
[32m+[m[32m1.3 getSplits(...)返回的是java.util.List<T>，里面中的每个元素是InputSplit。每个InputSpilt对应一个mapper任务。[m[41m[m
[32m+[m[32m1.4 InputSplit是对原始海量数据源的划分。本例中是在内存中产生数据，封装到InputSplit中。[m[41m[m
[32m+[m[32m1.5 InputSplit封装的必须是hadoop数据类型，实现Writable接口。[m[41m[m
[32m+[m[32m1.6 RecordReader读取每个InputSplit中的数据，解析成一个个的<k,v>，供map处理。[m[41m[m
[32m+[m[32m1.7 RecordReader有4个核心方法，分别是initialize(...)，nextKeyValue(),getCurrentKey()和getCurrentValue()。[m[41m[m
[32m+[m[32m1.8 initialize(...)的重要性在于拿到InputSplit和定义临时变量。[m[41m[m
[32m+[m[32m1.9 nextKeyValue(...)方法的每次调用可以获得key和value值[m[41m[m
[32m+[m[32m1.10 当nextKeyValue(...)调用后，紧接着调用getCurrentKey()和getCurrentValue()。[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m作业1：自定义InputFormat，实现从linux本地文件/etc/profile读取数据，然后进行单词统计。[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m2.OutputFormat是用于处理各种输出目的地的。[m[41m[m
[32m+[m[32m2.1 OutputFormat需要写出去的键值对，是来自于Reducer类，是通过RecordWriter获得的。[m[41m[m
[32m+[m[32m2.2 RecordWriter中的write(...)方法只有k和v，写到哪里去哪？这要通过单独传入OutputStream来处理。write就是把k和v写入到OutputStream中的。[m[41m[m
[32m+[m[32m2.3 RecordWriter类位于OutputFormat中的。因此，我们自定义的OutputFromat必须继承OutputFormat类型。那么，流对象必须在getRecordWriter(...)方法中获得。[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m作业2：自定义OutputFormat，实现单词统计，把输出记录写入到2个文件中，文件名分别是a和b。[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m作业3：对以下三列数据进行降序排列(第一列相同时，第二列降序；第二列相同时，第三列降序)[m[41m[m
[32m+[m[32m3	3	3[m[41m[m
[32m+[m[32m3	2	4[m[41m[m
[32m+[m[32m3	2	0[m[41m[m
[32m+[m[32m2	2	1[m[41m[m
[32m+[m[32m2	1	4[m[41m[m
[32m+[m[32m1	1	0[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m作业4：在自定义分组的例子中，按照第二列分组，取最大值/最小值[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[32m3.reduce端join[m[41m[m
[32m+[m[32m-------file1[ID	NAME]--------[m[41m  [m
[32m+[m[32m1	zhangsan[m[41m[m
[32m+[m[32m2	lisi[m[41m[m
[32m+[m[32m3	wangwu[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[32m-------file2[ID	VALUE]--------[m[41m[m
[32m+[m[32m1	45[m[41m[m
[32m+[m[32m2	56[m[41m[m
[32m+[m[32m3	89[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[32m-------结果[NAME VALUE]------------[m[41m[m
[32m+[m[32mzhagnsan	45[m[41m[m
[32m+[m[32mlisi	56[m[41m[m
[32m+[m[32mwangwu	89[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[32m问：当map读取原始文件时，能不能区分出是file1还是file2？[m[41m[m
[32m+[m[32m答：能。FileSplit fileSplit = (FileSplit)context.getInputSplit();[m[41m[m
[32m+[m[32m        String path =  fileSplit.getPath().toString();[m[41m[m
[32m+[m[41m[m
[32m+[m[32m问：map阶段如何打标记？[m[41m[m
[32m+[m[32m答：当我们判断出是file1时，对v2做标记，让v2的值是#zhagnsan；如果是fiel2是，让v2的值是*45[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m作业5：使用reduce端join方法完成统计[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[32m4.map端join[m[41m[m
[32m+[m[32m适用场景：小表可以全部读取放到内存中。两个在内存中装不下的大表，不适合map端join。[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[32m在一个TaskTracker中可以运行多个map任务。每个map任务是一个java进程，如果每个map从HDFS中读取相同的小表内容，就有些浪费了。[m[41m[m
[32m+[m[32m使用DistributedCache，小表内容可以加载在TaskTracker的linux磁盘上。每个map运行时只需要从linux磁盘加载数据就行了，不必每次从HDFS加载。[m[41m[m
[32m+[m[41m[m
[32m+[m[32m问：如何使用DistributedCache哪？[m[41m[m
[32m+[m[32m答：1.把文件上传到HDFS中[m[41m[m
[32m+[m[32m    2.在job.waitForCompletion(...)代码之前写DistributedCache.addCacheFile(hdfs路径, conf);[m[41m[m
[32m+[m[32m    3.在MyMapper类的setup(...)方法中使用DistributedCache.getLocalCacheFiles()获得文件的路径，读取文件内容[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m[41m[m
[32m+[m[32m作业6：数据文件还是作业5的，把file1上传到hdfs中，使用DistributedCache完成统计[m[41m[m
[32m+[m[32m--------------------------------------------------------------------------------[m
\ No newline at end of file[m
[1mdiff --git a/src/atask/taskone/Task1.java b/src/atask/taskone/Task1.java[m
[1mnew file mode 100644[m
[1mindex 0000000..406ccb5[m
[1m--- /dev/null[m
[1m+++ b/src/atask/taskone/Task1.java[m
[36m@@ -0,0 +1,109 @@[m
[32m+[m[32mpackage atask.taskone;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.util.Iterator;[m[41m[m
[32m+[m[32mimport java.util.StringTokenizer;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobClient;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobConf;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.MapReduceBase;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.OutputCollector;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reporter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.TextInputFormat;[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * 自定义InputFormat ,从linux本地文件/etc/profile读取数据然后进行数据统计[m[41m[m
[32m+[m[32m * @author Esc_penger[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class Task1 {[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mfinal FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.114.137:9000"), new Configuration());[m[41m[m
[32m+[m		[32m//1.将数据从linux本地磁盘上传到hdfs中[m[41m[m
[32m+[m		[32mString localFilename="/etc/profile";[m[41m[m
[32m+[m		[32mString hdfsFilename="/input/profile";[m[41m[m
[32m+[m		[32mString hdfsFilenameOutput="/output";[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mcleanhdfsworkspace(fileSystem, hdfsFilename, hdfsFilenameOutput);[m[41m[m
[32m+[m[32m//		copyLocalFile2hdfs(localFilename,hdfsFilename,fileSystem);[m[41m[m
[32m+[m		[32m//2.自定义mapper,reducer[m[41m[m
[32m+[m		[32m//3.执行方法[m[41m[m
[32m+[m		[32mJobConf jobConf = new JobConf(Task1.class);[m[41m[m
[32m+[m		[32mjobConf.setMapperClass(CountMapper.class);[m[41m[m
[32m+[m		[32mjobConf.setReducerClass(CountReducer.class);[m[41m[m
[32m+[m		[32mjobConf.setInputFormat(TextInputFormat.class);[m[41m[m
[32m+[m		[32mjobConf.setInputFormat(TextInputFormat.class);[m[41m[m
[32m+[m		[32mjobConf.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjobConf.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjobConf.setJobName("name");[m[41m[m
[32m+[m		[32mFileInputFormat.setInputPaths(jobConf, new Path(hdfsFilename));[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(jobConf, new Path(hdfsFilenameOutput));[m[41m[m
[32m+[m		[32mJobClient.runJob(jobConf);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mprivate static void cleanhdfsworkspace(FileSystem fileSystem, String inputpath,String outputpath) throws Exception{[m[41m[m
[32m+[m		[32m//输入路径保证有文件[m[41m[m
[32m+[m		[32mboolean checkFileExist = checkFileExist(inputpath, fileSystem);[m[41m[m
[32m+[m		[32mif(!checkFileExist){[m[41m[m
[32m+[m			[32mthrow new Exception();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m		[32m//输出路径保证为空[m[41m[m
[32m+[m		[32mif(checkFileExist(outputpath,fileSystem)){[m[41m[m
[32m+[m			[32mfileSystem.deleteOnExit(new Path(outputpath));[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mprivate static boolean checkFileExist(String hdfsfilename,FileSystem fileSystem) throws IOException{[m[41m[m
[32m+[m		[32mreturn fileSystem.exists(new Path(hdfsfilename));[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mprivate static void copyLocalFile2hdfs(String localFilename,[m[41m[m
[32m+[m			[32mString hdfsFilename,FileSystem fileSystem) throws IOException {[m[41m[m
[32m+[m		[32mfileSystem.copyFromLocalFile(new Path(localFilename), new Path(hdfsFilename));[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static  class  CountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void map(LongWritable key, Text value,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> collector, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mStringTokenizer stringTokenizer = new StringTokenizer(value.toString());[m[41m[m
[32m+[m			[32mString next="";[m[41m[m
[32m+[m			[32mwhile(stringTokenizer.hasMoreTokens()){[m[41m[m
[32m+[m				[32mnext = stringTokenizer.nextToken();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcollector.collect(new Text(next), new LongWritable(1l));[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class CountReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void reduce(Text key, Iterator<LongWritable> values,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> collector, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mLongWritable countvalue=null;[m[41m[m
[32m+[m			[32mlong sum=0l;[m[41m[m
[32m+[m			[32mwhile(values.hasNext()){[m[41m[m
[32m+[m				[32mLongWritable next = values.next();[m[41m[m
[32m+[m				[32msum+=next.get();[m[41m[m
[32m+[m				[32mcountvalue=new LongWritable(sum);[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcollector.collect(key, countvalue);[m[41m	[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/atask/taskthree/Task3.java b/src/atask/taskthree/Task3.java[m
[1mnew file mode 100644[m
[1mindex 0000000..24a850b[m
[1m--- /dev/null[m
[1m+++ b/src/atask/taskthree/Task3.java[m
[36m@@ -0,0 +1,20 @@[m
[32m+[m[32mpackage atask.taskthree;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.MapReduceBase;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reducer;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class Task3 {[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static  class MyMapper extends MapReduceBase implements Mapper<K1, V1, K2, V2>{[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends MapReduceBase implements Reducer<K2, V2, K3, V3>{[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static void  main[m[41m [m
[32m+[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/atask/tasktwo/Task2.java b/src/atask/tasktwo/Task2.java[m
[1mnew file mode 100644[m
[1mindex 0000000..a461bb8[m
[1m--- /dev/null[m
[1m+++ b/src/atask/tasktwo/Task2.java[m
[36m@@ -0,0 +1,225 @@[m
[32m+[m[32mpackage atask.tasktwo;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataOutputStream;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.io.UnsupportedEncodingException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.util.Iterator;[m[41m[m
[32m+[m[32mimport java.util.StringTokenizer;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FSDataOutputStream;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.NullWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.compress.CompressionCodec;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.compress.GzipCodec;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.InputSplit;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobClient;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobConf;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.MapReduceBase;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.OutputCollector;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.RecordReader;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.RecordWriter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reporter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.TextInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.OutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.TaskAttemptContext;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.util.Progressable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.util.ReflectionUtils;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport atask.taskone.Task1;[m[41m[m
[32m+[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * 自定义OutputFormat，实现单词统计，把输出记录写入到2个文件中，文件名分别是a和b[m[41m[m
[32m+[m[32m * @author Esc_penger[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class Task2 {[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mfinal FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.114.137:9000"), new Configuration());[m[41m[m
[32m+[m		[32m//1.将数据从linux本地磁盘上传到hdfs中[m[41m[m
[32m+[m		[32mString localFilename="/etc/profile";[m[41m[m
[32m+[m		[32mString hdfsFilename="/input/profile";[m[41m[m
[32m+[m		[32mString hdfsFilenameOutput="/output";[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mcleanhdfsworkspace(fileSystem, hdfsFilename, hdfsFilenameOutput);[m[41m[m
[32m+[m[32m//		copyLocalFile2hdfs(localFilename,hdfsFilename,fileSystem);[m[41m[m
[32m+[m		[32m//2.自定义mapper,reducer[m[41m[m
[32m+[m		[32m//3.执行方法[m[41m[m
[32m+[m		[32mJobConf jobConf = new JobConf(Task1.class);[m[41m[m
[32m+[m		[32mjobConf.setMapperClass(CountMapper.class);[m[41m[m
[32m+[m		[32mjobConf.setReducerClass(CountReducer.class);[m[41m[m
[32m+[m		[32mjobConf.setInputFormat(TextInputFormat.class);[m[41m[m
[32m+[m		[32mjobConf.setOutputFormat(MyOutputFormat.class);[m[41m[m
[32m+[m		[32mjobConf.setInputFormat(TextInputFormat.class);[m[41m[m
[32m+[m		[32mjobConf.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjobConf.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjobConf.setJobName("name");[m[41m[m
[32m+[m		[32mFileInputFormat.setInputPaths(jobConf, new Path(hdfsFilename));[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(jobConf, new Path(hdfsFilenameOutput));[m[41m[m
[32m+[m		[32mJobClient.runJob(jobConf);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mprivate static void cleanhdfsworkspace(FileSystem fileSystem, String inputpath,String outputpath) throws Exception{[m[41m[m
[32m+[m		[32m//输入路径保证有文件[m[41m[m
[32m+[m		[32mboolean checkFileExist = checkFileExist(inputpath, fileSystem);[m[41m[m
[32m+[m		[32mif(!checkFileExist){[m[41m[m
[32m+[m			[32mthrow new Exception();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m		[32m//输出路径保证为空[m[41m[m
[32m+[m		[32mif(checkFileExist(outputpath,fileSystem)){[m[41m[m
[32m+[m			[32mfileSystem.deleteOnExit(new Path(outputpath));[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mprivate static boolean checkFileExist(String hdfsfilename,FileSystem fileSystem) throws IOException{[m[41m[m
[32m+[m		[32mreturn fileSystem.exists(new Path(hdfsfilename));[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mprivate static void copyLocalFile2hdfs(String localFilename,[m[41m[m
[32m+[m			[32mString hdfsFilename,FileSystem fileSystem) throws IOException {[m[41m[m
[32m+[m		[32mfileSystem.copyFromLocalFile(new Path(localFilename), new Path(hdfsFilename));[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static  class  CountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void map(LongWritable key, Text value,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> collector, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mStringTokenizer stringTokenizer = new StringTokenizer(value.toString());[m[41m[m
[32m+[m			[32mString next="";[m[41m[m
[32m+[m			[32mwhile(stringTokenizer.hasMoreTokens()){[m[41m[m
[32m+[m				[32mnext = stringTokenizer.nextToken();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcollector.collect(new Text(next), new LongWritable(1l));[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class CountReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void reduce(Text key, Iterator<LongWritable> values,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> collector, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mLongWritable countvalue=null;[m[41m[m
[32m+[m			[32mlong sum=0l;[m[41m[m
[32m+[m			[32mwhile(values.hasNext()){[m[41m[m
[32m+[m				[32mLongWritable next = values.next();[m[41m[m
[32m+[m				[32msum+=next.get();[m[41m[m
[32m+[m				[32mcountvalue=new LongWritable(sum);[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcollector.collect(key, countvalue);[m[41m	[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyOutputFormat <K, V> extends FileOutputFormat<K, V>{[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mprotected static class MyRecordWriter <K,V>implements RecordWriter<K, V>{[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mprivate static final String utf8="UTF-8";[m[41m[m
[32m+[m			[32m//bytes[m[41m[m
[32m+[m			[32mprivate static byte[] newline;[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mprotected DataOutputStream out;[m[41m[m
[32m+[m			[32mprivate static byte[] keyValueSeparator;[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mstatic{[m[41m[m
[32m+[m					[32mtry {[m[41m[m
[32m+[m						[32m//设置分隔符为--------[m[41m[m
[32m+[m						[32mkeyValueSeparator="----------".getBytes(utf8);[m[41m[m
[32m+[m						[32mnewline="/n".getBytes(utf8);[m[41m[m
[32m+[m					[32m} catch (UnsupportedEncodingException e) {[m[41m[m
[32m+[m						[32me.printStackTrace();[m[41m[m
[32m+[m					[32m}[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m[41m[m
[32m+[m			[32mpublic MyRecordWriter(DataOutputStream out) {[m[41m[m
[32m+[m				[32msuper();[m[41m[m
[32m+[m				[32mthis.out = out;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m			[32m@Override[m[41m[m
[32m+[m			[32mpublic synchronized void write(K key, V value) throws IOException {[m[41m[m
[32m+[m				[32m  boolean nullKey = key == null || key instanceof NullWritable;[m[41m[m
[32m+[m			[32m      boolean nullValue = value == null || value instanceof NullWritable;[m[41m[m
[32m+[m			[32m      if (nullKey && nullValue) {[m[41m[m
[32m+[m			[32m        return;[m[41m[m
[32m+[m			[32m      }[m[41m[m
[32m+[m			[32m      if (!nullKey) {[m[41m[m
[32m+[m			[32m        writeObject(key);[m[41m[m
[32m+[m			[32m      }[m[41m[m
[32m+[m			[32m      if (!(nullKey || nullValue)) {[m[41m[m
[32m+[m			[32m        out.write(keyValueSeparator);[m[41m[m
[32m+[m			[32m      }[m[41m[m
[32m+[m			[32m      if (!nullValue) {[m[41m[m
[32m+[m			[32m        writeObject(value);[m[41m[m
[32m+[m			[32m      }[m[41m[m
[32m+[m			[32m      out.write(newline);[m[41m[m
[32m+[m			[32m      out.write(newline);[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m    private void writeObject(Object o) throws IOException {[m[41m[m
[32m+[m		[32m        if (o instanceof Text) {[m[41m[m
[32m+[m		[32m          Text to = (Text) o;[m[41m[m
[32m+[m		[32m          out.write(to.getBytes(), 0, to.getLength());[m[41m[m
[32m+[m		[32m        } else {[m[41m[m
[32m+[m		[32m          out.write(o.toString().getBytes(utf8));[m[41m[m
[32m+[m		[32m        }[m[41m[m
[32m+[m		[32m      }[m[41m[m
[32m+[m[41m[m
[32m+[m			[32m@Override[m[41m[m
[32m+[m			[32mpublic synchronized void close(Reporter reporter) throws IOException {[m[41m[m
[32m+[m				[32mout.close();[m[41m[m
[32m+[m[41m				[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m			[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic RecordWriter<K, V> getRecordWriter(FileSystem ignored,[m[41m[m
[32m+[m				[32mJobConf job, String name, Progressable progress)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mboolean isCompressed = getCompressOutput(job);[m[41m[m
[32m+[m			[32mString keyValueSeparator = job.get("mapred.textoutputformat.separator",[m[41m [m
[32m+[m[32m                    "\t");[m[41m[m
[32m+[m			[32m//是不是压缩结果[m[41m[m
[32m+[m			[32mif(!isCompressed){[m[41m[m
[32m+[m				[32mname="abc";[m[41m[m
[32m+[m				[32mPath file = FileOutputFormat.getTaskOutputPath(job, name);[m[41m[m
[32m+[m				[32mFileSystem fileSystem = file.getFileSystem(job);[m[41m[m
[32m+[m				[32mFSDataOutputStream fileOut = fileSystem.create(file, progress);[m[41m[m
[32m+[m				[32mreturn new MyRecordWriter<K, V>(fileOut);[m[41m[m
[32m+[m			[32m}else{[m[41m[m
[32m+[m				[32mClass<? extends CompressionCodec> codeClass = getOutputCompressorClass(job, GzipCodec.class);[m[41m[m
[32m+[m				[32mCompressionCodec codec = ReflectionUtils.newInstance(codeClass, job);[m[41m[m
[32m+[m				[32mPath file = FileOutputFormat.getTaskOutputPath(job, name+codec.getDefaultExtension());[m[41m[m
[32m+[m				[32mFileSystem fileSystem = file.getFileSystem(job);[m[41m[m
[32m+[m				[32mFSDataOutputStream fileOut = fileSystem.create(file,progress);[m[41m[m
[32m+[m				[32mDataOutputStream outputStream = new DataOutputStream(codec.createOutputStream(fileOut));[m[41m[m
[32m+[m				[32mreturn new MyRecordWriter<K, V>(outputStream);[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/cmd/WordCountApp.java b/src/cmd/WordCountApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..7b5bb4c[m
[1m--- /dev/null[m
[1m+++ b/src/cmd/WordCountApp.java[m
[36m@@ -0,0 +1,97 @@[m
[32m+[m[32mpackage cmd;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.util.StringTokenizer;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configured;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.util.Tool;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.util.ToolRunner;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class WordCountApp extends Configured implements Tool{[m[41m[m
[32m+[m	[32mprivate static String INPUT_PATH = null;[m[41m[m
[32m+[m	[32mprivate static String OUT_PATH = null;[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic int run(String[] args) throws Exception {[m[41m[m
[32m+[m		[32mINPUT_PATH = args[0];[m[41m[m
[32m+[m		[32mOUT_PATH = args[1];[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mConfiguration conf = getConf();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m		[32mreturn 0;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mToolRunner.run(new Configuration(), new WordCountApp(), args);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mStringTokenizer stringTokenizer = new StringTokenizer(value.toString());[m[41m[m
[32m+[m			[32mwhile(stringTokenizer.hasMoreElements()){[m[41m[m
[32m+[m				[32mString nextToken = stringTokenizer.nextToken();[m[41m[m
[32m+[m				[32mcontext.write(new Text(nextToken), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[32m//			final String line = value.toString();[m[41m[m
[32m+[m[32m//			final String[] splited = line.split("\t");[m[41m[m
[32m+[m[32m//[m[41m			[m
[32m+[m[32m//			//产生的<k,v>对少了[m[41m[m
[32m+[m[32m//			for (String word : splited) {[m[41m[m
[32m+[m[32m//				//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m[32m//				context.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m[32m//			}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/combiner/WordCountApp.java b/src/combiner/WordCountApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..d208606[m
[1m--- /dev/null[m
[1m+++ b/src/combiner/WordCountApp.java[m
[36m@@ -0,0 +1,87 @@[m
[32m+[m[32mpackage combiner;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * hello	you[m[41m[m
[32m+[m[32m * hello	me[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class WordCountApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32m//问：为什么使用Combiner？答：目的是减少map端的输出，意味着shuffle时传输的数据量小，网络开销小了[m[41m[m
[32m+[m		[32m//问:使用combiner有什么限制？[m[41m[m
[32m+[m		[32mjob.setCombinerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m		[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/counter/WordCountApp.java b/src/counter/WordCountApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..b306df9[m
[1m--- /dev/null[m
[1m+++ b/src/counter/WordCountApp.java[m
[36m@@ -0,0 +1,86 @@[m
[32m+[m[32mpackage counter;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Counter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * hello	you[m[41m[m
[32m+[m[32m * hello	me[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class WordCountApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mfinal Counter counter = context.getCounter("Sensitive Words", "hello");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mif(line.contains("hello")) {[m[41m[m
[32m+[m				[32mcounter.increment(1L);[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/group/GroupApp.java b/src/group/GroupApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..4ef0411[m
[1m--- /dev/null[m
[1m+++ b/src/group/GroupApp.java[m
[36m@@ -0,0 +1,128 @@[m
[32m+[m[32mpackage group;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataInput;[m[41m[m
[32m+[m[32mimport java.io.DataOutput;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.RawComparator;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.WritableComparable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.WritableComparator;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class GroupApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/data";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , GroupApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(GroupApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(NewK2.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setGroupingComparatorClass(MyGroupComparator.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,NewK2,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mcontext.write(new NewK2(Long.parseLong(splited[0]), Long.parseLong(splited[1])), new LongWritable(Long.parseLong(splited[1])));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable>{[m[41m[m
[32m+[m		[32mprotected void reduce(NewK2 key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<NewK2,LongWritable,LongWritable,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mlong min = Long.MAX_VALUE;[m[41m[m
[32m+[m			[32mfor (LongWritable longWritable : values) {[m[41m[m
[32m+[m				[32mif(longWritable.get()<min) {[m[41m[m
[32m+[m					[32mmin = longWritable.get();[m[41m[m
[32m+[m				[32m}[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mcontext.write(new LongWritable(key.first), new LongWritable(min));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class NewK2 implements WritableComparable<NewK2>{[m[41m[m
[32m+[m		[32mlong first;[m[41m[m
[32m+[m		[32mlong second;[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mpublic NewK2() {[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mpublic NewK2(long first, long second) {[m[41m[m
[32m+[m			[32mthis.first = first;[m[41m[m
[32m+[m			[32mthis.second = second;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m			[32mout.writeLong(this.first);[m[41m[m
[32m+[m			[32mout.writeLong(this.second);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m			[32mthis.first = in.readLong();[m[41m[m
[32m+[m			[32mthis.second = in.readLong();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic int compareTo(NewK2 o) {[m[41m[m
[32m+[m			[32mfinal long minus = this.first - o.first;[m[41m[m
[32m+[m			[32mif(minus!=0) {[m[41m[m
[32m+[m				[32mreturn (int)minus;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mreturn (int)(this.second - o.second);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyGroupComparator implements RawComparator<NewK2>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic int compare(NewK2 o1, NewK2 o2) {[m[41m[m
[32m+[m			[32mreturn 0;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m		[32m/**[m[41m[m
[32m+[m		[32m * b1	表示第1个参与比较的字节数组[m[41m[m
[32m+[m		[32m * s1	表示第1个字节数组中开始比较的位置[m[41m[m
[32m+[m		[32m * l1	表示第1个字节数组参与比较的字节长度[m[41m[m
[32m+[m		[32m * b2	表示第2个参与比较的字节数组[m[41m[m
[32m+[m		[32m * s2	表示第2个字节数组中开始比较的位置[m[41m[m
[32m+[m		[32m * l2	表示第2个字节数组参与比较的字节长度[m[41m[m
[32m+[m		[32m */[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {[m[41m[m
[32m+[m			[32mreturn WritableComparator.compareBytes(b1, s1, 8, b2, s2, 8);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/group/data b/src/group/data[m
[1mnew file mode 100644[m
[1mindex 0000000..aab6501[m
[1m--- /dev/null[m
[1m+++ b/src/group/data[m
[36m@@ -0,0 +1,10 @@[m
[32m+[m[32m3	3[m[41m[m
[32m+[m[32m3	2[m[41m[m
[32m+[m[32m3	1[m[41m[m
[32m+[m[32m2	2[m[41m[m
[32m+[m[32m2	1[m[41m[m
[32m+[m[32m1	1[m[41m[m
[32m+[m[32m-------------[m[41m[m
[32m+[m[32m1	1[m[41m[m
[32m+[m[32m2	2[m[41m[m
[32m+[m[32m3	3[m
\ No newline at end of file[m
[1mdiff --git a/src/hbase/BatchImport.java b/src/hbase/BatchImport.java[m
[1mnew file mode 100644[m
[1mindex 0000000..a2c3b99[m
[1m--- /dev/null[m
[1m+++ b/src/hbase/BatchImport.java[m
[36m@@ -0,0 +1,93 @@[m
[32m+[m[32m//package hbase;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import java.text.SimpleDateFormat;[m[41m[m
[32m+[m[32m//import java.util.Date;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.Put;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.mapreduce.TableReducer;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.util.Bytes;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.NullWritable;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//public class BatchImport {[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	public static void main(String[] args) throws Exception {[m[41m[m
[32m+[m[32m//		final Configuration configuration = new Configuration();[m[41m[m
[32m+[m[32m//		// 设置zookeeper[m[41m[m
[32m+[m[32m//		configuration.set("hbase.zookeeper.quorum", "hadoop0");[m[41m[m
[32m+[m[32m//		// 设置hbase表名称[m[41m[m
[32m+[m[32m//		configuration.set(TableOutputFormat.OUTPUT_TABLE, "wlan_log");[m[41m[m
[32m+[m[32m//		// 将该值改大，防止hbase超时退出[m[41m[m
[32m+[m[32m//		configuration.set("dfs.socket.timeout", "180000");[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		final Job job = new Job(configuration, "HBaseBatchImport");[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		job.setMapperClass(BatchImportMapper.class);[m[41m[m
[32m+[m[32m//		job.setReducerClass(BatchImportReducer.class);[m[41m[m
[32m+[m[32m//		// 设置map的输出，不设置reduce的输出类型[m[41m[m
[32m+[m[32m//		job.setMapOutputKeyClass(LongWritable.class);[m[41m[m
[32m+[m[32m//		job.setMapOutputValueClass(Text.class);[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		job.setInputFormatClass(TextInputFormat.class);[m[41m[m
[32m+[m[32m//		// 不再设置输出路径，而是设置输出格式类型[m[41m[m
[32m+[m[32m//		job.setOutputFormatClass(TableOutputFormat.class);[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		FileInputFormat.setInputPaths(job, "hdfs://hadoop0:9000/input");[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		job.waitForCompletion(true);[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	static class BatchImportMapper extends[m[41m[m
[32m+[m[32m//			Mapper<LongWritable, Text, LongWritable, Text> {[m[41m[m
[32m+[m[32m//		SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyyMMddHHmmss");[m[41m[m
[32m+[m[32m//		Text v2 = new Text();[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		protected void map(LongWritable key, Text value, Context context)[m[41m[m
[32m+[m[32m//				throws java.io.IOException, InterruptedException {[m[41m[m
[32m+[m[32m//			final String[] splited = value.toString().split("\t");[m[41m[m
[32m+[m[32m//			try {[m[41m[m
[32m+[m[32m//				//把long型转换为格式化字符串[m[41m[m
[32m+[m[32m//				final Date date = new Date(Long.parseLong(splited[0].trim()));[m[41m[m
[32m+[m[32m//				final String dateFormat = dateformat1.format(date);[m[41m[m
[32m+[m[32m//[m[41m				[m
[32m+[m[32m//				String rowKey = splited[1] + ":" + dateFormat;[m[41m[m
[32m+[m[32m//				v2.set(rowKey + "\t" + value.toString());[m[41m[m
[32m+[m[32m//				context.write(key, v2);[m[41m[m
[32m+[m[32m//			} catch (NumberFormatException e) {[m[41m[m
[32m+[m[32m//				final org.apache.hadoop.mapreduce.Counter counter = context.getCounter("BatchImport",[m[41m[m
[32m+[m[32m//						"ErrorFormat");[m[41m[m
[32m+[m[32m//				counter.increment(1L);[m[41m[m
[32m+[m[32m//				System.out.println("出错了" + splited[0] + " " + e.getMessage());[m[41m[m
[32m+[m[32m//			}[m[41m[m
[32m+[m[32m//		};[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	static class BatchImportReducer extends[m[41m[m
[32m+[m[32m//			TableReducer<LongWritable, Text, NullWritable> {[m[41m[m
[32m+[m[32m//		protected void reduce(LongWritable key,[m[41m[m
[32m+[m[32m//				java.lang.Iterable<Text> values, Context context)[m[41m[m
[32m+[m[32m//				throws java.io.IOException, InterruptedException {[m[41m[m
[32m+[m[32m//			for (Text text : values) {[m[41m[m
[32m+[m[32m//				final String[] splited = text.toString().split("\t");[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//				//行键[m[41m[m
[32m+[m[32m//				final String rowKey = splited[0];[m[41m[m
[32m+[m[32m//[m[41m				[m
[32m+[m[32m//				final Put put = new Put(Bytes.toBytes(rowKey));[m[41m[m
[32m+[m[32m//				//把第1个字段写出去[m[41m[m
[32m+[m[32m//				put.add(Bytes.toBytes("cf"), Bytes.toBytes("date"),Bytes.toBytes(splited[1]));[m[41m[m
[32m+[m[32m//				put.add(Bytes.toBytes("cf"), Bytes.toBytes("phonenum"),Bytes.toBytes(splited[2]));[m[41m[m
[32m+[m[32m//				// 省略其他字段，调用put.add(....)即可[m[41m[m
[32m+[m[32m//				context.write(NullWritable.get(), put);[m[41m[m
[32m+[m[32m//			}[m[41m[m
[32m+[m[32m//		};[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//}[m[41m[m
[1mdiff --git a/src/hbase/HBaseTest.java b/src/hbase/HBaseTest.java[m
[1mnew file mode 100644[m
[1mindex 0000000..9809829[m
[1m--- /dev/null[m
[1m+++ b/src/hbase/HBaseTest.java[m
[36m@@ -0,0 +1,97 @@[m
[32m+[m[32m//package hbase;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import java.io.IOException;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.HBaseConfiguration;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.HColumnDescriptor;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.HTableDescriptor;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.Delete;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.Get;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.HBaseAdmin;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.HTable;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.Put;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.Result;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.ResultScanner;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.client.Scan;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.hbase.util.Bytes;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m////HBaseAdmin 管理hbase的，主要负责DDL操作[m[41m[m
[32m+[m[32m////HTable 管理表中数据的，主要负责DML操作[m[41m[m
[32m+[m[32m//public class HBaseTest {[m[41m[m
[32m+[m[32m//	public static final String TABLE_NAME = "t1";[m[41m[m
[32m+[m[32m//	public static final String ROW_KEY = "rk1";[m[41m[m
[32m+[m[32m//[m[41m	[m
[32m+[m[32m//	public static void main(String[] args) throws Exception{[m[41m[m
[32m+[m[32m//		//ddl(TABLE_NAME, "f1");[m[41m[m
[32m+[m[32m//		dml();[m[41m[m
[32m+[m[32m//		scan();[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m	[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	private static void scan() throws IOException {[m[41m[m
[32m+[m[32m//		final Configuration conf = getConf();[m[41m[m
[32m+[m[32m//		final HTable hTable = new HTable(conf, TABLE_NAME);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		//使用scan对象可以设定startRow、stopRow[m[41m[m
[32m+[m[32m//		Scan scan = new Scan();[m[41m[m
[32m+[m[32m//		//scan.setStartRow(startRow);[m[41m[m
[32m+[m[32m//		//scan.setStopRow(stopRow);[m[41m[m
[32m+[m[32m//		//scan.setFilter(filter);[m[41m[m
[32m+[m[32m//		final ResultScanner scanner = hTable.getScanner(scan);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		//指定列簇、列[m[41m[m
[32m+[m[32m//		//final ResultScanner scanner = hTable.getScanner(Bytes.toBytes("f1"), Bytes.toBytes("c1"));[m[41m[m
[32m+[m[32m//		for (Result result : scanner) {[m[41m[m
[32m+[m[32m//			System.out.println(result);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		hTable.close();[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	private static void dml() throws IOException {[m[41m[m
[32m+[m[32m//		final Configuration conf = getConf();[m[41m[m
[32m+[m[32m//		final HTable hTable = new HTable(conf, TABLE_NAME);[m[41m[m
[32m+[m[32m//		Put put = new Put(Bytes.toBytes(ROW_KEY));[m[41m[m
[32m+[m[32m//		put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes("张三"));[m[41m[m
[32m+[m[32m//		hTable.put(put);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		Get get = new Get(Bytes.toBytes(ROW_KEY));[m[41m[m
[32m+[m[32m//		final Result result = hTable.get(get);[m[41m[m
[32m+[m[32m//		final Object valueString = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1")));[m[41m[m
[32m+[m[32m//		//System.out.println(valueString);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		//Delete delete = new Delete(Bytes.toBytes(ROW_KEY));[m[41m[m
[32m+[m[32m//		//hTable.delete(delete);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		hTable.close();[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m	[m
[32m+[m[32m//	public static void ddl(String tableName, String... familyNames) throws Exception{[m[41m[m
[32m+[m[32m//		final Configuration conf = getConf();[m[41m[m
[32m+[m[32m//		final HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		//创建表[m[41m[m
[32m+[m[32m//		HTableDescriptor htableDesc = new HTableDescriptor(tableName);[m[41m[m
[32m+[m[32m//		for (String familyName : familyNames) {[m[41m[m
[32m+[m[32m//			HColumnDescriptor familyDesc = new HColumnDescriptor(familyName);[m[41m[m
[32m+[m[32m//			htableDesc.addFamily(familyDesc);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//		if(!hBaseAdmin.tableExists(TABLE_NAME)) {[m[41m[m
[32m+[m[32m//			hBaseAdmin.createTable(htableDesc);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		System.out.println("表是否存在"+hBaseAdmin.tableExists(tableName));[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		//hBaseAdmin.disableTable(TABLE_NAME);[m[41m[m
[32m+[m[32m//		//hBaseAdmin.deleteTable(TABLE_NAME);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		hBaseAdmin.close();[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	private static Configuration getConf() {[m[41m[m
[32m+[m[32m//		final Configuration conf = HBaseConfiguration.create();[m[41m[m
[32m+[m[32m//		conf.setStrings("hbase.zookeeper.quorum", "hadoop0");[m[41m[m
[32m+[m[32m//		return conf;[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//}[m[41m[m
[1mdiff --git a/src/hdfs/App1.java b/src/hdfs/App1.java[m
[1mnew file mode 100644[m
[1mindex 0000000..f27f886[m
[1m--- /dev/null[m
[1m+++ b/src/hdfs/App1.java[m
[36m@@ -0,0 +1,26 @@[m
[32m+[m[32mpackage hdfs;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.InputStream;[m[41m[m
[32m+[m[32mimport java.net.URL;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FsUrlStreamHandlerFactory;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.IOUtils;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class App1 {[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception {[m[41m[m
[32m+[m		[32mURL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal URL url = new URL("hdfs://chaoren1:9000/hello2");[m[41m[m
[32m+[m		[32mfinal InputStream is = url.openStream();[m[41m[m
[32m+[m		[32m  /**[m[41m[m
[32m+[m		[32m   * Copies from one stream to another.[m[41m[m
[32m+[m		[32m   * @param in 输入流[m[41m[m
[32m+[m		[32m   * @param out 输出流[m[41m[m
[32m+[m		[32m   * @param bufferSize 换成区大小[m[41m [m
[32m+[m		[32m   * @param close 是否关闭流[m[41m[m
[32m+[m		[32m   */[m[41m[m
[32m+[m		[32mIOUtils.copyBytes(is, System.out, 1024, true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/hdfs/App2.java b/src/hdfs/App2.java[m
[1mnew file mode 100644[m
[1mindex 0000000..be3e5b2[m
[1m--- /dev/null[m
[1m+++ b/src/hdfs/App2.java[m
[36m@@ -0,0 +1,62 @@[m
[32m+[m[32mpackage hdfs;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.FileInputStream;[m[41m[m
[32m+[m[32mimport java.io.FileNotFoundException;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FSDataInputStream;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FSDataOutputStream;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileStatus;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.IOUtils;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class App2 {[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mfinal FileSystem fileSystem = FileSystem.get(new URI("hdfs://chaoren1:9000"), new Configuration());[m[41m[m
[32m+[m		[32mSystem.out.println(fileSystem.getClass().getName());[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m		[m
[32m+[m		[32m//创建文件夹[m[41m[m
[32m+[m		[32mmkdir(fileSystem);[m[41m[m
[32m+[m		[32m//上传[m[41m[m
[32m+[m		[32mputdata(fileSystem);[m[41m[m
[32m+[m		[32m//下载[m[41m[m
[32m+[m		[32mdownload(fileSystem);[m[41m[m
[32m+[m		[32m//查看目录列表[m[41m[m
[32m+[m		[32mfinal FileStatus[] listStatuses = fileSystem.listStatus(new Path("/"));[m[41m[m
[32m+[m		[32mfor (FileStatus fileStatus : listStatuses) {[m[41m[m
[32m+[m			[32mSystem.out.println(fileStatus.getPath().toString());[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m		[32mSystem.out.println("-------------------------");[m[41m[m
[32m+[m		[32m//删除[m[41m[m
[32m+[m		[32mfinal boolean isDeleted = fileSystem.delete(new Path("/dir1"), true);[m[41m[m
[32m+[m		[32mif(isDeleted) {[m[41m[m
[32m+[m			[32mSystem.out.println("删除成功");[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mprivate static void download(final FileSystem fileSystem)[m[41m[m
[32m+[m			[32mthrows IOException {[m[41m[m
[32m+[m		[32mfinal FSDataInputStream in = fileSystem.open(new Path("/dir1/readme"));[m[41m[m
[32m+[m		[32mIOUtils.copyBytes(in, System.out, 1024, false);[m[41m[m
[32m+[m		[32min.close();[m[41m[m
[32m+[m		[32mSystem.out.println("-------------------------");[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mprivate static void putdata(final FileSystem fileSystem)[m[41m[m
[32m+[m			[32mthrows IOException, FileNotFoundException {[m[41m[m
[32m+[m		[32mfinal FSDataOutputStream out = fileSystem.create(new Path("/dir1/readme"));[m[41m[m
[32m+[m		[32mfinal FileInputStream in = new FileInputStream("/mnt/home/cr00/data/hello");[m[41m[m
[32m+[m		[32mIOUtils.copyBytes(in, out, 1024, true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mprivate static void mkdir(final FileSystem fileSystem) throws IOException {[m[41m[m
[32m+[m		[32mfinal boolean successful = fileSystem.mkdirs(new Path("/dir1"));[m[41m[m
[32m+[m		[32mif(successful) {[m[41m[m
[32m+[m			[32mSystem.out.println("创建目录成功");[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/inputformat/MyDBInputFormatApp.java b/src/inputformat/MyDBInputFormatApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..527e0ad[m
[1m--- /dev/null[m
[1m+++ b/src/inputformat/MyDBInputFormatApp.java[m
[36m@@ -0,0 +1,103 @@[m
[32m+[m[32mpackage inputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataInput;[m[41m[m
[32m+[m[32mimport java.io.DataOutput;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.sql.PreparedStatement;[m[41m[m
[32m+[m[32mimport java.sql.ResultSet;[m[41m[m
[32m+[m[32mimport java.sql.SQLException;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport mapreduce.WordCountApp;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.NullWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Writable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.db.DBConfiguration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.db.DBInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.db.DBWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * 要运行本示例[m[41m[m
[32m+[m[32m * 1.把mysql的jdbc驱动放到各TaskTracker节点的lib目录下[m[41m[m
[32m+[m[32m * 2.重启集群[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class MyDBInputFormatApp {[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://hadoop0:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mDBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop0:3306/test", "root", "admin");[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setInputFormatClass(DBInputFormat.class);[m[41m[m
[32m+[m		[32mDBInputFormat.setInput(job, MyUser.class, "myuser", null, null, "id", "name");[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(NullWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m[m
[32m+[m		[32mjob.setNumReduceTasks(0);		//指定不需要使用reduce，直接把map输出写入到HDFS中[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(NullWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, MyUser, Text, NullWritable>{[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, MyUser value, org.apache.hadoop.mapreduce.Mapper<LongWritable,MyUser,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mcontext.write(new Text(value.toString()), NullWritable.get());[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyUser implements Writable, DBWritable{[m[41m[m
[32m+[m		[32mint id;[m[41m[m
[32m+[m		[32mString name;[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m			[32mout.writeInt(id);[m[41m[m
[32m+[m			[32mText.writeString(out, name);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m			[32mthis.id = in.readInt();[m[41m[m
[32m+[m			[32mthis.name = Text.readString(in);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void write(PreparedStatement statement) throws SQLException {[m[41m[m
[32m+[m			[32mstatement.setInt(1, id);[m[41m[m
[32m+[m			[32mstatement.setString(2, name);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void readFields(ResultSet resultSet) throws SQLException {[m[41m[m
[32m+[m			[32mthis.id = resultSet.getInt(1);[m[41m[m
[32m+[m			[32mthis.name = resultSet.getString(2);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic String toString() {[m[41m[m
[32m+[m			[32mreturn id + "\t" + name;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/inputformat/MyDBInputFormatApp2.java b/src/inputformat/MyDBInputFormatApp2.java[m
[1mnew file mode 100644[m
[1mindex 0000000..3ae11c6[m
[1m--- /dev/null[m
[1m+++ b/src/inputformat/MyDBInputFormatApp2.java[m
[36m@@ -0,0 +1,124 @@[m
[32m+[m[32m//package inputformat;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import java.io.DataInput;[m[41m[m
[32m+[m[32m//import java.io.DataOutput;[m[41m[m
[32m+[m[32m//import java.io.IOException;[m[41m[m
[32m+[m[32m//import java.net.URI;[m[41m[m
[32m+[m[32m//import java.sql.PreparedStatement;[m[41m[m
[32m+[m[32m//import java.sql.ResultSet;[m[41m[m
[32m+[m[32m//import java.sql.SQLException;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import mapreduce.WordCountApp;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//import org.apache.hadoop.filecache.DistributedCache;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.NullWritable;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.io.Writable;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.FileOutputFormat;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.JobClient;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.JobConf;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.MapReduceBase;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.Mapper;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.OutputCollector;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.Reporter;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.lib.db.DBConfiguration;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.lib.db.DBInputFormat;[m[41m[m
[32m+[m[32m//import org.apache.hadoop.mapred.lib.db.DBWritable;[m[41m[m
[32m+[m[32m//import org.apache.log4j.Level;[m[41m[m
[32m+[m[32m//import org.apache.log4j.LogManager;[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m///**[m[41m[m
[32m+[m[32m// * 要运行本示例[m[41m[m
[32m+[m[32m// * 1.把mysql的jdbc驱动放到各TaskTracker节点的lib目录下[m[41m[m
[32m+[m[32m// * 2.重启集群[m[41m[m
[32m+[m[32m// *[m[41m[m
[32m+[m[32m// */[m[41m[m
[32m+[m[32m//public class MyDBInputFormatApp2 {[m[41m[m
[32m+[m[32m//	private static final String OUT_PATH = "hdfs://crxy0:9000/out";[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//	public static void main(String[] args) throws Exception{[m[41m[m
[32m+[m[32m//		LogManager.getRootLogger().setLevel(Level.toLevel(Level.DEBUG_INT));[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		final JobConf job = new JobConf(WordCountApp.class);[m[41m[m
[32m+[m[32m//		job.setBoolean("keep.failed.task.files", true);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		DistributedCache.addArchiveToClassPath(new Path("hdfs://crxy0:9000/tmp/mysql-connector-java-5.1.10.jar"), job);[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		final String s1 = job.get(DBConfiguration.DRIVER_CLASS_PROPERTY);[m[41m[m
[32m+[m[32m//		System.out.println(s1);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), job);[m[41m[m
[32m+[m[32m//		filesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		job.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver", "jdbc:mysql://crxy2:3306/test", "root", "admin");[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		job.setInputFormat(DBInputFormat.class);[m[41m[m
[32m+[m[32m//		DBInputFormat.setInput(job, MyUser.class, "myuser", null, null, "id", "name");[m[41m[m
[32m+[m[32m//		job.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m[32m//		job.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m[32m//		job.setMapOutputValueClass(NullWritable.class);[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		job.setNumReduceTasks(0);		//指定不需要使用reduce，直接把map输出写入到HDFS中[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		job.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m[32m//		job.setOutputValueClass(NullWritable.class);[m[41m[m
[32m+[m[32m//		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//[m[41m	[m
[32m+[m[32m//		JobClient.runJob(job);[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m	[m
[32m+[m[32m//	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, MyUser, Text, NullWritable>{[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		@Override[m[41m[m
[32m+[m[32m//		public void map(LongWritable key, MyUser value,[m[41m[m
[32m+[m[32m//				OutputCollector<Text, NullWritable> collector, Reporter reporter)[m[41m[m
[32m+[m[32m//				throws IOException {[m[41m[m
[32m+[m[32m//			collector.collect(new Text(value.toString()), NullWritable.get());[m[41m[m
[32m+[m[32m//		};[m[41m[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//[m[41m	[m
[32m+[m[32m//	public static class MyUser implements Writable, DBWritable{[m[41m[m
[32m+[m[32m//		int id;[m[41m[m
[32m+[m[32m//		String name;[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//		@Override[m[41m[m
[32m+[m[32m//		public void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m[32m//			out.writeInt(id);[m[41m[m
[32m+[m[32m//			Text.writeString(out, name);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		@Override[m[41m[m
[32m+[m[32m//		public void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m[32m//			this.id = in.readInt();[m[41m[m
[32m+[m[32m//			this.name = Text.readString(in);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		@Override[m[41m[m
[32m+[m[32m//		public void write(PreparedStatement statement) throws SQLException {[m[41m[m
[32m+[m[32m//			statement.setInt(1, id);[m[41m[m
[32m+[m[32m//			statement.setString(2, name);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		@Override[m[41m[m
[32m+[m[32m//		public void readFields(ResultSet resultSet) throws SQLException {[m[41m[m
[32m+[m[32m//			this.id = resultSet.getInt(1);[m[41m[m
[32m+[m[32m//			this.name = resultSet.getString(2);[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m[m
[32m+[m[32m//		@Override[m[41m[m
[32m+[m[32m//		public String toString() {[m[41m[m
[32m+[m[32m//			return id + "\t" + name;[m[41m[m
[32m+[m[32m//		}[m[41m[m
[32m+[m[32m//[m[41m		[m
[32m+[m[32m//	}[m[41m[m
[32m+[m[32m//}[m[41m[m
[1mdiff --git a/src/inputformat/MyGenericWritableApp.java b/src/inputformat/MyGenericWritableApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..642d7a1[m
[1m--- /dev/null[m
[1m+++ b/src/inputformat/MyGenericWritableApp.java[m
[36m@@ -0,0 +1,113 @@[m
[32m+[m[32mpackage inputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport mapreduce.WordCountApp;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.GenericWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.IntWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Writable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.MultipleInputs;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.TextInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class MyGenericWritableApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mMultipleInputs.addInputPath(job, new Path("hdfs://chaoren1:9000/files/hello"), KeyValueTextInputFormat.class, MyMapper.class);[m[41m[m
[32m+[m		[32mMultipleInputs.addInputPath(job, new Path("hdfs://chaoren1:9000/files/hello2"), TextInputFormat.class, MyMapper2.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m		[m
[32m+[m		[32m//job.setMapperClass(MyMapper.class);	//不应该有这一行[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(MyGenericWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<Text, Text, Text, MyGenericWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<Text,Text,Text,MyGenericWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m				[32mcontext.write(key, new MyGenericWritable(new LongWritable(1)));[m[41m[m
[32m+[m				[32mcontext.write(value, new MyGenericWritable(new LongWritable(1)));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper2 extends Mapper<LongWritable, Text, Text, MyGenericWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,MyGenericWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split(",");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mfinal Text text = new Text("1");[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new MyGenericWritable(text));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, MyGenericWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<MyGenericWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,MyGenericWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (MyGenericWritable times : values) {[m[41m[m
[32m+[m				[32mfinal Writable writable = times.get();[m[41m[m
[32m+[m				[32mif(writable instanceof LongWritable) {[m[41m[m
[32m+[m					[32mcount += ((LongWritable)writable).get();[m[41m					[m
[32m+[m				[32m}[m[41m[m
[32m+[m				[32mif(writable instanceof Text) {[m[41m[m
[32m+[m					[32mcount += Long.parseLong(((Text)writable).toString());[m[41m[m
[32m+[m				[32m}[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyGenericWritable extends GenericWritable{[m[41m[m
[32m+[m		[32mpublic MyGenericWritable() {}[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mpublic MyGenericWritable(Text text) {[m[41m[m
[32m+[m			[32msuper.set(text);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32mpublic MyGenericWritable(LongWritable longWritable) {[m[41m[m
[32m+[m			[32msuper.set(longWritable);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mprotected Class<? extends Writable>[] getTypes() {[m[41m[m
[32m+[m			[32mreturn new Class[] {LongWritable.class, Text.class};[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/inputformat/MyKeyValueTextInputFormatApp.java b/src/inputformat/MyKeyValueTextInputFormatApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..654a0b8[m
[1m--- /dev/null[m
[1m+++ b/src/inputformat/MyKeyValueTextInputFormatApp.java[m
[36m@@ -0,0 +1,54 @@[m
[32m+[m[32mpackage inputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * hello	you	hello	you[m[41m[m
[32m+[m[32m * hello	me[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class MyKeyValueTextInputFormatApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/hello";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mconf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , MyKeyValueTextInputFormatApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(MyKeyValueTextInputFormatApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setInputFormatClass(KeyValueTextInputFormat.class);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjob.setNumReduceTasks(0);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<Text, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32mprotected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<Text,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m				[32mcontext.write(key, new LongWritable(1));[m[41m[m
[32m+[m				[32mcontext.write(value, new LongWritable(1));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/inputformat/MyNLineInputFormatApp.java b/src/inputformat/MyNLineInputFormatApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..af21024[m
[1m--- /dev/null[m
[1m+++ b/src/inputformat/MyNLineInputFormatApp.java[m
[36m@@ -0,0 +1,85 @@[m
[32m+[m[32mpackage inputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * TextInputFormat处理的数据来自于一个InputSplit。InputSplit是根据大小划分的。[m[41m[m
[32m+[m[32m * NLineInputFormat决定每个Mapper处理的记录数是相同的。[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class MyNLineInputFormatApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/hello";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32m//设置每个map可以处理多少条记录[m[41m[m
[32m+[m		[32mconf.setInt("mapreduce.input.lineinputformat.linespermap", 2);[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , MyNLineInputFormatApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(MyNLineInputFormatApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setInputFormatClass(NLineInputFormat.class);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/inputformat/MyselInputFormatApp.java b/src/inputformat/MyselInputFormatApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..e1d36e5[m
[1m--- /dev/null[m
[1m+++ b/src/inputformat/MyselInputFormatApp.java[m
[36m@@ -0,0 +1,218 @@[m
[32m+[m[32mpackage inputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataInput;[m[41m[m
[32m+[m[32mimport java.io.DataOutput;[m[41m[m
[32m+[m[32mimport java.io.FileInputStream;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.util.ArrayList;[m[41m[m
[32m+[m[32mimport java.util.List;[m[41m[m
[32m+[m[32mimport java.util.Random;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport mapreduce.WordCountApp;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.ArrayWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.NullWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Writable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.InputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.InputSplit;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.JobContext;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.RecordReader;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.TaskAttemptContext;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * 数据源来自于内存[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class MyselInputFormatApp {[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setInputFormatClass(MyselfMemoryInputFormat.class);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<NullWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(NullWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<NullWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m/**[m[41m[m
[32m+[m	[32m * 从内存中产生数据，然后解析成一个个的键值对[m[41m[m
[32m+[m	[32m *[m[41m[m
[32m+[m	[32m */[m[41m[m
[32m+[m	[32mpublic static class MyselfMemoryInputFormat extends InputFormat<NullWritable, Text>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic List<InputSplit> getSplits(JobContext context)[m[41m[m
[32m+[m				[32mthrows IOException, InterruptedException {[m[41m[m
[32m+[m			[32mfinal ArrayList<InputSplit> result = new ArrayList<InputSplit>();[m[41m[m
[32m+[m			[32mresult.add(new MemoryInputSplit());[m[41m[m
[32m+[m			[32mresult.add(new MemoryInputSplit());[m[41m[m
[32m+[m			[32mresult.add(new MemoryInputSplit());[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mreturn result;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic RecordReader<NullWritable, Text> createRecordReader([m[41m[m
[32m+[m				[32mInputSplit split, TaskAttemptContext context)[m[41m[m
[32m+[m				[32mthrows IOException, InterruptedException {[m[41m[m
[32m+[m			[32mreturn new MemoryRecordReader();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MemoryInputSplit extends InputSplit implements Writable{[m[41m[m
[32m+[m		[32mfinal int SIZE = 10;[m[41m[m
[32m+[m		[32mfinal ArrayWritable arrayWritable = new ArrayWritable(Text.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32m/**[m[41m[m
[32m+[m		[32m * 先创建一个java数组类型，然后转化为hadoop的数组类型[m[41m[m
[32m+[m		[32m */[m[41m[m
[32m+[m		[32mpublic MemoryInputSplit() {[m[41m[m
[32m+[m			[32mText[] array = new Text[SIZE];[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mfinal Random random = new Random();[m[41m[m
[32m+[m			[32mfor (int i = 0; i < SIZE; i++) {[m[41m[m
[32m+[m				[32mfinal int nextInt = random.nextInt(999999);[m[41m[m
[32m+[m				[32mfinal Text text = new Text("Text"+nextInt);[m[41m[m
[32m+[m				[32marray[i] = text;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32marrayWritable.set(array);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic long getLength() throws IOException, InterruptedException {[m[41m[m
[32m+[m			[32mreturn SIZE;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic String[] getLocations() throws IOException, InterruptedException {[m[41m[m
[32m+[m			[32mreturn new String[] {"localhost"};[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32mpublic ArrayWritable getValues() {[m[41m[m
[32m+[m			[32mreturn arrayWritable;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m			[32marrayWritable.write(out);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m			[32marrayWritable.readFields(in);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MemoryRecordReader extends RecordReader<NullWritable, Text>{[m[41m[m
[32m+[m		[32mWritable[] values = null;[m[41m[m
[32m+[m		[32mText value = null;[m[41m[m
[32m+[m		[32mint i = 0;[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void initialize(InputSplit split, TaskAttemptContext context)[m[41m[m
[32m+[m				[32mthrows IOException, InterruptedException {[m[41m[m
[32m+[m			[32mMemoryInputSplit inputSplit = (MemoryInputSplit)split;[m[41m[m
[32m+[m			[32mArrayWritable writables = inputSplit.getValues();[m[41m[m
[32m+[m			[32mthis.values = writables.get();[m[41m[m
[32m+[m			[32mthis.i = 0;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic boolean nextKeyValue() throws IOException, InterruptedException {[m[41m[m
[32m+[m			[32mif(i>=values.length) {[m[41m[m
[32m+[m				[32mreturn false;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mif(this.value==null) {[m[41m[m
[32m+[m				[32mthis.value = new Text();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mthis.value.set((Text)values[i]);[m[41m[m
[32m+[m			[32mi++;[m[41m[m
[32m+[m			[32mreturn true;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic NullWritable getCurrentKey() throws IOException,[m[41m[m
[32m+[m				[32mInterruptedException {[m[41m[m
[32m+[m			[32mreturn NullWritable.get();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic Text getCurrentValue() throws IOException, InterruptedException {[m[41m[m
[32m+[m			[32mreturn value;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic float getProgress() throws IOException, InterruptedException {[m[41m[m
[32m+[m			[32mreturn 0;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void close() throws IOException {[m[41m[m
[32m+[m[41m			[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/mapreduce/KpiApp.java b/src/mapreduce/KpiApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..a722c01[m
[1m--- /dev/null[m
[1m+++ b/src/mapreduce/KpiApp.java[m
[36m@@ -0,0 +1,128 @@[m
[32m+[m[32mpackage mapreduce;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataInput;[m[41m[m
[32m+[m[32mimport java.io.DataOutput;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Writable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class KpiApp {[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static final String INPUT_PATH = "hdfs://chaoren1:9000/kpi";[m[41m[m
[32m+[m	[32mpublic static final String OUT_PATH = "hdfs://chaoren1:9000/kpi_out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mfinal Configuration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf, KpiApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(KpiApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, new Path(INPUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32m//当reduce输出类型与map输出类型一致时，map输出类型可以不设置[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(KpiWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{[m[41m[m
[32m+[m		[32mfinal Text k2 = new Text();[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m			[32mfinal String mobileNumber = splited[1];[m[41m[m
[32m+[m			[32mk2.set(mobileNumber);[m[41m[m
[32m+[m			[32mfinal KpiWritable v2 = new KpiWritable(Long.parseLong(splited[6]), Long.parseLong(splited[7]), Long.parseLong(splited[8]), Long.parseLong(splited[9]));[m[41m[m
[32m+[m			[32mcontext.write(k2, v2);[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{[m[41m[m
[32m+[m		[32mfinal KpiWritable v3 = new KpiWritable();[m[41m[m
[32m+[m		[32mprotected void reduce(Text k2, java.lang.Iterable<KpiWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,KpiWritable,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mlong upPackNum	=0L;[m[41m[m
[32m+[m			[32mlong downPackNum=0L;[m[41m[m
[32m+[m			[32mlong upPayLoad	=0L;[m[41m[m
[32m+[m			[32mlong downPayLoad=0L;[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mfor (KpiWritable kpiWritable : v2s) {[m[41m[m
[32m+[m				[32mupPackNum += kpiWritable.upPackNum;[m[41m[m
[32m+[m				[32mdownPackNum += kpiWritable.downPackNum;[m[41m[m
[32m+[m				[32mupPayLoad += kpiWritable.upPayLoad;[m[41m[m
[32m+[m				[32mdownPayLoad += kpiWritable.downPayLoad;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mv3.set(upPackNum, downPackNum, upPayLoad, downPayLoad);[m[41m[m
[32m+[m			[32mcontext.write(k2, v3);[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[32mclass KpiWritable  implements Writable{[m[41m[m
[32m+[m	[32mlong upPackNum;[m[41m[m
[32m+[m	[32mlong downPackNum;[m[41m[m
[32m+[m	[32mlong upPayLoad;[m[41m[m
[32m+[m	[32mlong downPayLoad;[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mpublic KpiWritable() {}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic KpiWritable(long upPackNum, long downPackNum, long upPayLoad,[m[41m[m
[32m+[m			[32mlong downPayLoad) {[m[41m[m
[32m+[m		[32msuper();[m[41m[m
[32m+[m		[32mset(upPackNum, downPackNum, upPayLoad, downPayLoad);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic void set(long upPackNum, long downPackNum, long upPayLoad,[m[41m[m
[32m+[m			[32mlong downPayLoad) {[m[41m[m
[32m+[m		[32mthis.upPackNum = upPackNum;[m[41m[m
[32m+[m		[32mthis.downPackNum = downPackNum;[m[41m[m
[32m+[m		[32mthis.upPayLoad = upPayLoad;[m[41m[m
[32m+[m		[32mthis.downPayLoad = downPayLoad;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m		[32mout.writeLong(this.upPackNum);[m[41m[m
[32m+[m		[32mout.writeLong(this.downPackNum);[m[41m[m
[32m+[m		[32mout.writeLong(this.upPayLoad);[m[41m[m
[32m+[m		[32mout.writeLong(this.downPayLoad);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m		[32mthis.upPackNum = in.readLong();[m[41m[m
[32m+[m		[32mthis.downPackNum = in.readLong();[m[41m[m
[32m+[m		[32mthis.upPayLoad = in.readLong();[m[41m[m
[32m+[m		[32mthis.downPayLoad = in.readLong();[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic String toString() {[m[41m[m
[32m+[m		[32mreturn upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/mapreduce/KpiApp2.java b/src/mapreduce/KpiApp2.java[m
[1mnew file mode 100644[m
[1mindex 0000000..69895e3[m
[1m--- /dev/null[m
[1m+++ b/src/mapreduce/KpiApp2.java[m
[36m@@ -0,0 +1,77 @@[m
[32m+[m[32mpackage mapreduce;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.NullWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class KpiApp2 {[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static final String INPUT_PATH = "hdfs://chaoren1:9000/kpi";[m[41m[m
[32m+[m	[32mpublic static final String OUT_PATH = "hdfs://chaoren1:9000/kpi_out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mfinal Configuration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf, KpiApp2.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(KpiApp2.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, new Path(INPUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32m//当reduce输出类型与map输出类型一致时，map输出类型可以不设置[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(KpiWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(NullWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m			[32mfinal String mobileNumber = splited[1];[m[41m[m
[32m+[m			[32mfinal Text k2 = new Text(mobileNumber);[m[41m[m
[32m+[m			[32mfinal KpiWritable v2 = new KpiWritable(Long.parseLong(splited[6]), Long.parseLong(splited[7]), Long.parseLong(splited[8]), Long.parseLong(splited[9]));[m[41m[m
[32m+[m			[32mcontext.write(k2, v2);[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, KpiWritable, Text, NullWritable>{[m[41m[m
[32m+[m		[32mprotected void reduce(Text k2, java.lang.Iterable<KpiWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,KpiWritable,Text,NullWritable>.Context context) throws IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mlong upPackNum	=0L;[m[41m[m
[32m+[m			[32mlong downPackNum=0L;[m[41m[m
[32m+[m			[32mlong upPayLoad	=0L;[m[41m[m
[32m+[m			[32mlong downPayLoad=0L;[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mfor (KpiWritable kpiWritable : v2s) {[m[41m[m
[32m+[m				[32mupPackNum += kpiWritable.upPackNum;[m[41m[m
[32m+[m				[32mdownPackNum += kpiWritable.downPackNum;[m[41m[m
[32m+[m				[32mupPayLoad += kpiWritable.upPayLoad;[m[41m[m
[32m+[m				[32mdownPayLoad += kpiWritable.downPayLoad;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m			[m
[32m+[m[41m			[m
[32m+[m			[32mcontext.write(new Text(k2.toString() + "\t" + upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad), NullWritable.get());[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m
\ No newline at end of file[m
[1mdiff --git a/src/mapreduce/WordCountApp.java b/src/mapreduce/WordCountApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..dbbcd5b[m
[1m--- /dev/null[m
[1m+++ b/src/mapreduce/WordCountApp.java[m
[36m@@ -0,0 +1,82 @@[m
[32m+[m[32mpackage mapreduce;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32m/**[m[41m[m
[32m+[m[32m * hello	you[m[41m[m
[32m+[m[32m * hello	me[m[41m[m
[32m+[m[32m *[m[41m[m
[32m+[m[32m */[m[41m[m
[32m+[m[32mpublic class WordCountApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/old/WordCountApp.java b/src/old/WordCountApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..fcfac0d[m
[1m--- /dev/null[m
[1m+++ b/src/old/WordCountApp.java[m
[36m@@ -0,0 +1,75 @@[m
[32m+[m[32mpackage old;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.util.Iterator;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobClient;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobConf;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.MapReduceBase;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.OutputCollector;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reporter;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class WordCountApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal JobConf job = new JobConf(conf , WordCountApp.class);[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mJobClient.runJob(job);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void map(LongWritable key, Text value,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> output, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32moutput.collect(new Text(word), new LongWritable(1L));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void reduce(Text key, Iterator<LongWritable> values,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> output, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mlong times = 0L;[m[41m[m
[32m+[m			[32mwhile (values.hasNext()) {[m[41m[m
[32m+[m				[32mLongWritable longWritable = (LongWritable) values.next();[m[41m[m
[32m+[m				[32mtimes += longWritable.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32moutput.collect(key, new LongWritable(times));[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/outputformat/MyMultipleOutputFormatApp.java b/src/outputformat/MyMultipleOutputFormatApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..adbedbf[m
[1m--- /dev/null[m
[1m+++ b/src/outputformat/MyMultipleOutputFormatApp.java[m
[36m@@ -0,0 +1,110 @@[m
[32m+[m[32mpackage outputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.util.Iterator;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport mapreduce.WordCountApp;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.FileOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobClient;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.JobConf;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.MapReduceBase;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.OutputCollector;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.Reporter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.TextOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapred.lib.MultipleOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.util.Progressable;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class MyMultipleOutputFormatApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal JobConf job = new JobConf(conf , WordCountApp.class);[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjob.setOutputFormat(MyMultipleFilesTextOutputFormat.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mJobClient.runJob(job);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m			[32m@Override[m[41m[m
[32m+[m			[32mpublic void map(LongWritable key, Text value,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> output, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m				[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m				[32mfinal String line = value.toString();[m[41m[m
[32m+[m				[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m				[m
[32m+[m				[32m//产生的<k,v>对少了[m[41m[m
[32m+[m				[32mfor (String word : splited) {[m[41m[m
[32m+[m					[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m					[32moutput.collect(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m				[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends MapReduceBase implements  Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void reduce(Text key, Iterator<LongWritable> values,[m[41m[m
[32m+[m				[32mOutputCollector<Text, LongWritable> output, Reporter reporter)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m				[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m				[32mlong count = 0L;[m[41m[m
[32m+[m				[32mwhile(values.hasNext()) {[m[41m[m
[32m+[m					[32mLongWritable times = values.next();[m[41m[m
[32m+[m					[32mcount += times.get();[m[41m[m
[32m+[m				[32m}[m[41m[m
[32m+[m				[32moutput.collect(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMultipleFilesTextOutputFormat extends MultipleOutputFormat<Text, LongWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mprotected org.apache.hadoop.mapred.RecordWriter<Text, LongWritable> getBaseRecordWriter([m[41m[m
[32m+[m				[32mFileSystem fs, JobConf job, String name, Progressable progress)[m[41m[m
[32m+[m				[32mthrows IOException {[m[41m[m
[32m+[m			[32mfinal TextOutputFormat<Text, LongWritable> textOutputFormat = new TextOutputFormat<Text, LongWritable>();[m[41m[m
[32m+[m			[32mreturn textOutputFormat.getRecordWriter(fs, job, name, progress);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m[41m		[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mprotected String generateFileNameForKeyValue(Text key,[m[41m[m
[32m+[m				[32mLongWritable value, String name) {[m[41m[m
[32m+[m			[32m//输出的文件名就是k3的值[m[41m[m
[32m+[m			[32mfinal String keyString = key.toString();[m[41m[m
[32m+[m			[32mif(keyString.startsWith("hello")) {[m[41m[m
[32m+[m				[32mreturn "hello";[m[41m[m
[32m+[m			[32m}else {[m[41m[m
[32m+[m				[32mreturn keyString;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/outputformat/MySlefOutputFormatApp.java b/src/outputformat/MySlefOutputFormatApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..d737f2f[m
[1m--- /dev/null[m
[1m+++ b/src/outputformat/MySlefOutputFormatApp.java[m
[36m@@ -0,0 +1,143 @@[m
[32m+[m[32mpackage outputformat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[32mimport java.net.URISyntaxException;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport mapreduce.WordCountApp;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FSDataOutputStream;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.JobContext;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.OutputCommitter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.OutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.RecordWriter;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.TaskAttemptContext;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class MySlefOutputFormatApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/files/hello";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m	[32mprivate static final String OUT_FIE_NAME = "/abc";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , WordCountApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(WordCountApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjob.setOutputFormatClass(MySelfTextOutputFormat.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//为什么要把hadoop类型转换为java类型？[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32m//产生的<k,v>对少了[m[41m[m
[32m+[m			[32mfor (String word : splited) {[m[41m[m
[32m+[m				[32m//在for循环体内，临时变量word的出现次数是常量1[m[41m[m
[32m+[m				[32mcontext.write(new Text(word), new LongWritable(1));[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>[m[41m[m
[32m+[m	[32m//分区，默认只有一个区[m[41m[m
[32m+[m	[32m//排序后的结果：<hello,1><hello,1><me,1><you,1>[m[41m[m
[32m+[m	[32m//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>[m[41m[m
[32m+[m	[32m//归约(可选)[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32m//map产生的<k,v>分发到reduce的过程称作shuffle[m[41m[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{[m[41m[m
[32m+[m		[32m//每一组调用一次reduce函数，一共调用了3次[m[41m[m
[32m+[m		[32m//分组的数量与reduce函数的调用次数有什么关系？[m[41m[m
[32m+[m		[32m//reduce函数的调用次数与输出的<k,v>的数量有什么关系？[m[41m[m
[32m+[m		[32mprotected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32m//count表示单词key在整个文件中的出现次数[m[41m[m
[32m+[m			[32mlong count = 0L;[m[41m[m
[32m+[m			[32mfor (LongWritable times : values) {[m[41m[m
[32m+[m				[32mcount += times.get();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mcontext.write(key, new LongWritable(count));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MySelfTextOutputFormat extends OutputFormat<Text, LongWritable>{[m[41m[m
[32m+[m		[32mFSDataOutputStream outputStream =  null;[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic RecordWriter<Text, LongWritable> getRecordWriter([m[41m[m
[32m+[m				[32mTaskAttemptContext context) throws IOException,[m[41m[m
[32m+[m				[32mInterruptedException {[m[41m[m
[32m+[m			[32mtry {[m[41m[m
[32m+[m				[32mfinal FileSystem fileSystem = FileSystem.get(new URI(MySlefOutputFormatApp.OUT_PATH), context.getConfiguration());[m[41m[m
[32m+[m				[32m//指定的是输出文件路径[m[41m[m
[32m+[m				[32mfinal Path opath = new Path(MySlefOutputFormatApp.OUT_PATH+OUT_FIE_NAME);[m[41m[m
[32m+[m					[32mthis.outputStream = fileSystem.create(opath, false);[m[41m[m
[32m+[m			[32m} catch (URISyntaxException e) {[m[41m[m
[32m+[m				[32me.printStackTrace();[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mreturn new MySlefRecordWriter(outputStream);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void checkOutputSpecs(JobContext context) throws IOException,[m[41m[m
[32m+[m				[32mInterruptedException {[m[41m[m
[32m+[m[41m			[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic OutputCommitter getOutputCommitter(TaskAttemptContext context)[m[41m[m
[32m+[m				[32mthrows IOException, InterruptedException {[m[41m[m
[32m+[m			[32mreturn new FileOutputCommitter(new Path(MySlefOutputFormatApp.OUT_PATH), context);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MySlefRecordWriter extends RecordWriter<Text, LongWritable>{[m[41m[m
[32m+[m		[32mFSDataOutputStream outputStream = null;[m[41m[m
[32m+[m[41m[m
[32m+[m		[32mpublic MySlefRecordWriter(FSDataOutputStream outputStream) {[m[41m[m
[32m+[m			[32mthis.outputStream = outputStream;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void write(Text key, LongWritable value) throws IOException,[m[41m[m
[32m+[m				[32mInterruptedException {[m[41m[m
[32m+[m			[32mthis.outputStream.writeBytes(key.toString());[m[41m[m
[32m+[m			[32mthis.outputStream.writeBytes("\t");[m[41m[m
[32m+[m			[32mthis.outputStream.writeLong(value.get());[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void close(TaskAttemptContext context) throws IOException,[m[41m[m
[32m+[m				[32mInterruptedException {[m[41m[m
[32m+[m			[32mthis.outputStream.close();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/partitioner/KpiApp.java b/src/partitioner/KpiApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..fb55649[m
[1m--- /dev/null[m
[1m+++ b/src/partitioner/KpiApp.java[m
[36m@@ -0,0 +1,141 @@[m
[32m+[m[32mpackage partitioner;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataInput;[m[41m[m
[32m+[m[32mimport java.io.DataOutput;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Writable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Partitioner;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class KpiApp {[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static final String INPUT_PATH = "hdfs://chaoren1:9000/kpi";[m[41m[m
[32m+[m	[32mpublic static final String OUT_PATH = "hdfs://chaoren1:9000/kpi_out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mfinal Configuration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf, KpiApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(KpiApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, new Path(INPUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setPartitionerClass(MyPartitioner.class);[m[41m[m
[32m+[m[41m[m
[32m+[m		[32mjob.setNumReduceTasks(2);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(Text.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(KpiWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{[m[41m[m
[32m+[m		[32mfinal Text k2 = new Text();[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m			[32mfinal String mobileNumber = splited[1];[m[41m[m
[32m+[m			[32mk2.set(mobileNumber);[m[41m[m
[32m+[m			[32mfinal KpiWritable v2 = new KpiWritable(Long.parseLong(splited[6]), Long.parseLong(splited[7]), Long.parseLong(splited[8]), Long.parseLong(splited[9]));[m[41m[m
[32m+[m			[32mcontext.write(k2, v2);[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{[m[41m[m
[32m+[m		[32mfinal KpiWritable v3 = new KpiWritable();[m[41m[m
[32m+[m		[32mprotected void reduce(Text k2, java.lang.Iterable<KpiWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,KpiWritable,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mlong upPackNum	=0L;[m[41m[m
[32m+[m			[32mlong downPackNum=0L;[m[41m[m
[32m+[m			[32mlong upPayLoad	=0L;[m[41m[m
[32m+[m			[32mlong downPayLoad=0L;[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mfor (KpiWritable kpiWritable : v2s) {[m[41m[m
[32m+[m				[32mupPackNum += kpiWritable.upPackNum;[m[41m[m
[32m+[m				[32mdownPackNum += kpiWritable.downPackNum;[m[41m[m
[32m+[m				[32mupPayLoad += kpiWritable.upPayLoad;[m[41m[m
[32m+[m				[32mdownPayLoad += kpiWritable.downPayLoad;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mv3.set(upPackNum, downPackNum, upPayLoad, downPayLoad);[m[41m[m
[32m+[m			[32mcontext.write(k2, v3);[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mstatic class MyPartitioner extends Partitioner<Text, KpiWritable>{[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic int getPartition(Text key, KpiWritable value, int numPartitions) {[m[41m[m
[32m+[m			[32mfinal int length = key.toString().length();[m[41m[m
[32m+[m			[32mreturn length==11?0:1;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[32mclass KpiWritable  implements Writable{[m[41m[m
[32m+[m	[32mlong upPackNum;[m[41m[m
[32m+[m	[32mlong downPackNum;[m[41m[m
[32m+[m	[32mlong upPayLoad;[m[41m[m
[32m+[m	[32mlong downPayLoad;[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mpublic KpiWritable() {}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic KpiWritable(long upPackNum, long downPackNum, long upPayLoad,[m[41m[m
[32m+[m			[32mlong downPayLoad) {[m[41m[m
[32m+[m		[32msuper();[m[41m[m
[32m+[m		[32mset(upPackNum, downPackNum, upPayLoad, downPayLoad);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic void set(long upPackNum, long downPackNum, long upPayLoad,[m[41m[m
[32m+[m			[32mlong downPayLoad) {[m[41m[m
[32m+[m		[32mthis.upPackNum = upPackNum;[m[41m[m
[32m+[m		[32mthis.downPackNum = downPackNum;[m[41m[m
[32m+[m		[32mthis.upPayLoad = upPayLoad;[m[41m[m
[32m+[m		[32mthis.downPayLoad = downPayLoad;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m		[32mout.writeLong(this.upPackNum);[m[41m[m
[32m+[m		[32mout.writeLong(this.downPackNum);[m[41m[m
[32m+[m		[32mout.writeLong(this.upPayLoad);[m[41m[m
[32m+[m		[32mout.writeLong(this.downPayLoad);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m		[32mthis.upPackNum = in.readLong();[m[41m[m
[32m+[m		[32mthis.downPackNum = in.readLong();[m[41m[m
[32m+[m		[32mthis.upPayLoad = in.readLong();[m[41m[m
[32m+[m		[32mthis.downPayLoad = in.readLong();[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic String toString() {[m[41m[m
[32m+[m		[32mreturn upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/rpc/MyBiz.java b/src/rpc/MyBiz.java[m
[1mnew file mode 100644[m
[1mindex 0000000..e1aa4be[m
[1m--- /dev/null[m
[1m+++ b/src/rpc/MyBiz.java[m
[36m@@ -0,0 +1,25 @@[m
[32m+[m[32mpackage rpc;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.ipc.VersionedProtocol;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class MyBiz implements  MyBizable{[m[41m[m
[32m+[m	[32m/* (non-Javadoc)[m[41m[m
[32m+[m	[32m * @see rpc.MyBizable#hello(java.lang.String)[m[41m[m
[32m+[m	[32m */[m[41m[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic String hello(String name) {[m[41m[m
[32m+[m		[32mSystem.out.println("我被调用了");[m[41m[m
[32m+[m		[32mreturn "hello "+name;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m	[32m/* (non-Javadoc)[m[41m[m
[32m+[m	[32m * @see rpc.MyBizable#getProtocolVersion(java.lang.String, long)[m[41m[m
[32m+[m	[32m */[m[41m[m
[32m+[m	[32m@Override[m[41m[m
[32m+[m	[32mpublic long getProtocolVersion(String protocol, long clientVersion)[m[41m[m
[32m+[m			[32mthrows IOException {[m[41m[m
[32m+[m		[32mreturn MyBizable.PORT;[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/rpc/MyBizable.java b/src/rpc/MyBizable.java[m
[1mnew file mode 100644[m
[1mindex 0000000..6c7ff8c[m
[1m--- /dev/null[m
[1m+++ b/src/rpc/MyBizable.java[m
[36m@@ -0,0 +1,14 @@[m
[32m+[m[32mpackage rpc;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.ipc.VersionedProtocol;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic interface MyBizable extends VersionedProtocol{[m[41m[m
[32m+[m	[32mpublic static final int PORT = 12345;[m[41m[m
[32m+[m	[32mpublic abstract String hello(String name);[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic abstract long getProtocolVersion(String protocol, long clientVersion)[m[41m[m
[32m+[m			[32mthrows IOException;[m[41m[m
[32m+[m[41m[m
[32m+[m[32m}[m
\ No newline at end of file[m
[1mdiff --git a/src/rpc/MyClient.java b/src/rpc/MyClient.java[m
[1mnew file mode 100644[m
[1mindex 0000000..c6fb2d4[m
[1m--- /dev/null[m
[1m+++ b/src/rpc/MyClient.java[m
[36m@@ -0,0 +1,14 @@[m
[32m+[m[32mpackage rpc;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.net.InetSocketAddress;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.ipc.RPC;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class MyClient {[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mMyBizable client = (MyBizable) RPC.getProxy(MyBizable.class, MyBizable.PORT, new InetSocketAddress(MyServer.ADDRESS, MyServer.PORT), new Configuration());[m[41m[m
[32m+[m		[32mfinal String result = client.hello("world");[m[41m[m
[32m+[m		[32mSystem.out.println(result);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/rpc/MyServer.java b/src/rpc/MyServer.java[m
[1mnew file mode 100644[m
[1mindex 0000000..e54008b[m
[1m--- /dev/null[m
[1m+++ b/src/rpc/MyServer.java[m
[36m@@ -0,0 +1,23 @@[m
[32m+[m[32mpackage rpc;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.ipc.RPC;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.ipc.RPC.Server;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class MyServer {[m[41m[m
[32m+[m	[32mpublic static final String ADDRESS = "localhost";[m[41m[m
[32m+[m	[32mpublic static final int PORT = 2454;[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m	[32m    /**[m[41m[m
[32m+[m	[32m     * 构造一个RPC的服务端[m[41m[m
[32m+[m	[32m     * @param instance 实例对象的方法会被客户端调用。[m[41m[m
[32m+[m	[32m     * @param bindAddress the address to bind on to listen for connection[m[41m[m
[32m+[m	[32m     * @param port the port to listen for connections on[m[41m[m
[32m+[m	[32m     * @param conf the configuration to use[m[41m[m
[32m+[m	[32m     */[m[41m[m
[32m+[m		[32mfinal Server server = RPC.getServer(new MyBiz(), MyServer.ADDRESS, MyServer.PORT, new Configuration());[m[41m[m
[32m+[m		[32mserver.start();[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m[32m}[m
\ No newline at end of file[m
[1mdiff --git a/src/sort/SortApp.java b/src/sort/SortApp.java[m
[1mnew file mode 100644[m
[1mindex 0000000..0f82103[m
[1m--- /dev/null[m
[1m+++ b/src/sort/SortApp.java[m
[36m@@ -0,0 +1,96 @@[m
[32m+[m[32mpackage sort;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport java.io.DataInput;[m[41m[m
[32m+[m[32mimport java.io.DataOutput;[m[41m[m
[32m+[m[32mimport java.io.IOException;[m[41m[m
[32m+[m[32mimport java.net.URI;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mimport org.apache.hadoop.conf.Configuration;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.FileSystem;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.fs.Path;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.LongWritable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.Text;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.io.WritableComparable;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Job;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Mapper;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.Reducer;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;[m[41m[m
[32m+[m[32mimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;[m[41m[m
[32m+[m[41m[m
[32m+[m[32mpublic class SortApp {[m[41m[m
[32m+[m	[32mprivate static final String INPUT_PATH = "hdfs://chaoren1:9000/data";[m[41m[m
[32m+[m	[32mprivate static final String OUT_PATH = "hdfs://chaoren1:9000/out";[m[41m[m
[32m+[m[41m[m
[32m+[m	[32mpublic static void main(String[] args) throws Exception{[m[41m[m
[32m+[m		[32mConfiguration conf = new Configuration();[m[41m[m
[32m+[m		[32mfinal FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);[m[41m[m
[32m+[m		[32mfilesystem.delete(new Path(OUT_PATH), true);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mfinal Job job = new Job(conf , SortApp.class.getSimpleName());[m[41m[m
[32m+[m		[32mjob.setJarByClass(SortApp.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mFileInputFormat.setInputPaths(job, INPUT_PATH);[m[41m[m
[32m+[m		[32mjob.setMapperClass(MyMapper.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputKeyClass(NewK2.class);[m[41m[m
[32m+[m		[32mjob.setMapOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.setReducerClass(MyReducer.class);[m[41m[m
[32m+[m		[32mjob.setOutputKeyClass(LongWritable.class);[m[41m[m
[32m+[m		[32mjob.setOutputValueClass(LongWritable.class);[m[41m[m
[32m+[m		[32mFileOutputFormat.setOutputPath(job, new Path(OUT_PATH));[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mjob.waitForCompletion(true);[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable>{[m[41m[m
[32m+[m		[32m//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次[m[41m[m
[32m+[m		[32mprotected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,NewK2,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mfinal String line = value.toString();[m[41m[m
[32m+[m			[32mfinal String[] splited = line.split("\t");[m[41m[m
[32m+[m[41m			[m
[32m+[m			[32mcontext.write(new NewK2(Long.parseLong(splited[0]), Long.parseLong(splited[1])), new LongWritable(Long.parseLong(splited[1])));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable>{[m[41m[m
[32m+[m		[32mprotected void reduce(NewK2 key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<NewK2,LongWritable,LongWritable,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {[m[41m[m
[32m+[m			[32mcontext.write(new LongWritable(key.first), new LongWritable(key.second));[m[41m[m
[32m+[m		[32m};[m[41m[m
[32m+[m	[32m}[m[41m[m
[32m+[m[41m	[m
[32m+[m	[32mpublic static class NewK2 implements WritableComparable<NewK2>{[m[41m[m
[32m+[m		[32mlong first;[m[41m[m
[32m+[m		[32mlong second;[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mpublic NewK2() {[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m		[32mpublic NewK2(long first, long second) {[m[41m[m
[32m+[m			[32mthis.first = first;[m[41m[m
[32m+[m			[32mthis.second = second;[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void write(DataOutput out) throws IOException {[m[41m[m
[32m+[m			[32mout.writeLong(this.first);[m[41m[m
[32m+[m			[32mout.writeLong(this.second);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic void readFields(DataInput in) throws IOException {[m[41m[m
[32m+[m			[32mthis.first = in.readLong();[m[41m[m
[32m+[m			[32mthis.second = in.readLong();[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m[m
[32m+[m		[32m@Override[m[41m[m
[32m+[m		[32mpublic int compareTo(NewK2 o) {[m[41m[m
[32m+[m			[32mfinal long minus = this.first - o.first;[m[41m[m
[32m+[m			[32mif(minus!=0) {[m[41m[m
[32m+[m				[32mreturn (int)minus;[m[41m[m
[32m+[m			[32m}[m[41m[m
[32m+[m			[32mreturn (int)(this.second - o.second);[m[41m[m
[32m+[m		[32m}[m[41m[m
[32m+[m[41m		[m
[32m+[m	[32m}[m[41m[m
[32m+[m[32m}[m[41m[m
[1mdiff --git a/src/sort/data b/src/sort/data[m
[1mnew file mode 100644[m
[1mindex 0000000..ede5e78[m
[1m--- /dev/null[m
[1m+++ b/src/sort/data[m
[36m@@ -0,0 +1,13 @@[m
[32m+[m[32m3	3	3[m[41m[m
[32m+[m[32m3	2	4[m[41m[m
[32m+[m[32m3	2	0[m[41m[m
[32m+[m[32m2	2	1[m[41m[m
[32m+[m[32m2	1	4[m[41m[m
[32m+[m[32m1	1	0[m[41m[m
[32m+[m[32m-------------[m[41m[m
[32m+[m[32m1	1[m[41m[m
[32m+[m[32m2	1[m[41m[m
[32m+[m[32m2	2[m[41m[m
[32m+[m[32m3	1[m[41m[m
[32m+[m[32m3	2[m[41m[m
[32m+[m[32m3	3[m
\ No newline at end of file[m

[33mcommit 55b1e3a2215eedea5e7d4ae4f1ae50eb6421b91a[m
Author: penger <gongpengllpp@sina.com>
Date:   Fri Oct 24 09:40:41 2014 +0800

    repository init

[1mdiff --git a/README.md b/README.md[m
[1mnew file mode 100644[m
[1mindex 0000000..e69de29[m
