# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM artifactory.corp.olacabs.com:5000/ubuntu-java8:14.04.1

ARG STREAMLINE_VERSION

ENV JAVA_HOME /opt/jdk1.8.0_111/

# Add Hortonworks Registry binary. Maven profile "-Pdocker" will place the binary in the correct location
#ADD hortonworks-streamline-$STREAMLINE_VERSION.tar.gz /

# Install system dependencies
RUN ls
COPY ../../../webservices/target/streamline-$STREAMLINE_VERSION.jar /hortonworks-streamline-$STREAMLINE_VERSION.jar
#ADD streamline-$STREAMLINE_VERSION.jar /

CMD hortonworks-streamline-$STREAMLINE_VERSION/bin/streamline-server-start.sh hortonworks-streamline-$STREAMLINE_VERSION/conf/streamline-dev.yaml