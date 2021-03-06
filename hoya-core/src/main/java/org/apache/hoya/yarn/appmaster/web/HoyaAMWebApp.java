/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hoya.yarn.appmaster.web;

import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;

/**
 * 
 */
public class HoyaAMWebApp extends WebApp {
  public static final String BASE_PATH = "hoyaam";
  public static final String CONTAINER_STATS = "/stats";
  public static final String CLUSTER_SPEC = "/spec";
  
  @Override
  public void setup() {
    // Make one of these to ensure that the jax-b annotations
    // are properly picked up.
    //bind(JAXBContextResolver.class);
    
    // Get exceptions printed to the screen
    bind(GenericExceptionHandler.class);

    route("/", HoyaAMController.class);
    route(CONTAINER_STATS, HoyaAMController.class, "containerStats");
    route(CLUSTER_SPEC, HoyaAMController.class, "specification");
  }

}
