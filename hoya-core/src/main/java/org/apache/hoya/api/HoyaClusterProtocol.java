/*
 * Licensed under the Apache License, Version 2.0 (the "License") throws IOException, YarnException;
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *    http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License") throws IOException, YarnException; you may not use this file except in compliance
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

package org.apache.hoya.api;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hoya.HoyaXmlConfKeys;
import org.apache.hoya.api.proto.Messages;

import java.io.IOException;

/**
 * Cluster protocol. This can currently act as a versioned IPC
 * endpoint or be relayed via protobuf
 */
@KerberosInfo(
  serverPrincipal = HoyaXmlConfKeys.KEY_HOYA_KERBEROS_PRINCIPAL)
public interface HoyaClusterProtocol extends VersionedProtocol {
  public static final long versionID = 0x01;

  /**
   * Stop the cluster
   */

  
  Messages.StopClusterResponseProto stopCluster(Messages.StopClusterRequestProto request) throws
                                                                                          IOException, YarnException;


  /**
   * Flex the cluster. 
   */
  Messages.FlexClusterResponseProto flexCluster(Messages.FlexClusterRequestProto request) throws IOException,
                                                                                                 YarnException;


  /**
   * Get the current cluster status
   */
  Messages.GetJSONClusterStatusResponseProto getJSONClusterStatus(Messages.GetJSONClusterStatusRequestProto request) throws IOException, YarnException;


  /**
   * List all running nodes in a role
   */
  Messages.ListNodeUUIDsByRoleResponseProto listNodeUUIDsByRole(Messages.ListNodeUUIDsByRoleRequestProto request) throws IOException, YarnException;


  /**
   * Get the details on a node
   */
  Messages.GetNodeResponseProto getNode(Messages.GetNodeRequestProto request) throws IOException, YarnException;

  /**
   * Get the 
   * details on a list of nodes.
   * Unknown nodes are not returned
   * <i>Important: the order of the results are undefined</i>
   */
  Messages.GetClusterNodesResponseProto getClusterNodes(Messages.GetClusterNodesRequestProto request) throws IOException, YarnException;

  Messages.EchoResponseProto echo(Messages.EchoRequestProto request) throws IOException, YarnException;

  Messages.KillContainerResponseProto killContainer(Messages.KillContainerRequestProto request) throws IOException, YarnException;
  
}
